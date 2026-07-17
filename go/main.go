// list-top-level-prefixes: Go implementation of list_top_level_prefixes.py.
//
// Lists every top-level folder in an S3 bucket with its total size and
// newest object timestamp, plus the whole-bucket total. Same adaptive
// parallelism model as the Python script — each work unit starts as a flat
// pagination and is split into per-subfolder units (Delimiter='/') once it
// exceeds the page budget — but without Python's per-request CPU overhead,
// so throughput is bounded by network latency x concurrency instead of the
// interpreter. Use this one for large buckets.
//
// Output JSON is shape-compatible with the Python script.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const rootObjectsKey = "(bucket root)"

type folderStat struct {
	size   int64
	latest time.Time // zero value = no objects seen
}

type scanner struct {
	client     *s3.Client
	bucket     string
	splitPages int

	sem chan struct{} // limits concurrent S3 requests
	wg  sync.WaitGroup

	mu    sync.Mutex
	stats map[string]*folderStat // keyed by top-level folder

	completedUnits atomic.Int64
	totalUnits     atomic.Int64
}

func (sc *scanner) record(topFolder string, size int64, latest time.Time) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	st := sc.stats[topFolder]
	st.size += size
	if latest.After(st.latest) {
		st.latest = latest
	}
}

// listWithDelimiter lists one directory level under prefix, returning the
// subfolder prefixes plus size/newest-timestamp of objects directly at this
// level.
func (sc *scanner) listWithDelimiter(ctx context.Context, prefix string) ([]string, int64, time.Time, error) {
	var subfolders []string
	var directSize int64
	var directLatest time.Time
	var continuationToken *string

	for {
		out, err := sc.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(sc.bucket),
			Prefix:            aws.String(prefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, 0, time.Time{}, err
		}
		for _, cp := range out.CommonPrefixes {
			subfolders = append(subfolders, *cp.Prefix)
		}
		for _, obj := range out.Contents {
			directSize += aws.ToInt64(obj.Size)
			if obj.LastModified != nil && obj.LastModified.After(directLatest) {
				directLatest = *obj.LastModified
			}
		}
		if out.NextContinuationToken == nil {
			return subfolders, directSize, directLatest, nil
		}
		continuationToken = out.NextContinuationToken
	}
}

// scanUnit scans one subtree, splitting adaptively: flat pagination first,
// and if the subtree exceeds the page budget, discard the partial count and
// re-list one level deep, fanning subfolders out as new units.
func (sc *scanner) scanUnit(ctx context.Context, topFolder, prefix string) {
	defer sc.wg.Done()
	defer sc.completedUnits.Add(1)

	sc.sem <- struct{}{}
	defer func() { <-sc.sem }()

	var size int64
	var latest time.Time
	pages := 0
	var continuationToken *string

	for {
		out, err := sc.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(sc.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nerror scanning %s: %v\n", prefix, err)
			return
		}
		pages++
		for _, obj := range out.Contents {
			size += aws.ToInt64(obj.Size)
			if obj.LastModified != nil && obj.LastModified.After(latest) {
				latest = *obj.LastModified
			}
		}
		if out.NextContinuationToken == nil {
			sc.record(topFolder, size, latest)
			return
		}
		continuationToken = out.NextContinuationToken
		if pages >= sc.splitPages {
			break
		}
	}

	// Too big for one serial scan — split into subfolders instead. The
	// partial flat counts above are discarded; the delimiter listing counts
	// this level's direct objects and the children recount their subtrees.
	subfolders, directSize, directLatest, err := sc.listWithDelimiter(ctx, prefix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nerror splitting %s: %v\n", prefix, err)
		return
	}
	sc.record(topFolder, directSize, directLatest)
	sc.totalUnits.Add(int64(len(subfolders)))
	for _, sub := range subfolders {
		sc.wg.Add(1)
		go sc.scanUnit(ctx, topFolder, sub)
	}
}

func formatSize(sizeBytes int64) string {
	const gb = 1 << 30
	const tb = 1 << 40
	switch {
	case sizeBytes >= tb:
		return fmt.Sprintf("%.2f TB", float64(sizeBytes)/float64(tb))
	case sizeBytes >= gb:
		return fmt.Sprintf("%.2f GB", float64(sizeBytes)/float64(gb))
	default:
		return fmt.Sprintf("%d bytes", sizeBytes)
	}
}

func formatTimestamp(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format("2006-01-02T15:04:05Z")
	return &s
}

type folderJSON struct {
	TotalBytes            int64   `json:"total_bytes"`
	TotalSizeHuman        string  `json:"total_size_human"`
	LatestObjectTimestamp *string `json:"latest_object_timestamp"`
}

type outputJSON struct {
	Folders map[string]folderJSON `json:"folders"`
	Summary summaryJSON           `json:"summary"`
}

type summaryJSON struct {
	TotalFolders          int     `json:"total_folders"`
	BucketTotalSizeBytes  int64   `json:"bucket_total_size_bytes"`
	BucketTotalSizeHuman  string  `json:"bucket_total_size_human"`
	LatestObjectTimestamp *string `json:"latest_object_timestamp"`
}

func main() {
	output := flag.String("o", "top_level_prefixes.json", "output file name")
	workers := flag.Int("w", 200, "number of concurrent S3 requests")
	top := flag.Int("t", 20, "how many largest folders to print to stdout")
	splitPages := flag.Int("s", 20, "page budget (1000 objects per page) above which a subtree is split into parallel per-subfolder scans")
	region := flag.String("region", "", "AWS region override (default: from profile/env)")
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: list-top-level-prefixes [flags] <bucket>")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if *splitPages < 1 {
		fmt.Fprintln(os.Stderr, "-s must be >= 1")
		os.Exit(2)
	}
	bucket := flag.Arg(0)

	ctx := context.Background()
	var optFns []func(*config.LoadOptions) error
	if *region != "" {
		optFns = append(optFns, config.WithRegion(*region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load AWS config: %v\n", err)
		os.Exit(1)
	}

	sc := &scanner{
		client:     s3.NewFromConfig(cfg),
		bucket:     bucket,
		splitPages: *splitPages,
		sem:        make(chan struct{}, *workers),
		stats:      map[string]*folderStat{},
	}

	fmt.Printf("Using %d concurrent requests, splitting folders larger than %d list pages\n", *workers, *splitPages)
	fmt.Printf("Listing first-level folders in bucket: %s\n", bucket)

	start := time.Now()

	foldersWithSlash, rootSize, rootLatest, err := sc.listWithDelimiter(ctx, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to list bucket root: %v\n", err)
		os.Exit(1)
	}
	folders := make([]string, 0, len(foldersWithSlash))
	for _, f := range foldersWithSlash {
		name := strings.TrimSuffix(f, "/")
		folders = append(folders, name)
		sc.stats[name] = &folderStat{}
	}
	sort.Strings(folders)
	fmt.Printf("Found %d first-level folder(s)\n", len(folders))

	sc.totalUnits.Store(int64(len(folders)))
	for _, folder := range folders {
		sc.wg.Add(1)
		go sc.scanUnit(ctx, folder, folder+"/")
	}

	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fmt.Printf("\rScanning units %d/%d", sc.completedUnits.Load(), sc.totalUnits.Load())
			case <-progressDone:
				fmt.Printf("\rScanning units %d/%d\n", sc.completedUnits.Load(), sc.totalUnits.Load())
				return
			}
		}
	}()

	sc.wg.Wait()
	close(progressDone)

	if rootSize > 0 || !rootLatest.IsZero() {
		sc.stats[rootObjectsKey] = &folderStat{size: rootSize, latest: rootLatest}
	}

	var bucketTotal int64
	var overallLatest time.Time
	names := make([]string, 0, len(sc.stats))
	for name, st := range sc.stats {
		bucketTotal += st.size
		if st.latest.After(overallLatest) {
			overallLatest = st.latest
		}
		names = append(names, name)
	}
	// Descending by size, folder name as a stable tie-breaker.
	sort.Slice(names, func(i, j int) bool {
		si, sj := sc.stats[names[i]].size, sc.stats[names[j]].size
		if si != sj {
			return si > sj
		}
		return names[i] < names[j]
	})

	out := outputJSON{
		Folders: make(map[string]folderJSON, len(sc.stats)),
		Summary: summaryJSON{
			TotalFolders:          len(folders),
			BucketTotalSizeBytes:  bucketTotal,
			BucketTotalSizeHuman:  formatSize(bucketTotal),
			LatestObjectTimestamp: formatTimestamp(overallLatest),
		},
	}
	for name, st := range sc.stats {
		out.Folders[name] = folderJSON{
			TotalBytes:            st.size,
			TotalSizeHuman:        formatSize(st.size),
			LatestObjectTimestamp: formatTimestamp(st.latest),
		}
	}

	f, err := os.Create(*output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create %s: %v\n", *output, err)
		os.Exit(1)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", *output, err)
		os.Exit(1)
	}
	f.Close()
	fmt.Printf("Successfully saved data to %s\n", *output)

	ts := "null"
	if p := formatTimestamp(overallLatest); p != nil {
		ts = *p
	}
	fmt.Printf("\nBucket total: %d bytes (%s), newest object: %s\n", bucketTotal, formatSize(bucketTotal), ts)

	n := *top
	if n > len(names) {
		n = len(names)
	}
	fmt.Printf("\nTop %d folders by size:\n", n)
	for _, name := range names[:n] {
		st := sc.stats[name]
		tsStr := "-"
		if p := formatTimestamp(st.latest); p != nil {
			tsStr = *p
		}
		fmt.Printf("  %15s  %20s  %s/\n", formatSize(st.size), tsStr, name)
	}

	fmt.Printf("\nTotal elapsed time: %.2f seconds\n", time.Since(start).Seconds())
}
