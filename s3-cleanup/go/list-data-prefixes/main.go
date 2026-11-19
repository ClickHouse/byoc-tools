// list-data-prefixes: Go implementation of list_data_prefixes.py.
//
// Scans the full ch-s3-000 ~ ch-s3-fff shard space of a BYOC shared data
// bucket, discovers every next-level (instance UUID) prefix, and sums each
// prefix's total size and newest object timestamp. Output JSON is
// shape-compatible with the Python script (data_prefixes.json), so it feeds
// straight into get_final_dirty_data_prefix.py.
//
// Built for throughput: shard discovery and per-prefix stats scans are
// pipelined — as soon as one ch-s3-xxx shard's delimiter listing returns,
// its prefixes' stats scans start, while other shards are still listing.
// A single -w cap bounds total in-flight S3 requests.
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

type prefixStat struct {
	size   int64
	latest time.Time // zero value = no objects seen
}

type lister struct {
	client *s3.Client
	bucket string

	sem chan struct{} // caps in-flight S3 requests (both phases combined)
	wg  sync.WaitGroup

	mu            sync.Mutex
	stats         map[string]*prefixStat // keyed by full path ch-s3-xxx/uuid
	shardPrefixes map[string][]string    // ch-s3-xxx -> next-level prefix names

	shardsDone atomic.Int64
	pathsDone  atomic.Int64
	pathsTotal atomic.Int64
	errCount   atomic.Int64
}

// listShard delimiter-lists one ch-s3-xxx/ shard and fans each discovered
// next-level prefix out as a concurrent stats scan.
func (l *lister) listShard(ctx context.Context, shard string) {
	defer l.wg.Done()
	defer l.shardsDone.Add(1)

	parent := shard + "/"
	var names []string
	var continuationToken *string
	for {
		l.sem <- struct{}{}
		out, err := l.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(l.bucket),
			Prefix:            aws.String(parent),
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
		})
		<-l.sem
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError processing %s: %v\n", shard, err)
			l.errCount.Add(1)
			return
		}
		for _, cp := range out.CommonPrefixes {
			name := strings.TrimSuffix(strings.TrimPrefix(*cp.Prefix, parent), "/")
			if name != "" {
				names = append(names, name)
			}
		}
		if out.NextContinuationToken == nil {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	sort.Strings(names)
	l.mu.Lock()
	l.shardPrefixes[shard] = names
	for _, name := range names {
		l.stats[shard+"/"+name] = &prefixStat{}
	}
	l.mu.Unlock()

	l.pathsTotal.Add(int64(len(names)))
	for _, name := range names {
		l.wg.Add(1)
		go l.scanPrefix(ctx, shard+"/"+name)
	}
}

// scanPrefix flat-paginates one full path, summing sizes and tracking the
// newest object timestamp.
func (l *lister) scanPrefix(ctx context.Context, fullPath string) {
	defer l.wg.Done()
	defer l.pathsDone.Add(1)

	var size int64
	var latest time.Time
	var continuationToken *string
	for {
		l.sem <- struct{}{}
		out, err := l.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(l.bucket),
			Prefix:            aws.String(fullPath + "/"),
			ContinuationToken: continuationToken,
		})
		<-l.sem
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError counting objects for %s: %v\n", fullPath, err)
			l.errCount.Add(1)
			return
		}
		for _, obj := range out.Contents {
			size += aws.ToInt64(obj.Size)
			if obj.LastModified != nil && obj.LastModified.After(latest) {
				latest = *obj.LastModified
			}
		}
		if out.NextContinuationToken == nil {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	l.mu.Lock()
	st := l.stats[fullPath]
	st.size = size
	st.latest = latest
	l.mu.Unlock()
}

func formatSize(sizeBytes int64) string {
	const kb = 1 << 10
	const mb = 1 << 20
	const gb = 1 << 30
	const tb = 1 << 40
	switch {
	case sizeBytes >= tb:
		return fmt.Sprintf("%.2f TB", float64(sizeBytes)/float64(tb))
	case sizeBytes >= gb:
		return fmt.Sprintf("%.2f GB", float64(sizeBytes)/float64(gb))
	case sizeBytes >= mb:
		return fmt.Sprintf("%.2f MB", float64(sizeBytes)/float64(mb))
	case sizeBytes >= kb:
		return fmt.Sprintf("%.2f KB", float64(sizeBytes)/float64(kb))
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

type outputJSON struct {
	Prefixes                     []string           `json:"prefixes"`
	PrefixSizesBytes             map[string]int64   `json:"prefix_sizes_bytes"`
	PrefixLatestObjectTimestamps map[string]*string `json:"prefix_latest_object_timestamps"`
	Summary                      summaryJSON        `json:"summary"`
}

type summaryJSON struct {
	TotalUniquePrefixes    int     `json:"total_unique_prefixes"`
	TotalUUIDsWithPrefixes int     `json:"total_uuids_with_prefixes"`
	TotalFullPaths         int     `json:"total_full_paths"`
	TotalSizeBytes         int64   `json:"total_size_bytes"`
	TotalSizeHuman         string  `json:"total_size_human"`
	LatestObjectTimestamp  *string `json:"latest_object_timestamp"`
}

func main() {
	output := flag.String("o", "data_prefixes.json", "output file name")
	workers := flag.Int("w", 200, "number of concurrent S3 requests")
	region := flag.String("region", "", "AWS region override (default: from profile/env)")
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: list-data-prefixes [flags] <bucket>")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if *workers < 1 {
		fmt.Fprintln(os.Stderr, "-w must be >= 1")
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

	l := &lister{
		client:        s3.NewFromConfig(cfg),
		bucket:        bucket,
		sem:           make(chan struct{}, *workers),
		stats:         map[string]*prefixStat{},
		shardPrefixes: map[string][]string{},
	}

	// Preflight: fail fast with one clear error (bad credentials, wrong
	// region, missing bucket) instead of 4096 identical per-shard errors
	// and a bogus empty output file.
	if _, err := l.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(1),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "cannot access bucket %s: %v\n", bucket, err)
		fmt.Fprintln(os.Stderr, "hint: set AWS_PROFILE (e.g. AWS_PROFILE=BYOC_Test_Dev_Admin) and pass -region if the bucket is not in the profile's default region")
		os.Exit(1)
	}

	const totalShards = 0x1000
	fmt.Printf("Scanning %d ch-s3-* prefixes in bucket: %s, using %d concurrent requests\n",
		totalShards, bucket, *workers)

	start := time.Now()

	for i := 0; i < totalShards; i++ {
		shard := fmt.Sprintf("ch-s3-%03x", i)
		l.wg.Add(1)
		go l.listShard(ctx, shard)
	}

	progressDone := make(chan struct{})
	progressExited := make(chan struct{})
	go func() {
		defer close(progressExited)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fmt.Printf("\rShards %d/%d, prefix scans %d/%d",
					l.shardsDone.Load(), totalShards, l.pathsDone.Load(), l.pathsTotal.Load())
			case <-progressDone:
				fmt.Printf("\rShards %d/%d, prefix scans %d/%d\n",
					l.shardsDone.Load(), totalShards, l.pathsDone.Load(), l.pathsTotal.Load())
				return
			}
		}
	}()

	l.wg.Wait()
	close(progressDone)
	<-progressExited

	fullPaths := make([]string, 0, len(l.stats))
	for path := range l.stats {
		fullPaths = append(fullPaths, path)
	}
	sort.Strings(fullPaths)

	uniqueNames := map[string]struct{}{}
	shardsWithPrefixes := 0
	for _, names := range l.shardPrefixes {
		if len(names) > 0 {
			shardsWithPrefixes++
		}
		for _, name := range names {
			uniqueNames[name] = struct{}{}
		}
	}

	var totalSize int64
	var overallLatest time.Time
	for _, st := range l.stats {
		totalSize += st.size
		if st.latest.After(overallLatest) {
			overallLatest = st.latest
		}
	}

	out := outputJSON{
		Prefixes:                     fullPaths,
		PrefixSizesBytes:             make(map[string]int64, len(fullPaths)),
		PrefixLatestObjectTimestamps: make(map[string]*string, len(fullPaths)),
		Summary: summaryJSON{
			TotalUniquePrefixes:    len(uniqueNames),
			TotalUUIDsWithPrefixes: shardsWithPrefixes,
			TotalFullPaths:         len(fullPaths),
			TotalSizeBytes:         totalSize,
			TotalSizeHuman:         formatSize(totalSize),
			LatestObjectTimestamp:  formatTimestamp(overallLatest),
		},
	}
	for _, path := range fullPaths {
		st := l.stats[path]
		out.PrefixSizesBytes[path] = st.size
		out.PrefixLatestObjectTimestamps[path] = formatTimestamp(st.latest)
	}

	fmt.Printf("\nTotal unique next-level prefixes found: %d\n", len(uniqueNames))
	fmt.Printf("Total UUIDs with prefixes: %d\n", shardsWithPrefixes)
	fmt.Printf("Total full paths: %d\n", len(fullPaths))
	fmt.Printf("Total size across all prefixes: %d bytes (%s)\n", totalSize, formatSize(totalSize))
	fmt.Printf("Saving to %s...\n", *output)

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

	if len(fullPaths) > 0 {
		var minSize, maxSize int64
		minSize = -1
		for _, st := range l.stats {
			if minSize < 0 || st.size < minSize {
				minSize = st.size
			}
			if st.size > maxSize {
				maxSize = st.size
			}
		}
		avg := totalSize / int64(len(fullPaths))
		fmt.Println("\nStatistics:")
		fmt.Printf("  Min size per prefix: %d bytes (%s)\n", minSize, formatSize(minSize))
		fmt.Printf("  Max size per prefix: %d bytes (%s)\n", maxSize, formatSize(maxSize))
		fmt.Printf("  Average size per prefix: %d bytes (%s)\n", avg, formatSize(avg))
	}

	fmt.Printf("\nTotal elapsed time: %.2f seconds\n", time.Since(start).Seconds())

	if n := l.errCount.Load(); n > 0 {
		fmt.Fprintf(os.Stderr, "\n⚠️  %d scan errors — output is INCOMPLETE, do not feed it into deletion\n", n)
		os.Exit(1)
	}
}
