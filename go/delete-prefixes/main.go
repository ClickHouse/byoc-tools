// delete-prefixes: Go implementation of delete_prefixes.py.
//
// Deletes all S3 objects under the dirty prefixes identified by
// get_final_dirty_data_prefix.py / get_final_dirty_backup_prefix.py.
// Reads the same input JSON and writes the same log JSON shape, but is
// built for throughput: listing and deleting are pipelined — every LIST
// page (1000 keys) is handed to a concurrent DeleteObjects batch
// immediately instead of listing the whole prefix first — DeleteObjects
// runs in Quiet mode, and keys that fail inside a batch are retried.
// Concurrency is a single global cap on in-flight S3 requests shared by
// LIST and DELETE, so -w can safely go into the hundreds.
//
// On versioned buckets this creates delete markers (no VersionId is
// passed), same as the Python script.
package main

import (
	"bufio"
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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	maxDeleteAttempts = 3
	retryBaseDelay    = 500 * time.Millisecond
)

type inputJSON struct {
	DirtyPaths      []string         `json:"dirty_paths"`
	DirtyPathsSizes map[string]int64 `json:"dirty_paths_sizes_bytes"`
}

type prefixResult struct {
	found   int64
	deleted int64
	errors  []string
}

type deleter struct {
	client *s3.Client
	bucket string
	dryRun bool

	sem chan struct{} // caps in-flight S3 requests (LIST + DELETE combined)
	wg  sync.WaitGroup

	mu      sync.Mutex
	results map[string]*prefixResult

	prefixesDone   atomic.Int64
	objectsFound   atomic.Int64
	objectsDeleted atomic.Int64
}

func (d *deleter) record(prefix string, found, deleted int64, errs []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r := d.results[prefix]
	r.found += found
	r.deleted += deleted
	r.errors = append(r.errors, errs...)
}

// deleteBatch deletes up to 1000 keys in one DeleteObjects call. Keys that
// come back in the per-key Errors list are retried with backoff; a failed
// API call retries the whole batch (on top of the SDK's own retryer).
func (d *deleter) deleteBatch(ctx context.Context, prefix string, keys []types.ObjectIdentifier) {
	defer d.wg.Done()

	remaining := keys
	for attempt := 1; ; attempt++ {
		d.sem <- struct{}{}
		out, err := d.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(d.bucket),
			Delete: &types.Delete{Objects: remaining, Quiet: aws.Bool(true)},
		})
		<-d.sem

		if err != nil {
			if attempt < maxDeleteAttempts {
				time.Sleep(time.Duration(attempt) * retryBaseDelay)
				continue
			}
			d.record(prefix, 0, 0, []string{fmt.Sprintf("DeleteObjects failed for %d keys: %v", len(remaining), err)})
			return
		}

		deleted := int64(len(remaining) - len(out.Errors))
		d.objectsDeleted.Add(deleted)
		d.record(prefix, 0, deleted, nil)

		if len(out.Errors) == 0 {
			return
		}
		if attempt >= maxDeleteAttempts {
			errs := make([]string, 0, len(out.Errors))
			for _, e := range out.Errors {
				errs = append(errs, fmt.Sprintf("%s: %s - %s", aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message)))
			}
			d.record(prefix, 0, 0, errs)
			return
		}
		remaining = remaining[:0]
		for _, e := range out.Errors {
			remaining = append(remaining, types.ObjectIdentifier{Key: e.Key})
		}
		time.Sleep(time.Duration(attempt) * retryBaseDelay)
	}
}

// deletePrefix paginates the prefix and fans each page out as a concurrent
// delete batch, so deletion starts while listing is still in flight.
func (d *deleter) deletePrefix(ctx context.Context, prefix string) {
	defer d.wg.Done()
	defer d.prefixesDone.Add(1)

	var continuationToken *string
	for {
		d.sem <- struct{}{}
		out, err := d.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(d.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		<-d.sem
		if err != nil {
			d.record(prefix, 0, 0, []string{fmt.Sprintf("ListObjectsV2 failed: %v", err)})
			return
		}

		if n := len(out.Contents); n > 0 {
			d.objectsFound.Add(int64(n))
			d.record(prefix, int64(n), 0, nil)
			if !d.dryRun {
				ids := make([]types.ObjectIdentifier, 0, n)
				for _, obj := range out.Contents {
					ids = append(ids, types.ObjectIdentifier{Key: obj.Key})
				}
				d.wg.Add(1)
				go d.deleteBatch(ctx, prefix, ids)
			}
		}

		if out.NextContinuationToken == nil {
			return
		}
		continuationToken = out.NextContinuationToken
	}
}

func loadDirtyPaths(filename string) ([]string, map[string]int64, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	var in inputJSON
	if err := json.NewDecoder(f).Decode(&in); err != nil {
		return nil, nil, fmt.Errorf("invalid JSON: %w", err)
	}
	sizes := in.DirtyPathsSizes
	if sizes == nil {
		sizes = map[string]int64{}
	}
	return in.DirtyPaths, sizes, nil
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

func displaySummary(dirtyPaths []string, sizes map[string]int64, bucket string, dryRun bool) {
	line := strings.Repeat("=", 70)
	fmt.Println("\n" + line)
	fmt.Println("DELETION SUMMARY")
	fmt.Println(line)
	fmt.Printf("Bucket: %s\n", bucket)
	mode := "LIVE DELETION"
	if dryRun {
		mode = "DRY RUN (no actual deletion)"
	}
	fmt.Printf("Mode: %s\n", mode)
	fmt.Printf("Total prefixes to process: %d\n", len(dirtyPaths))

	if len(sizes) > 0 {
		var total int64
		for _, p := range dirtyPaths {
			total += sizes[p]
		}
		fmt.Printf("Total size: %d bytes (%s)\n", total, formatSize(total))
	}

	fmt.Println("\nSample prefixes (first 10):")
	for i, p := range dirtyPaths {
		if i >= 10 {
			fmt.Printf("  ... and %d more\n", len(dirtyPaths)-10)
			break
		}
		sizeInfo := ""
		if sz, ok := sizes[p]; ok {
			sizeInfo = fmt.Sprintf(" (%s)", formatSize(sz))
		}
		fmt.Printf("  %d. %s%s\n", i+1, p, sizeInfo)
	}
	fmt.Println(line)
}

func getConfirmation() bool {
	fmt.Println("\n⚠️  WARNING: This will permanently delete objects from S3!")
	fmt.Println("This action cannot be undone.")
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("\nType 'yes' to confirm deletion, or 'no' to cancel: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			return false
		}
		switch strings.ToLower(strings.TrimSpace(line)) {
		case "yes":
			return true
		case "no":
			return false
		default:
			fmt.Println("Please type 'yes' or 'no'")
		}
	}
}

type prefixLogJSON struct {
	TotalObjects int64    `json:"total_objects"`
	Deleted      int64    `json:"deleted"`
	Errors       []string `json:"errors"`
}

type logJSON struct {
	Bucket              string                   `json:"bucket"`
	InputFile           string                   `json:"input_file"`
	DryRun              bool                     `json:"dry_run"`
	TotalPrefixes       int                      `json:"total_prefixes"`
	TotalObjectsFound   int64                    `json:"total_objects_found"`
	TotalObjectsDeleted int64                    `json:"total_objects_deleted"`
	TotalErrors         int                      `json:"total_errors"`
	Results             map[string]prefixLogJSON `json:"results"`
}

func main() {
	input := flag.String("i", "dirty_data_result.json", "input JSON file with dirty paths")
	workers := flag.Int("w", 200, "number of concurrent S3 requests (LIST + DELETE combined)")
	dryRun := flag.Bool("dry-run", false, "show what would be deleted without actually deleting")
	outputLog := flag.String("o", "", "optional log file to save deletion results")
	yes := flag.Bool("yes", false, "skip the interactive confirmation prompt")
	region := flag.String("region", "", "AWS region override (default: from profile/env)")
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: delete-prefixes [flags] <bucket>")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if *workers < 1 {
		fmt.Fprintln(os.Stderr, "-w must be >= 1")
		os.Exit(2)
	}
	bucket := flag.Arg(0)

	start := time.Now()

	fmt.Printf("Loading dirty paths from %s...\n", *input)
	dirtyPaths, sizes, err := loadDirtyPaths(*input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading input file: %v\n", err)
		os.Exit(1)
	}
	if len(dirtyPaths) == 0 {
		fmt.Println("No dirty paths found in input file. Nothing to delete.")
		return
	}
	fmt.Printf("Loaded %d dirty paths\n", len(dirtyPaths))

	displaySummary(dirtyPaths, sizes, bucket, *dryRun)

	if !*dryRun && !*yes {
		if !getConfirmation() {
			fmt.Println("\nDeletion cancelled.")
			return
		}
	}

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

	d := &deleter{
		client:  s3.NewFromConfig(cfg),
		bucket:  bucket,
		dryRun:  *dryRun,
		sem:     make(chan struct{}, *workers),
		results: make(map[string]*prefixResult, len(dirtyPaths)),
	}
	for _, p := range dirtyPaths {
		d.results[p] = &prefixResult{}
	}

	action := "Starting deletion"
	if *dryRun {
		action = "Simulating deletion"
	}
	fmt.Printf("\n%s with %d concurrent S3 requests...\n", action, *workers)

	for _, p := range dirtyPaths {
		d.wg.Add(1)
		go d.deletePrefix(ctx, p)
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
				fmt.Printf("\rPrefixes %d/%d, objects found %d, deleted %d",
					d.prefixesDone.Load(), len(dirtyPaths), d.objectsFound.Load(), d.objectsDeleted.Load())
			case <-progressDone:
				fmt.Printf("\rPrefixes %d/%d, objects found %d, deleted %d\n",
					d.prefixesDone.Load(), len(dirtyPaths), d.objectsFound.Load(), d.objectsDeleted.Load())
				return
			}
		}
	}()

	d.wg.Wait()
	close(progressDone)
	<-progressExited

	totalErrors := 0
	var failedPrefixes []string
	for _, p := range dirtyPaths {
		r := d.results[p]
		totalErrors += len(r.errors)
		if len(r.errors) > 0 || (!*dryRun && r.deleted < r.found) {
			failedPrefixes = append(failedPrefixes, p)
		}
	}
	sort.Strings(failedPrefixes)

	line := strings.Repeat("=", 70)
	fmt.Println("\n" + line)
	fmt.Println("DELETION RESULTS")
	fmt.Println(line)
	fmt.Printf("Total prefixes processed: %d\n", len(dirtyPaths))
	fmt.Printf("Total objects found: %d\n", d.objectsFound.Load())
	if !*dryRun {
		fmt.Printf("Total objects deleted: %d\n", d.objectsDeleted.Load())
		fmt.Printf("Total errors: %d\n", totalErrors)
	}

	if len(failedPrefixes) > 0 {
		fmt.Printf("\n⚠️  %d prefixes had errors:\n", len(failedPrefixes))
		for i, p := range failedPrefixes {
			if i >= 10 {
				fmt.Printf("  ... and %d more prefixes with errors\n", len(failedPrefixes)-10)
				break
			}
			r := d.results[p]
			fmt.Printf("  - %s: %d/%d deleted\n", p, r.deleted, r.found)
			for j, e := range r.errors {
				if j >= 3 {
					fmt.Printf("    ... and %d more errors\n", len(r.errors)-3)
					break
				}
				fmt.Printf("    Error: %s\n", e)
			}
		}
	}

	if *outputLog != "" {
		lg := logJSON{
			Bucket:              bucket,
			InputFile:           *input,
			DryRun:              *dryRun,
			TotalPrefixes:       len(dirtyPaths),
			TotalObjectsFound:   d.objectsFound.Load(),
			TotalObjectsDeleted: d.objectsDeleted.Load(),
			TotalErrors:         totalErrors,
			Results:             make(map[string]prefixLogJSON, len(dirtyPaths)),
		}
		for p, r := range d.results {
			errs := r.errors
			if errs == nil {
				errs = []string{}
			}
			lg.Results[p] = prefixLogJSON{TotalObjects: r.found, Deleted: r.deleted, Errors: errs}
		}
		f, err := os.Create(*outputLog)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create %s: %v\n", *outputLog, err)
			os.Exit(1)
		}
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(lg); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", *outputLog, err)
			os.Exit(1)
		}
		f.Close()
		fmt.Printf("\nLog saved to %s\n", *outputLog)
	}

	fmt.Printf("\nTotal elapsed time: %.2f seconds\n", time.Since(start).Seconds())

	if len(failedPrefixes) > 0 && !*dryRun {
		os.Exit(1)
	}
}
