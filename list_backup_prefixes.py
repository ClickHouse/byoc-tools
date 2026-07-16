import argparse
import concurrent.futures
import json
import time
from datetime import datetime
from typing import Optional, Set, List, Dict, Tuple

from utils import (
    create_s3_client,
    discover_ch_s3_prefixes,
    list_next_level_prefixes,
    sum_prefix_stats,
    format_size,
    format_timestamp,
    print_progress,
)


def list_uuids_under_backup_prefix(
    s3_client, bucket_name: str, ch_s3_prefix: str
) -> Set[str]:
    """
    List all UUIDs under a ch-s3-uuid/ prefix.
    
    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the S3 bucket
        ch_s3_prefix: ch-s3-uuid prefix (e.g., 'ch-s3-008cf44d-1e83-40ab-b6bc-6f0a8b4683b4')
    
    Returns:
        Set of UUID strings found under the prefix
    """
    full_prefix = f"{ch_s3_prefix}/"
    return list_next_level_prefixes(s3_client, bucket_name, full_prefix)


def collect_all_backup_prefixes(
    bucket_name: str, output_file: str, max_workers: int = 50
):
    """
    Collect all backup prefixes in format ch-s3-uuid/uuid.
    Returns a list of all full paths (ch-s3-uuid/uuid).
    
    Args:
        bucket_name: Name of the S3 bucket
        output_file: Output JSON file path
        max_workers: Number of concurrent workers
    """
    print(f"Using {max_workers} concurrent workers")

    # Create S3 client with appropriate connection pool
    s3_client = create_s3_client(max_workers)

    start_time = time.perf_counter()

    # Phase 1: Discover all ch-s3-* prefixes
    print("Discovering ch-s3-* prefixes...")
    ch_s3_prefixes = discover_ch_s3_prefixes(s3_client, bucket_name)
    print(f"Found {len(ch_s3_prefixes)} ch-s3-* prefixes with valid UUID format")

    if not ch_s3_prefixes:
        print("No ch-s3-* prefixes found. Exiting.")
        return []

    # Dictionary to store ch-s3-uuid -> list of UUIDs mapping
    ch_s3_to_uuids: Dict[str, List[str]] = {}
    # Dictionary to store prefix -> total size mapping (bytes)
    prefix_to_size: Dict[str, int] = {}
    # Dictionary to store prefix -> newest object LastModified mapping
    prefix_to_latest: Dict[str, Optional[datetime]] = {}
    all_uuids = set()

    # Phase 2: For each ch-s3-uuid, list all UUIDs underneath
    def process_backup_prefix(ch_s3_prefix: str) -> Tuple[str, List[str]]:
        try:
            uuids_set = list_uuids_under_backup_prefix(
                s3_client, bucket_name, ch_s3_prefix
            )
            uuids_list = sorted(list(uuids_set))
            return (ch_s3_prefix, uuids_list)
        except Exception as e:
            print(f"Error processing {ch_s3_prefix}: {e}")
            return (ch_s3_prefix, [])

    print("\nListing UUIDs under each ch-s3-* prefix...")
    total_count = len(ch_s3_prefixes)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_backup_prefix, prefix): prefix
            for prefix in ch_s3_prefixes
        }

        completed_count = 0
        for future in concurrent.futures.as_completed(futures):
            ch_s3_prefix, uuids = future.result()
            ch_s3_to_uuids[ch_s3_prefix] = uuids
            all_uuids.update(uuids)
            completed_count += 1
            print_progress("Listing UUIDs", completed_count, total_count)

    # Collect all full paths
    print("\nCollecting full paths...")
    all_full_paths = []

    for ch_s3_prefix, uuids in ch_s3_to_uuids.items():
        for uuid in uuids:
            full_path = f"{ch_s3_prefix}/{uuid}"
            all_full_paths.append(full_path)

    # Sort for consistent output
    sorted_full_paths = sorted(all_full_paths)

    # Phase 3: Calculate sizes and newest object timestamps for each prefix concurrently
    print("\nCounting objects in each prefix...")

    def stats_for_prefix(full_path: str) -> Tuple[str, int, Optional[datetime]]:
        try:
            total_size, latest_modified = sum_prefix_stats(
                s3_client, bucket_name, f"{full_path}/"
            )
            return (full_path, total_size, latest_modified)
        except Exception as e:
            print(f"Error counting objects for {full_path}: {e}")
            return (full_path, 0, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        size_futures = {
            executor.submit(stats_for_prefix, path): path for path in sorted_full_paths
        }

        completed_count = 0
        total_sizes = len(sorted_full_paths)
        for future in concurrent.futures.as_completed(size_futures):
            full_path, total_size, latest_modified = future.result()
            prefix_to_size[full_path] = total_size
            prefix_to_latest[full_path] = latest_modified
            completed_count += 1
            print_progress("Counting sizes", completed_count, total_sizes)

    overall_latest = max(
        (ts for ts in prefix_to_latest.values() if ts is not None), default=None
    )

    # Prepare output data with counts
    output_data = {
        "prefixes": sorted_full_paths,
        "prefix_sizes_bytes": prefix_to_size,
        "prefix_latest_object_timestamps": {
            path: format_timestamp(prefix_to_latest.get(path))
            for path in sorted_full_paths
        },
        "summary": {
            "total_unique_uuids": len(all_uuids),
            "total_ch_s3_prefixes": len(ch_s3_prefixes),
            "total_full_paths": len(sorted_full_paths),
            "total_size_bytes": sum(prefix_to_size.values()),
            "total_size_human": format_size(sum(prefix_to_size.values())),
            "latest_object_timestamp": format_timestamp(overall_latest),
        },
    }

    # Save to file in JSON format
    print(f"\nTotal unique UUIDs found: {len(all_uuids)}")
    print(f"Total ch-s3-* prefixes: {len(ch_s3_prefixes)}")
    print(f"Total full paths: {len(sorted_full_paths)}")
    total_bytes = sum(prefix_to_size.values())
    print(
        f"Total size across all prefixes: {total_bytes} bytes ({format_size(total_bytes)})"
    )
    print(f"Saving to {output_file}...")

    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"Successfully saved data to {output_file}")

    # Print some statistics
    if prefix_to_size:
        sizes = list(prefix_to_size.values())
        print(f"\nStatistics:")
        print(f"  Min size per prefix: {min(sizes)} bytes ({format_size(min(sizes))})")
        print(f"  Max size per prefix: {max(sizes)} bytes ({format_size(max(sizes))})")
        print(
            f"  Average size per prefix: {sum(sizes) / len(sizes):.2f} bytes ({format_size(int(sum(sizes) / len(sizes)))})"
        )

    elapsed = time.perf_counter() - start_time
    print(f"\nTotal elapsed time: {elapsed:.2f} seconds")

    return sorted_full_paths


def main():
    parser = argparse.ArgumentParser(
        description="List all backup prefixes under ch-s3-uuid/uuid in an S3 bucket"
    )
    parser.add_argument("bucket_name", help="Name of the S3 bucket")
    parser.add_argument(
        "-o",
        "--output",
        default="backup_prefixes.json",
        help="Output file name (default: backup_prefixes.json)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=50,
        help="Number of concurrent workers (default: 50)",
    )

    args = parser.parse_args()

    collect_all_backup_prefixes(args.bucket_name, args.output, args.workers)


if __name__ == "__main__":
    main()

