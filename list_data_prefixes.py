import argparse
import concurrent.futures
import json
import time
from typing import Set, List, Dict, Tuple

from utils import (
    create_s3_client,
    list_next_level_prefixes,
    sum_sizes_in_prefix,
    format_size,
    print_progress,
)


def list_prefixes_for_ch_s3_prefix(
    s3_client, bucket_name: str, ch_s3_prefix: str
) -> Set[str]:
    """
    List next-level prefixes for a single ch-s3-{xxx} prefix.
    """
    full_prefix = f"{ch_s3_prefix}/"
    return list_next_level_prefixes(s3_client, bucket_name, full_prefix)


def collect_all_next_level_prefixes(
    bucket_name: str, output_file: str, max_workers: int = 50
):
    """
    Collect all next-level prefixes under ch-s3-{000~fff}.
    Returns a list of all full paths (ch-s3-xxx/prefix).
    """
    print(f"Using {max_workers} concurrent workers")

    # Create S3 client with appropriate connection pool
    s3_client = create_s3_client(max_workers)

    # Always scan the full expected space ch-s3-000 ~ ch-s3-fff
    ch_s3_prefixes = [f"ch-s3-{i:03x}" for i in range(0x000, 0x1000)]

    total_count = len(ch_s3_prefixes)
    print(
        f"Scanning {total_count} ch-s3-* prefixes in bucket: {bucket_name}, using {max_workers} concurrent workers"
    )

    start_time = time.perf_counter()

    # Dictionary to store uuid -> list of prefixes mapping
    uuid_to_prefixes: Dict[str, List[str]] = {}
    # Dictionary to store prefix -> total size mapping (bytes)
    prefix_to_size: Dict[str, int] = {}
    all_next_level_prefixes = set()

    # Use concurrent processing to speed up
    def process_prefix(ch_s3_prefix: str) -> Tuple[str, List[str]]:
        try:
            prefixes_set = list_prefixes_for_ch_s3_prefix(
                s3_client, bucket_name, ch_s3_prefix
            )
            prefixes_list = sorted(list(prefixes_set))
            return (ch_s3_prefix, prefixes_list)
        except Exception as e:
            print(f"Error processing {ch_s3_prefix}: {e}")
            return (ch_s3_prefix, [])

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_prefix, prefix): prefix for prefix in ch_s3_prefixes
        }

        completed_count = 0
        for future in concurrent.futures.as_completed(futures):
            ch_s3_prefix, prefixes = future.result()
            uuid_to_prefixes[ch_s3_prefix] = prefixes
            all_next_level_prefixes.update(prefixes)
            completed_count += 1
            print_progress("Listing prefixes", completed_count, total_count)

    # Collect all full paths and count objects for each prefix
    print("\nCounting objects in each prefix...")
    all_full_paths = []

    for ch_s3_prefix, prefixes in uuid_to_prefixes.items():
        for prefix in prefixes:
            full_path = f"{ch_s3_prefix}/{prefix}"
            all_full_paths.append(full_path)

    # Sort for consistent output
    sorted_full_paths = sorted(all_full_paths)

    # Sum sizes for each prefix concurrently
    def size_for_prefix(full_path: str) -> Tuple[str, int]:
        try:
            total_size = sum_sizes_in_prefix(s3_client, bucket_name, f"{full_path}/")
            return (full_path, total_size)
        except Exception as e:
            print(f"Error counting objects for {full_path}: {e}")
            return (full_path, 0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        size_futures = {
            executor.submit(size_for_prefix, path): path for path in sorted_full_paths
        }

        completed_count = 0
        total_sizes = len(sorted_full_paths)
        for future in concurrent.futures.as_completed(size_futures):
            full_path, total_size = future.result()
            prefix_to_size[full_path] = total_size
            completed_count += 1
            print_progress("Counting sizes", completed_count, total_sizes)

    # Prepare output data with counts
    output_data = {
        "prefixes": sorted_full_paths,
        "prefix_sizes_bytes": prefix_to_size,
        "summary": {
            "total_unique_prefixes": len(all_next_level_prefixes),
            "total_uuids_with_prefixes": len(
                [u for u in uuid_to_prefixes.keys() if uuid_to_prefixes[u]]
            ),
            "total_full_paths": len(sorted_full_paths),
            "total_size_bytes": sum(prefix_to_size.values()),
            "total_size_human": format_size(sum(prefix_to_size.values())),
        },
    }

    # Save to file in JSON format
    print(f"\nTotal unique next-level prefixes found: {len(all_next_level_prefixes)}")
    print(
        f"Total UUIDs with prefixes: {len([u for u in uuid_to_prefixes.keys() if uuid_to_prefixes[u]])}"
    )
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
        description="List all data prefixes under ch-s3-{000~fff} in an S3 bucket"
    )
    parser.add_argument("bucket_name", help="Name of the S3 bucket")
    parser.add_argument(
        "-o",
        "--output",
        default="data_prefixes.json",
        help="Output file name (default: data_prefixes.json)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=50,
        help="Number of concurrent workers (default: 50)",
    )

    args = parser.parse_args()

    collect_all_next_level_prefixes(args.bucket_name, args.output, args.workers)


if __name__ == "__main__":
    main()
