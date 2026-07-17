"""
List every top-level folder in an S3 bucket with its total size and newest
object timestamp, plus the whole-bucket total.

Unlike the layout-specific scripts (list_data_prefixes.py,
list_backup_prefixes.py, list_system_table_prefixes.py), this one makes no
assumption about the bucket layout: it enumerates whatever first-level
prefixes exist (ch-s3-000/, ch-s3-{uuid}/, bare instance-uuid UDF dirs,
metrics/, ...) and sums each subtree. Objects sitting directly at the bucket
root (no folder) are reported under the pseudo-entry "(bucket root)".

Note: this is a full ListObjectsV2 walk of the bucket. It is exact and
real-time, but on very large buckets (hundreds of TB / billions of objects)
it can take hours — prefer CloudWatch BucketSizeBytes or S3 Inventory there.
"""

import argparse
import concurrent.futures
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from utils import (
    create_s3_client,
    sum_prefix_stats,
    format_size,
    format_timestamp,
    print_progress,
)


ROOT_OBJECTS_KEY = "(bucket root)"


def list_root_folders_and_objects(
    s3_client, bucket_name: str
) -> Tuple[List[str], int, Optional[datetime]]:
    """
    List the bucket root with Delimiter='/'.

    Returns (sorted first-level folder names without trailing slash,
    total size of objects sitting directly at the root,
    newest LastModified among those root objects or None).
    """
    folders: List[str] = []
    root_size = 0
    root_latest: Optional[datetime] = None
    continuation_token = None

    while True:
        params = {"Bucket": bucket_name, "Delimiter": "/"}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**params)

        for common_prefix in response.get("CommonPrefixes", []):
            folders.append(common_prefix["Prefix"].rstrip("/"))

        for obj in response.get("Contents", []):
            root_size += obj.get("Size", 0)
            last_modified = obj.get("LastModified")
            if last_modified and (
                root_latest is None or last_modified > root_latest
            ):
                root_latest = last_modified

        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break

    return sorted(folders), root_size, root_latest


def collect_top_level_prefixes(
    bucket_name: str, output_file: str, max_workers: int, top: int
):
    print(f"Using {max_workers} concurrent workers")

    s3_client = create_s3_client(max_workers)

    start_time = time.perf_counter()

    print(f"Listing first-level folders in bucket: {bucket_name}")
    folders, root_size, root_latest = list_root_folders_and_objects(
        s3_client, bucket_name
    )
    print(f"Found {len(folders)} first-level folder(s)")

    folder_to_size: Dict[str, int] = {}
    folder_to_latest: Dict[str, Optional[datetime]] = {}

    def stats_for_folder(folder: str) -> Tuple[str, int, Optional[datetime]]:
        try:
            total_size, latest_modified = sum_prefix_stats(
                s3_client, bucket_name, f"{folder}/"
            )
            return (folder, total_size, latest_modified)
        except Exception as e:
            print(f"Error counting objects for {folder}: {e}")
            return (folder, 0, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(stats_for_folder, f): f for f in folders}

        completed_count = 0
        total_count = len(folders)
        for future in concurrent.futures.as_completed(futures):
            folder, total_size, latest_modified = future.result()
            folder_to_size[folder] = total_size
            folder_to_latest[folder] = latest_modified
            completed_count += 1
            print_progress("Counting sizes", completed_count, total_count)

    if root_size or root_latest:
        folder_to_size[ROOT_OBJECTS_KEY] = root_size
        folder_to_latest[ROOT_OBJECTS_KEY] = root_latest

    bucket_total = sum(folder_to_size.values())
    overall_latest = max(
        (ts for ts in folder_to_latest.values() if ts is not None), default=None
    )

    # Descending by size, folder name as a stable tie-breaker.
    ordered_folders = sorted(
        folder_to_size, key=lambda name: (-folder_to_size[name], name)
    )

    output_data = {
        "folders": {
            folder: {
                "total_bytes": folder_to_size[folder],
                "total_size_human": format_size(folder_to_size[folder]),
                "latest_object_timestamp": format_timestamp(
                    folder_to_latest.get(folder)
                ),
            }
            for folder in ordered_folders
        },
        "summary": {
            "total_folders": len(folders),
            "bucket_total_size_bytes": bucket_total,
            "bucket_total_size_human": format_size(bucket_total),
            "latest_object_timestamp": format_timestamp(overall_latest),
        },
    }

    print(f"\nSaving to {output_file}...")
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"Successfully saved data to {output_file}")

    print(
        f"\nBucket total: {bucket_total} bytes ({format_size(bucket_total)}), "
        f"newest object: {format_timestamp(overall_latest)}"
    )
    print(f"\nTop {min(top, len(ordered_folders))} folders by size:")
    for folder in ordered_folders[:top]:
        print(
            f"  {format_size(folder_to_size[folder]):>15}  "
            f"{format_timestamp(folder_to_latest.get(folder)) or '-':>20}  "
            f"{folder}/"
        )

    elapsed = time.perf_counter() - start_time
    print(f"\nTotal elapsed time: {elapsed:.2f} seconds")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "List every first-level folder in an S3 bucket with its size and "
            "newest object timestamp, plus the bucket total"
        )
    )
    parser.add_argument("bucket_name", help="Name of the S3 bucket")
    parser.add_argument(
        "-o",
        "--output",
        default="top_level_prefixes.json",
        help="Output file name (default: top_level_prefixes.json)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=50,
        help="Number of concurrent workers (default: 50)",
    )
    parser.add_argument(
        "-t",
        "--top",
        type=int,
        default=20,
        help="How many largest folders to print to stdout (default: 20)",
    )

    args = parser.parse_args()

    collect_top_level_prefixes(args.bucket_name, args.output, args.workers, args.top)


if __name__ == "__main__":
    main()
