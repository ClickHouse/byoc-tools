"""
List every top-level folder in an S3 bucket with its total size and newest
object timestamp, plus the whole-bucket total.

Unlike the layout-specific scripts (list_data_prefixes.py,
list_backup_prefixes.py, list_system_table_prefixes.py), this one makes no
assumption about the bucket layout: it enumerates whatever first-level
prefixes exist (ch-s3-000/, ch-s3-{uuid}/, bare instance-uuid UDF dirs,
metrics/, ...) and sums each subtree. Objects sitting directly at the bucket
root (no folder) are reported under the pseudo-entry "(bucket root)".

Parallelism model: a single ListObjectsV2 pagination is inherently serial
(each page needs the previous page's continuation token), so scanning one
folder per worker degenerates to single-threaded whenever one folder holds
most of the bucket. Splitting is therefore adaptive: each work unit starts
as a flat pagination, and only when it exceeds --split-pages pages is it
abandoned and re-listed one directory level at a time (Delimiter='/'),
fanning its subfolders out as new parallel units — recursively, until every
unit fits the budget. Small folders cost exactly one LIST request (no
split overhead); only genuinely large subtrees pay the extra split requests
that buy them parallelism.

Note: this is still a full walk of the bucket. It is exact and real-time,
but on very large buckets (hundreds of TB / billions of objects) it can take
a long time — prefer CloudWatch BucketSizeBytes or S3 Inventory there.
"""

import argparse
import concurrent.futures
import json
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from utils import (
    create_s3_client,
    format_size,
    format_timestamp,
    print_progress,
)


ROOT_OBJECTS_KEY = "(bucket root)"


def list_with_delimiter(
    s3_client, bucket_name: str, prefix: str
) -> Tuple[List[str], int, Optional[datetime]]:
    """
    List one directory level (Delimiter='/') under a prefix.

    Returns (subfolder prefixes with trailing slash, total size of objects
    directly at this level, newest LastModified among those objects or None).
    """
    subfolders: List[str] = []
    direct_size = 0
    direct_latest: Optional[datetime] = None
    continuation_token = None

    while True:
        params = {"Bucket": bucket_name, "Prefix": prefix, "Delimiter": "/"}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**params)

        for common_prefix in response.get("CommonPrefixes", []):
            subfolders.append(common_prefix["Prefix"])

        for obj in response.get("Contents", []):
            direct_size += obj.get("Size", 0)
            last_modified = obj.get("LastModified")
            if last_modified and (
                direct_latest is None or last_modified > direct_latest
            ):
                direct_latest = last_modified

        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break

    return subfolders, direct_size, direct_latest


def scan_task(
    s3_client,
    bucket_name: str,
    top_folder: str,
    prefix: str,
    split_pages: int,
) -> Tuple[str, int, Optional[datetime], List[str]]:
    """
    Scan one unit of work, splitting adaptively.

    Start with a flat pagination of the subtree. If it finishes within
    split_pages pages, done. Otherwise the subtree is too big to scan
    serially: abandon the partial flat scan (its counts are discarded) and
    re-list this prefix one directory level deep (Delimiter='/') instead,
    returning the direct objects' stats plus the subfolders as new work
    units. A prefix whose objects all sit at one level has no subfolders to
    fan out, so the delimiter listing itself completes the count.

    Returns (top_folder, size, latest, child_unit_prefixes).
    """
    try:
        size = 0
        latest: Optional[datetime] = None
        pages = 0
        continuation_token = None

        while True:
            params = {"Bucket": bucket_name, "Prefix": prefix}
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**params)
            pages += 1

            for obj in response.get("Contents", []):
                size += obj.get("Size", 0)
                last_modified = obj.get("LastModified")
                if last_modified and (latest is None or last_modified > latest):
                    latest = last_modified

            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                return top_folder, size, latest, []
            if pages >= split_pages:
                break

        # Too big for one serial scan — split into subfolders instead.
        subfolders, direct_size, direct_latest = list_with_delimiter(
            s3_client, bucket_name, prefix
        )
        return top_folder, direct_size, direct_latest, subfolders
    except Exception as e:
        print(f"\nError scanning {prefix}: {e}")
        return top_folder, 0, None, []


def collect_top_level_prefixes(
    bucket_name: str,
    output_file: str,
    max_workers: int,
    top: int,
    split_pages: int,
):
    print(
        f"Using {max_workers} concurrent workers, "
        f"splitting folders larger than {split_pages} list pages"
    )

    s3_client = create_s3_client(max_workers)

    start_time = time.perf_counter()

    print(f"Listing first-level folders in bucket: {bucket_name}")
    folders_with_slash, root_size, root_latest = list_with_delimiter(
        s3_client, bucket_name, ""
    )
    folders = sorted(f.rstrip("/") for f in folders_with_slash)
    print(f"Found {len(folders)} first-level folder(s)")

    folder_to_size: Dict[str, int] = {folder: 0 for folder in folders}
    folder_to_latest: Dict[str, Optional[datetime]] = {
        folder: None for folder in folders
    }

    # Work units are subtree prefixes; a unit that exceeds the page budget
    # fans out into one unit per subfolder, so a single huge folder can't
    # serialize the scan while small folders stay a single request each.
    # Aggregation happens only in this thread, so no locking is needed.
    completed_units = 0
    total_units = len(folders)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                scan_task, s3_client, bucket_name, folder, f"{folder}/", split_pages
            )
            for folder in folders
        }

        while futures:
            done, futures = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                top_folder, size, latest, child_units = future.result()
                folder_to_size[top_folder] += size
                if latest and (
                    folder_to_latest[top_folder] is None
                    or latest > folder_to_latest[top_folder]
                ):
                    folder_to_latest[top_folder] = latest

                for child_prefix in child_units:
                    futures.add(
                        executor.submit(
                            scan_task,
                            s3_client,
                            bucket_name,
                            top_folder,
                            child_prefix,
                            split_pages,
                        )
                    )
                total_units += len(child_units)
                completed_units += 1
            print_progress("Scanning units", completed_units, total_units)

    sys.stdout.write("\n")

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
    parser.add_argument(
        "-s",
        "--split-pages",
        type=int,
        default=20,
        help=(
            "Page budget (1000 objects per page) above which a subtree is "
            "split into parallel per-subfolder scans instead of one serial "
            "pagination. Lower = more parallelism (and more LIST requests) "
            "on large folders; small folders always cost one request. "
            "(default: 20)"
        ),
    )

    args = parser.parse_args()

    if args.split_pages < 1:
        parser.error("--split-pages must be >= 1")

    collect_top_level_prefixes(
        args.bucket_name, args.output, args.workers, args.top, args.split_pages
    )


if __name__ == "__main__":
    main()
