#!/usr/bin/env python3
"""
Delete S3 objects under dirty prefixes identified by get_final_dirty_data_prefix.py
or get_final_dirty_backup_prefix.py.

This script reads the output JSON from those scripts and deletes all objects
under the specified prefixes with manual confirmation and batch deletion support.
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Tuple

import boto3
from botocore.exceptions import ClientError

from utils import create_s3_client, format_size, print_progress, list_all_objects


def load_dirty_paths(filename: str) -> Tuple[List[str], Dict[str, int]]:
    """
    Load dirty paths from JSON file.
    Returns (dirty_paths_list, dirty_paths_sizes_dict).
    
    Args:
        filename: Path to JSON file with dirty paths
        
    Returns:
        Tuple of (dirty paths list, sizes dictionary)
    """
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object, got {type(data)}")
    
    dirty_paths = data.get("dirty_paths", [])
    if not isinstance(dirty_paths, list):
        raise ValueError(f"Expected 'dirty_paths' to be a list, got {type(dirty_paths)}")
    
    sizes = data.get("dirty_paths_sizes_bytes", {})
    if not isinstance(sizes, dict):
        sizes = {}
    
    return dirty_paths, sizes




def delete_objects_batch(s3_client, bucket_name: str, keys: List[str]) -> Tuple[int, List[str]]:
    """
    Delete objects using S3 batch delete API (up to 1000 objects per request).
    
    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the S3 bucket
        keys: List of object keys to delete (max 1000)
        
    Returns:
        Tuple of (deleted_count, errors_list)
    """
    if not keys:
        return 0, []
    
    if len(keys) > 1000:
        raise ValueError(f"Too many keys for batch delete: {len(keys)} (max 1000)")
    
    deleted_count = 0
    errors = []
    
    try:
        # Prepare delete request
        delete_objects = [{"Key": key} for key in keys]
        response = s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": delete_objects, "Quiet": False}
        )
        
        # Count successful deletions
        if "Deleted" in response:
            deleted_count = len(response["Deleted"])
        
        # Collect errors
        if "Errors" in response:
            for error in response["Errors"]:
                errors.append(f"{error['Key']}: {error['Code']} - {error['Message']}")
                
    except ClientError as e:
        errors.append(f"ClientError: {e}")
    except Exception as e:
        errors.append(f"Unexpected error: {e}")
    
    return deleted_count, errors


def delete_prefix(
    s3_client, bucket_name: str, prefix: str, dry_run: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Delete all objects under a prefix.
    
    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the S3 bucket
        prefix: Prefix to delete objects under
        dry_run: If True, don't actually delete, just count
        
    Returns:
        Tuple of (total_objects_found, deleted_count, errors_list)
    """
    # List all objects under the prefix
    object_keys = list_all_objects(s3_client, bucket_name, prefix)
    total_objects = len(object_keys)
    
    if total_objects == 0:
        return 0, 0, []
    
    if dry_run:
        return total_objects, 0, []
    
    deleted_count = 0
    all_errors = []
    
    # Delete in batches of 1000
    batch_size = 1000
    for i in range(0, total_objects, batch_size):
        batch = object_keys[i:i + batch_size]
        deleted, errors = delete_objects_batch(s3_client, bucket_name, batch)
        deleted_count += deleted
        all_errors.extend(errors)
    
    return total_objects, deleted_count, all_errors


def delete_prefixes_concurrent(
    s3_client,
    bucket_name: str,
    prefixes: List[str],
    max_workers: int = 10,
    dry_run: bool = False,
) -> Dict[str, Tuple[int, int, List[str]]]:
    """
    Delete objects under multiple prefixes concurrently.
    
    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the S3 bucket
        prefixes: List of prefixes to delete
        max_workers: Number of concurrent workers
        dry_run: If True, don't actually delete, just count
        
    Returns:
        Dictionary mapping prefix -> (total_objects, deleted_count, errors_list)
    """
    results = {}
    total = len(prefixes)
    completed = 0
    
    def process_prefix(prefix: str) -> Tuple[str, int, int, List[str]]:
        total_objs, deleted, errors = delete_prefix(
            s3_client, bucket_name, prefix, dry_run
        )
        return (prefix, total_objs, deleted, errors)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_prefix, prefix): prefix for prefix in prefixes
        }
        
        for future in as_completed(futures):
            prefix, total_objs, deleted, errors = future.result()
            results[prefix] = (total_objs, deleted, errors)
            completed += 1
            print_progress("Deleting prefixes", completed, total)
    
    return results


def display_summary(
    dirty_paths: List[str],
    sizes: Dict[str, int],
    bucket_name: str,
    dry_run: bool = False,
) -> None:
    """Display summary of what will be deleted."""
    print("\n" + "=" * 70)
    print("DELETION SUMMARY")
    print("=" * 70)
    print(f"Bucket: {bucket_name}")
    print(f"Mode: {'DRY RUN (no actual deletion)' if dry_run else 'LIVE DELETION'}")
    print(f"Total prefixes to process: {len(dirty_paths)}")
    
    if sizes:
        total_size = sum(sizes.get(path, 0) for path in dirty_paths)
        print(f"Total size: {total_size} bytes ({format_size(total_size)})")
    
    print("\nSample prefixes (first 10):")
    for i, path in enumerate(dirty_paths[:10], 1):
        size_info = ""
        if sizes and path in sizes:
            size_info = f" ({format_size(sizes[path])})"
        print(f"  {i}. {path}{size_info}")
    
    if len(dirty_paths) > 10:
        print(f"  ... and {len(dirty_paths) - 10} more")
    
    print("=" * 70)


def get_confirmation() -> bool:
    """Get manual confirmation from user."""
    print("\n⚠️  WARNING: This will permanently delete objects from S3!")
    print("This action cannot be undone.\n")
    
    while True:
        response = input("Type 'yes' to confirm deletion, or 'no' to cancel: ").strip().lower()
        if response == "yes":
            return True
        elif response == "no":
            return False
        else:
            print("Please type 'yes' or 'no'")


def main():
    parser = argparse.ArgumentParser(
        description="Delete S3 objects under dirty prefixes from analysis results"
    )
    parser.add_argument(
        "bucket_name",
        help="Name of the S3 bucket to delete from",
    )
    parser.add_argument(
        "-i",
        "--input",
        default="dirty_data_result.json",
        help="Input JSON file with dirty paths (default: dirty_data_result.json)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=10,
        help="Number of concurrent workers (default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )
    parser.add_argument(
        "-o",
        "--output-log",
        help="Optional log file to save deletion results",
    )
    
    args = parser.parse_args()
    
    start_time = time.perf_counter()
    
    # Load dirty paths
    print(f"Loading dirty paths from {args.input}...")
    try:
        dirty_paths, sizes = load_dirty_paths(args.input)
    except Exception as e:
        print(f"Error loading input file: {e}")
        sys.exit(1)
    
    if not dirty_paths:
        print("No dirty paths found in input file. Nothing to delete.")
        sys.exit(0)
    
    print(f"Loaded {len(dirty_paths)} dirty paths")
    
    # Display summary
    display_summary(dirty_paths, sizes, args.bucket_name, args.dry_run)
    
    # Get confirmation (unless dry-run)
    if not args.dry_run:
        if not get_confirmation():
            print("\nDeletion cancelled.")
            sys.exit(0)
    
    # Create S3 client
    print(f"\nCreating S3 client with {args.workers} workers...")
    s3_client = create_s3_client(args.workers)
    
    # Delete prefixes
    print(f"\n{'Simulating deletion' if args.dry_run else 'Starting deletion'}...")
    results = delete_prefixes_concurrent(
        s3_client,
        args.bucket_name,
        dirty_paths,
        args.workers,
        args.dry_run,
    )
    
    # Process results
    total_objects_found = 0
    total_objects_deleted = 0
    total_errors = 0
    failed_prefixes = []
    
    for prefix, (total_objs, deleted, errors) in results.items():
        total_objects_found += total_objs
        total_objects_deleted += deleted
        total_errors += len(errors)
        if errors or (not args.dry_run and deleted < total_objs):
            failed_prefixes.append((prefix, total_objs, deleted, errors))
    
    # Print summary
    print("\n" + "=" * 70)
    print("DELETION RESULTS")
    print("=" * 70)
    print(f"Total prefixes processed: {len(dirty_paths)}")
    print(f"Total objects found: {total_objects_found}")
    if not args.dry_run:
        print(f"Total objects deleted: {total_objects_deleted}")
        print(f"Total errors: {total_errors}")
    
    if failed_prefixes:
        print(f"\n⚠️  {len(failed_prefixes)} prefixes had errors:")
        for prefix, total_objs, deleted, errors in failed_prefixes[:10]:
            print(f"  - {prefix}: {deleted}/{total_objs} deleted")
            for error in errors[:3]:
                print(f"    Error: {error}")
            if len(errors) > 3:
                print(f"    ... and {len(errors) - 3} more errors")
        if len(failed_prefixes) > 10:
            print(f"  ... and {len(failed_prefixes) - 10} more prefixes with errors")
    
    # Save log if requested
    if args.output_log:
        log_data = {
            "bucket": args.bucket_name,
            "input_file": args.input,
            "dry_run": args.dry_run,
            "total_prefixes": len(dirty_paths),
            "total_objects_found": total_objects_found,
            "total_objects_deleted": total_objects_deleted,
            "total_errors": total_errors,
            "results": {
                prefix: {
                    "total_objects": total_objs,
                    "deleted": deleted,
                    "errors": errors,
                }
                for prefix, (total_objs, deleted, errors) in results.items()
            },
        }
        with open(args.output_log, "w", encoding="utf-8") as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)
        print(f"\nLog saved to {args.output_log}")
    
    elapsed = time.perf_counter() - start_time
    print(f"\nTotal elapsed time: {elapsed:.2f} seconds")
    
    # Exit with error code if there were failures
    if failed_prefixes and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()

