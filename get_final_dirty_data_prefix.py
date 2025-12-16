#!/usr/bin/env python3
"""
Find prefixes that are not in non_terminated_prefixes. output them with their full paths.
"""

import json
import argparse
import sys
import time
from typing import Set, List, Dict


def load_non_terminated_uuids(filename: str) -> Set[str]:
    """
    Load non-terminated UUIDs from a file.
    Handles UUIDs with or without 'ch-s3-' prefix.
    """
    uuids = set()
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Remove 'ch-s3-' prefix if present
            if line.startswith("ch-s3-"):
                # Extract UUID part (everything after 'ch-s3-')
                uuid = line.replace("ch-s3-", "", 1)
            else:
                uuid = line
            uuids.add(uuid)
    return uuids


def format_size(size_bytes: int) -> str:
    """Return human-readable size: GB if 1G<=size<1T, TB if >=1T, else bytes."""
    gb = 1024**3
    tb = 1024**4
    if gb <= size_bytes < tb:
        return f"{size_bytes / gb:.2f} GB"
    if size_bytes >= tb:
        return f"{size_bytes / tb:.2f} TB"
    return f"{size_bytes} bytes"


def print_progress(label: str, current: int, total: int, bar_length: int = 30):
    """Render a simple in-place progress bar."""
    if total <= 0:
        return
    pct = min(max(current / total, 0), 1)
    filled = int(bar_length * pct)
    bar = "#" * filled + "-" * (bar_length - filled)
    sys.stdout.write(f"\r{label} [{bar}] {current}/{total} ({pct*100:.1f}%)")
    sys.stdout.flush()
    if current >= total:
        sys.stdout.write("\n")


def load_data_prefixes(filename: str) -> tuple[List[str], Dict[str, int]]:
    """
    Load data prefixes list from JSON file.
    Returns (prefixes_list, prefix_sizes_dict).
    Supports both old format (list) and new format (dict with 'prefixes' key).
    """
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Handle new format: {"prefixes": [...], "prefix_sizes_bytes": {...}, ...}
    if isinstance(data, dict) and "prefixes" in data:
        prefixes = data["prefixes"]
        sizes = data.get("prefix_sizes_bytes", {})
        return prefixes, sizes
    # Handle old format: just a list
    elif isinstance(data, list):
        return data, {}
    else:
        raise ValueError(f"Unexpected JSON format in {filename}")


def extract_uuid_from_path(full_path: str) -> str:
    """
    Extract UUID from full path.
    Path format: ch-s3-xxx/uuid
    Returns: uuid
    """
    # Split by '/' and get the last part (UUID)
    parts = full_path.split("/")
    if len(parts) >= 2:
        return parts[-1]
    return full_path


def find_dirty_data_prefixes(
    data_prefixes: List[str], non_terminated_uuids: Set[str]
) -> tuple[List[str], Dict[str, int]]:
    """
    Find paths whose UUIDs are not in non_terminated_uuids.
    Returns (dirty_paths_list, dirty_paths_sizes_dict).
    """
    dirty_paths = []
    dirty_sizes = {}
    total = len(data_prefixes)

    for idx, full_path in enumerate(data_prefixes, 1):
        uuid = extract_uuid_from_path(full_path)
        # Check if UUID is not in non-terminated list
        if uuid not in non_terminated_uuids:
            dirty_paths.append(full_path)
        print_progress("Finding dirty prefixes", idx, total)

    return dirty_paths, dirty_sizes


def main():
    parser = argparse.ArgumentParser(
        description="Find UUIDs not in non_terminated_prefixes.list and output them with full paths"
    )
    parser.add_argument(
        "-p",
        "--data-prefixes",
        default="data_prefixes.json",
        help="Input JSON file with data prefixes list (default: data_prefixes.json)",
    )
    parser.add_argument(
        "-n",
        "--non-terminated",
        default="non_terminated_prefixes.list",
        help="Input file with non-terminated UUIDs list (default: non_terminated_prefixes.list)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="dirty_data_result.json",
        help="Output JSON file name (default: dirty_data_result.json)",
    )

    args = parser.parse_args()

    start_time = time.perf_counter()

    print(f"Loading data prefixes from {args.data_prefixes}...")
    data_prefixes, prefix_sizes = load_data_prefixes(args.data_prefixes)
    print(f"Loaded {len(data_prefixes)} paths from {args.data_prefixes}")

    print(f"Loading non-terminated UUIDs from {args.non_terminated}...")
    non_terminated_uuids = load_non_terminated_uuids(args.non_terminated)
    print(
        f"Loaded {len(non_terminated_uuids)} non-terminated UUIDs from {args.non_terminated}"
    )

    print("\nFinding dirty UUIDs (not in non-terminated list)...")
    dirty_paths, dirty_sizes = find_dirty_data_prefixes(
        data_prefixes, non_terminated_uuids
    )

    # Build size dict for dirty paths if available
    if prefix_sizes:
        dirty_sizes = {path: prefix_sizes.get(path, 0) for path in dirty_paths}

    # Sort for consistent output
    sorted_dirty_paths = sorted(dirty_paths)

    print(f"\nFound {len(sorted_dirty_paths)} dirty paths")

    # Prepare output data
    output_data = {
        "dirty_paths": sorted_dirty_paths,
    }
    if dirty_sizes:
        output_data["dirty_paths_sizes_bytes"] = {
            path: dirty_sizes.get(path, 0) for path in sorted_dirty_paths
        }
        total_dirty_size = sum(dirty_sizes.values())
        output_data["summary"] = {
            "total_dirty_paths": len(sorted_dirty_paths),
            "total_dirty_size_bytes": total_dirty_size,
            "total_dirty_size_human": format_size(total_dirty_size),
        }

    print(f"Saving results to {args.output}...")
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"Successfully saved {len(sorted_dirty_paths)} dirty paths to {args.output}")

    # Print summary statistics
    dirty_uuids = set(extract_uuid_from_path(path) for path in sorted_dirty_paths)
    print(f"\nSummary:")
    print(f"  Total dirty UUIDs: {len(dirty_uuids)}")
    print(f"  Total full paths: {len(sorted_dirty_paths)}")
    if dirty_sizes:
        total_dirty_size = sum(dirty_sizes.values())
        print(
            f"  Total dirty size: {total_dirty_size} bytes ({format_size(total_dirty_size)})"
        )

    elapsed = time.perf_counter() - start_time
    print(f"\nTotal elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
