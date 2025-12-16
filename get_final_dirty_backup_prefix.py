#!/usr/bin/env python3
"""
Find backup prefixes whose next-level UUIDs are not in the provided list.
Output them with their full paths.
"""

import json
import argparse
import time
from typing import Set, List, Dict

from utils import format_size, print_progress


def load_non_terminated_uuids(filename: str) -> Set[str]:
    """
    Load non-terminated UUIDs from a file.
    These are the next-level UUIDs (second level in ch-s3-uuid/uuid format).

    Args:
        filename: Path to file containing UUID list (one per line)

    Returns:
        Set of UUID strings
    """
    uuids = set()
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Remove any 'ch-s3-' prefix if present (though it shouldn't be for next-level UUIDs)
            if line.startswith("ch-s3-"):
                # Extract UUID part (everything after 'ch-s3-')
                uuid = line.replace("ch-s3-", "", 1)
            else:
                uuid = line
            uuids.add(uuid)
    return uuids


def load_backup_prefixes(filename: str) -> tuple[List[str], Dict[str, int]]:
    """
    Load backup prefixes list from JSON file.
    Returns (prefixes_list, prefix_sizes_dict).
    Supports both old format (list) and new format (dict with 'prefixes' key).

    Args:
        filename: Path to backup_prefixes.json file

    Returns:
        Tuple of (prefixes list, sizes dictionary)
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


def extract_next_level_uuid_from_path(full_path: str) -> str:
    """
    Extract next-level UUID from backup path.
    Path format: ch-s3-uuid/uuid
    Returns: uuid (the second-level UUID)

    Args:
        full_path: Full backup path (e.g., 'ch-s3-03238e68-e7d3-443b-a088-b850943dfb5b/4e33a077-8509-44e1-a878-4d2f7d9a5244')

    Returns:
        The second-level UUID (e.g., '4e33a077-8509-44e1-a878-4d2f7d9a5244')
    """
    # Split by '/' and get the last part (next-level UUID)
    parts = full_path.split("/")
    if len(parts) >= 2:
        return parts[-1]
    return full_path


def find_dirty_backup_prefixes(
    backup_prefixes: List[str], non_terminated_uuids: Set[str]
) -> tuple[List[str], Dict[str, int]]:
    """
    Find backup paths whose next-level UUIDs are not in non_terminated_uuids.
    Returns (dirty_paths_list, dirty_paths_sizes_dict).

    Args:
        backup_prefixes: List of backup prefix paths (ch-s3-uuid/uuid format)
        non_terminated_uuids: Set of non-terminated next-level UUIDs

    Returns:
        Tuple of (dirty paths list, empty sizes dict - will be filled later)
    """
    dirty_paths = []
    dirty_sizes = {}
    total = len(backup_prefixes)

    for idx, full_path in enumerate(backup_prefixes, 1):
        next_level_uuid = extract_next_level_uuid_from_path(full_path)
        # Check if next-level UUID is not in non-terminated list
        if next_level_uuid not in non_terminated_uuids:
            dirty_paths.append(full_path)
        print_progress("Finding dirty prefixes", idx, total)

    return dirty_paths, dirty_sizes


def main():
    parser = argparse.ArgumentParser(
        description="Find backup prefixes whose next-level UUIDs are not in the provided list and output them with full paths"
    )
    parser.add_argument(
        "-p",
        "--backup-prefixes",
        default="backup_prefixes.json",
        help="Input JSON file with backup prefixes list (default: backup_prefixes.json)",
    )
    parser.add_argument(
        "-n",
        "--non-terminated",
        default="non_terminated_uuids.list",
        help="Input file with non-terminated next-level UUIDs list (default: non_terminated_uuids.list)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="dirty_backup_result.json",
        help="Output JSON file name (default: dirty_backup_result.json)",
    )

    args = parser.parse_args()

    start_time = time.perf_counter()

    print(f"Loading backup prefixes from {args.backup_prefixes}...")
    backup_prefixes, prefix_sizes = load_backup_prefixes(args.backup_prefixes)
    print(f"Loaded {len(backup_prefixes)} paths from {args.backup_prefixes}")

    print(f"Loading non-terminated next-level UUIDs from {args.non_terminated}...")
    non_terminated_uuids = load_non_terminated_uuids(args.non_terminated)
    print(
        f"Loaded {len(non_terminated_uuids)} non-terminated next-level UUIDs from {args.non_terminated}"
    )

    print(
        "\nFinding dirty backup prefixes (next-level UUID not in non-terminated list)..."
    )
    dirty_paths, dirty_sizes = find_dirty_backup_prefixes(
        backup_prefixes, non_terminated_uuids
    )

    # Build size dict for dirty paths if available
    if prefix_sizes:
        dirty_sizes = {path: prefix_sizes.get(path, 0) for path in dirty_paths}

    # Sort for consistent output
    sorted_dirty_paths = sorted(dirty_paths)

    print(f"\nFound {len(sorted_dirty_paths)} dirty backup paths")

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

    print(
        f"Successfully saved {len(sorted_dirty_paths)} dirty backup paths to {args.output}"
    )

    # Print summary statistics
    dirty_uuids = set(
        extract_next_level_uuid_from_path(path) for path in sorted_dirty_paths
    )
    print(f"\nSummary:")
    print(f"  Total dirty next-level UUIDs: {len(dirty_uuids)}")
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
