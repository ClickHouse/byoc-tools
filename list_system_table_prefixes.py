"""
List system-tables prefixes in a BYOC cloud-shared bucket.

ClickHouse writes system log tables (query_log, metric_log, crash_log, ...) to
a separate S3 layout from user data:

    {bucket}/ch-s3-{KeyPrefix-uuid}/system-tables/mergetree/{server-pod-name}/store/{3-hex}/{table-uuid}/...

The KeyPrefix is per-instance. Each ClickHouse server pod gets its own
subdirectory inside.

Input can be either a whole bucket or a single instance prefix:

    {bucket}
    {bucket}/ch-s3-{KeyPrefix-uuid}
    s3://{bucket}/ch-s3-{KeyPrefix-uuid}

This script:
  1. Discovers all ch-s3-{full-uuid}/ prefixes at the bucket root, unless a
     single ch-s3-{full-uuid} prefix is provided
  2. For each, lists system-tables/mergetree/{pod-name}/ subdirectories
  3. Sums bytes and counts table UUIDs per (instance, pod) pair
  4. Groups per-pod sizes by spoken name, derived from c-...-server-...
  5. Optionally cross-references each KeyPrefix against ClickHouseCluster
     CRDs from one or more kubectl contexts (--context, repeatable) and
     annotates each instance with is_alive / spoken_name / namespace —
     useful for distinguishing data still in use from terminated-instance
     orphans

The script is structurally separate from list_data_prefixes.py because that
one only walks the 4096-way hex shard space (ch-s3-000 .. ch-s3-fff), which
is for user data. system-tables uses an entirely different prefix scheme.
"""

import argparse
import concurrent.futures
import json
import re
import subprocess
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

from utils import (
    create_s3_client,
    discover_ch_s3_prefixes,
    is_valid_uuid,
    list_next_level_prefixes,
    format_size,
    print_progress,
)


SYSTEM_TABLES_SUBPATH = "system-tables/mergetree"
SYSTEM_TABLES_POD_MARKER = f"/system-tables/mergetree/c-"
SERVER_MARKER = "-server-"
TABLE_UUID_IN_STORE_PATTERN = re.compile(
    r"/store/[0-9a-f]{3}/"
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    r"(?:/|$)",
    re.IGNORECASE,
)


def parse_s3_scan_target(s3_target: str) -> Tuple[str, Optional[str]]:
    """
    Parse CLI input into (bucket_name, optional ch-s3-{uuid} instance prefix).

    Accepted forms:
      - bucket-name
      - bucket-name/ch-s3-{uuid}
      - s3://bucket-name/ch-s3-{uuid}

    If a deeper object/prefix path is pasted, the first ch-s3-{uuid} component
    is used as the instance prefix.
    """
    target = s3_target.strip()
    if target.startswith("s3://"):
        target = target[len("s3://") :]
    target = target.strip("/")
    if not target:
        raise ValueError("S3 target must be a bucket or bucket/ch-s3-{uuid}")

    parts = [part for part in target.split("/") if part]
    bucket_name = parts[0]
    if len(parts) == 1:
        return bucket_name, None

    for part in parts[1:]:
        if not part.startswith("ch-s3-"):
            continue
        uuid_part = part[len("ch-s3-") :]
        if is_valid_uuid(uuid_part):
            return bucket_name, part
        raise ValueError(
            f"{part!r} is not a full ch-s3 UUID prefix; expected ch-s3-{{uuid}}"
        )

    raise ValueError(
        "S3 target with a prefix must include ch-s3-{uuid}; "
        "expected bucket or bucket/ch-s3-{uuid}"
    )


def extract_spoken_name_from_path(path: str) -> str:
    """Extract spoken name from a system-tables pod path."""
    if SYSTEM_TABLES_POD_MARKER not in path:
        raise ValueError(f"missing {SYSTEM_TABLES_POD_MARKER!r}")

    pod_name = path.split(SYSTEM_TABLES_POD_MARKER, 1)[1]
    if SERVER_MARKER not in pod_name:
        raise ValueError(f"missing {SERVER_MARKER!r}")

    spoken_name = pod_name.split(SERVER_MARKER, 1)[0]
    if not spoken_name:
        raise ValueError("empty spoken name")

    return spoken_name


def build_spoken_name_breakdown(
    prefix_to_size: Dict[str, int],
) -> Tuple[Dict[str, Dict], int]:
    """
    Group per-pod sizes by spoken name.

    Output order is descending by total size, with spoken name as a stable
    tie-breaker.
    """
    totals: Dict[str, int] = {}
    counts: Dict[str, int] = {}
    skipped = 0

    for path, size in prefix_to_size.items():
        try:
            spoken_name = extract_spoken_name_from_path(path)
        except ValueError as exc:
            skipped += 1
            sys.stderr.write(f"warning: skipping {path!r}: {exc}\n")
            continue

        totals[spoken_name] = totals.get(spoken_name, 0) + size
        counts[spoken_name] = counts.get(spoken_name, 0) + 1

    return (
        {
            spoken_name: {
                "total_bytes": totals[spoken_name],
                "total_size_human": format_size(totals[spoken_name]),
                "prefix_count": counts[spoken_name],
            }
            for spoken_name in sorted(totals, key=lambda name: (-totals[name], name))
        },
        skipped,
    )


def fetch_instance_index(contexts: List[str]) -> Dict[str, Dict[str, str]]:
    """
    Build {ch-s3-{KeyPrefix-uuid}: {spoken_name, namespace, context}} by
    merging live ClickHouseCluster CRDs from one or more kubectl contexts.

    The same AWS account can host multiple BYOC infras that share a single
    cloud-shared bucket. Caller must pass a context for each infra sharing
    the bucket — otherwise instances from omitted infras will be flagged
    as is_alive=false (false orphan).
    """
    index: Dict[str, Dict[str, str]] = {}
    for context in contexts:
        for item in _kubectl_get_clickhouseclusters(context):
            spec_s3 = (item.get("spec") or {}).get("s3") or {}
            key_prefix = spec_s3.get("keyPrefix") or ""
            if not key_prefix:
                continue
            name = item["metadata"]["name"]
            namespace = item["metadata"]["namespace"]
            spoken_name = name[2:] if name.startswith("c-") else name
            if key_prefix in index:
                sys.stderr.write(
                    f"warning: KeyPrefix {key_prefix} found in multiple "
                    f"contexts ({index[key_prefix]['context']} and "
                    f"{context}); keeping the first.\n"
                )
                continue
            index[key_prefix] = {
                "spoken_name": spoken_name,
                "namespace": namespace,
                "context": context,
            }
    return index


def _kubectl_get_clickhouseclusters(context: str) -> List[Dict]:
    """Run `kubectl get clickhousecluster -A -o json --context CTX` and return .items."""
    cmd = [
        "kubectl",
        "get",
        "clickhousecluster",
        "-A",
        "-o",
        "json",
        "--context",
        context,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(
            f"kubectl --context {context} failed (exit {proc.returncode}):\n"
            f"{proc.stderr}\n"
        )
        sys.exit(proc.returncode)
    return json.loads(proc.stdout).get("items", [])


def list_pods_under_instance(
    s3_client, bucket_name: str, ch_s3_prefix: str
) -> List[str]:
    """List the pod-name subdirectories under one ch-s3-{uuid}/system-tables/mergetree/."""
    full_prefix = f"{ch_s3_prefix}/{SYSTEM_TABLES_SUBPATH}/"
    return sorted(list_next_level_prefixes(s3_client, bucket_name, full_prefix))


def extract_table_uuid_from_key(key: str) -> Optional[str]:
    """Extract a table UUID from .../store/{3-hex}/{uuid}/... object keys."""
    match = TABLE_UUID_IN_STORE_PATTERN.search(key)
    if not match:
        return None
    return match.group(1).lower()


def extract_table_uuid_prefix_from_key(key: str) -> Optional[str]:
    """Extract a prefix ending at .../store/{3-hex}/{uuid} from an object key."""
    match = TABLE_UUID_IN_STORE_PATTERN.search(key)
    if not match:
        return None
    return key[: match.end(1)]


def sum_size_and_table_uuid_prefixes(
    s3_client, bucket_name: str, prefix: str
) -> Tuple[int, int, List[str]]:
    """Sum bytes and collect unique table UUID prefixes under one pod prefix."""
    total_size = 0
    table_uuids: Set[str] = set()
    table_uuid_prefixes: Set[str] = set()
    continuation_token = None

    while True:
        try:
            params = {"Bucket": bucket_name, "Prefix": prefix}
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**params)

            for obj in response.get("Contents", []):
                total_size += obj.get("Size", 0)
                key = obj.get("Key", "")
                table_uuid = extract_table_uuid_from_key(key)
                if table_uuid:
                    table_uuids.add(table_uuid)
                table_uuid_prefix = extract_table_uuid_prefix_from_key(key)
                if table_uuid_prefix:
                    table_uuid_prefixes.add(table_uuid_prefix)

            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break

        except Exception as exc:
            print(f"Unexpected error counting {prefix}: {exc}")
            break

    return total_size, len(table_uuids), sorted(table_uuid_prefixes)


def collect_system_table_prefixes(
    bucket_name: str,
    output_file: str,
    contexts: Optional[List[str]],
    max_workers: int,
    instance_prefix: Optional[str] = None,
):
    print(f"Using {max_workers} concurrent workers")

    instance_index: Optional[Dict[str, Dict[str, str]]] = None
    if contexts:
        instance_index = fetch_instance_index(contexts)
        print(
            f"Loaded {len(instance_index)} live instance(s) from "
            f"{len(contexts)} kubectl context(s)"
        )

    s3_client = create_s3_client(max_workers)

    start_time = time.perf_counter()

    if instance_prefix:
        print(
            f"Scanning only instance prefix {instance_prefix!r} "
            f"in bucket {bucket_name!r}"
        )
        instance_prefixes = [instance_prefix]
    else:
        print("Discovering ch-s3-{uuid}/ prefixes...")
        instance_prefixes = discover_ch_s3_prefixes(s3_client, bucket_name)
        print(f"Found {len(instance_prefixes)} ch-s3-{{uuid}}/ prefix(es)")

    if not instance_prefixes:
        print("Nothing to scan. Exiting.")
        return

    # Phase 1: enumerate per-pod subpaths per instance, in parallel.
    print("\nListing pod subdirectories under each instance's system-tables/mergetree/...")
    instance_to_pods: Dict[str, List[str]] = {}

    def list_pods(ch_s3_prefix: str) -> Tuple[str, List[str]]:
        try:
            return ch_s3_prefix, list_pods_under_instance(s3_client, bucket_name, ch_s3_prefix)
        except Exception as exc:
            print(f"Error listing pods for {ch_s3_prefix}: {exc}")
            return ch_s3_prefix, []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(list_pods, p): p for p in instance_prefixes}
        completed = 0
        total = len(instance_prefixes)
        for future in concurrent.futures.as_completed(futures):
            prefix, pods = future.result()
            if pods:
                instance_to_pods[prefix] = pods
            completed += 1
            print_progress("Listing pods", completed, total)

    if not instance_to_pods:
        print("\nNo system-tables/mergetree/ found under any instance prefix. Exiting.")
        return

    # Build the flat list of (instance, pod) full paths.
    full_paths: List[str] = []
    for prefix, pods in instance_to_pods.items():
        for pod in pods:
            full_paths.append(f"{prefix}/{SYSTEM_TABLES_SUBPATH}/{pod}")
    full_paths.sort()

    # Phase 2: sum bytes and count table UUIDs for each pod path.
    print(
        f"\nSumming object sizes and counting table UUIDs under "
        f"{len(full_paths)} pod path(s)..."
    )
    prefix_to_size: Dict[str, int] = {}
    prefix_to_table_uuid_count: Dict[str, int] = {}
    prefix_to_table_uuid_prefixes: Dict[str, List[str]] = {}

    def size_and_uuid_prefixes_for_path(
        path: str,
    ) -> Tuple[str, int, int, List[str]]:
        try:
            (
                size,
                table_uuid_count,
                table_uuid_prefixes,
            ) = sum_size_and_table_uuid_prefixes(
                s3_client, bucket_name, f"{path}/"
            )
            return path, size, table_uuid_count, table_uuid_prefixes
        except Exception as exc:
            print(f"Error counting {path}: {exc}")
            return path, 0, 0, []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(size_and_uuid_prefixes_for_path, p): p for p in full_paths
        }
        completed = 0
        total = len(full_paths)
        for future in concurrent.futures.as_completed(futures):
            path, size, table_uuid_count, table_uuid_prefixes = future.result()
            prefix_to_size[path] = size
            prefix_to_table_uuid_prefixes[path] = table_uuid_prefixes
            prefix_to_table_uuid_count[path] = table_uuid_count
            completed += 1
            print_progress("Counting sizes/table UUIDs", completed, total)

    # Phase 3: per-instance grouping + optional alive/dead annotation from
    # the kubectl-derived instance index.
    by_instances: Dict[str, Dict] = {}
    overall_alive_instances = 0
    overall_dead_instances = 0
    overall_alive_bytes = 0
    overall_dead_bytes = 0

    for prefix, pods in instance_to_pods.items():
        instance_total = sum(
            prefix_to_size.get(f"{prefix}/{SYSTEM_TABLES_SUBPATH}/{pod}", 0)
            for pod in pods
        )
        instance_table_uuid_count = sum(
            prefix_to_table_uuid_count.get(
                f"{prefix}/{SYSTEM_TABLES_SUBPATH}/{pod}", 0
            )
            for pod in pods
        )

        entry: Dict = {
            "total_bytes": instance_total,
            "total_size_human": format_size(instance_total),
            "replica_count": len(pods),
            "table_uuid_count_across_pods": instance_table_uuid_count,
        }

        if instance_index is not None:
            info = instance_index.get(prefix)
            if info:
                entry["is_alive"] = True
                entry["spoken_name"] = info["spoken_name"]
                entry["namespace"] = info["namespace"]
                entry["context"] = info["context"]
                overall_alive_instances += 1
                overall_alive_bytes += instance_total
            else:
                entry["is_alive"] = False
                overall_dead_instances += 1
                overall_dead_bytes += instance_total

        by_instances[prefix] = entry

    by_spoken_name, skipped_spoken_name_prefixes = build_spoken_name_breakdown(
        prefix_to_size
    )

    total_bytes = sum(prefix_to_size.values())
    total_table_uuid_count = sum(prefix_to_table_uuid_count.values())
    table_uuid_prefixes = sorted(
        table_uuid_prefix
        for prefixes in prefix_to_table_uuid_prefixes.values()
        for table_uuid_prefix in prefixes
    )
    summary: Dict = {
        "total_instances": len(by_instances),
        "total_spoken_names": len(by_spoken_name),
        "total_replicas": len(full_paths),
        "total_table_uuid_count_across_pod_paths": total_table_uuid_count,
        "total_table_uuid_prefixes": len(table_uuid_prefixes),
        "total_size_bytes": total_bytes,
        "total_size_human": format_size(total_bytes),
    }
    if instance_index is not None:
        summary.update(
            {
                "alive_instance_count": overall_alive_instances,
                "alive_instance_size_bytes": overall_alive_bytes,
                "alive_instance_size_human": format_size(overall_alive_bytes),
                "dead_instance_count": overall_dead_instances,
                "dead_instance_size_bytes": overall_dead_bytes,
                "dead_instance_size_human": format_size(overall_dead_bytes),
            }
        )

    output_data = {
        "prefixes": full_paths,
        "table_uuid_prefixes": table_uuid_prefixes,
        "prefix_sizes_bytes": prefix_to_size,
        "prefix_table_uuid_counts": prefix_to_table_uuid_count,
        "prefix_table_uuid_prefixes": prefix_to_table_uuid_prefixes,
        "by_instances": by_instances,
        "by_spoken_name": by_spoken_name,
        "summary": summary,
    }

    print(f"\nSaving to {output_file}...")
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"Successfully saved data to {output_file}")

    print("\nSummary:")
    print(f"  Instances with system-tables: {summary['total_instances']}")
    print(f"  Spoken names with system-tables: {summary['total_spoken_names']}")
    print(f"  Total replica subdirectories: {summary['total_replicas']}")
    print(
        "  Total table UUIDs across pod paths: "
        f"{summary['total_table_uuid_count_across_pod_paths']}"
    )
    print(f"  Total table UUID prefixes: {summary['total_table_uuid_prefixes']}")
    print(
        f"  Total size: {total_bytes} bytes ({summary['total_size_human']})"
    )
    if instance_index is not None:
        print(
            f"  Alive instances: {overall_alive_instances} "
            f"({summary['alive_instance_size_human']})"
        )
        print(
            f"  Dead instances:  {overall_dead_instances} "
            f"({summary['dead_instance_size_human']})"
        )
        if overall_dead_instances and len(contexts) == 1:
            print(
                "\n  Note: only one --context was provided. If multiple "
                "BYOC infras share this bucket (typically when they're in "
                "the same AWS account + region), instances belonging to "
                "the other infras will be reported as is_alive=false. "
                "Pass --context for each sharing infra to get an accurate "
                "alive/dead split."
            )

    elapsed = time.perf_counter() - start_time
    print(f"\nTotal elapsed time: {elapsed:.2f} seconds")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "List system-tables prefixes (per ClickHouse server pod) in a "
            "BYOC cloud-shared bucket or under one ch-s3 UUID prefix. "
            "Optionally cross-reference each KeyPrefix against live "
            "ClickHouseCluster CRDs to flag terminated-instance orphans."
        )
    )
    parser.add_argument(
        "s3_target",
        help=(
            "Cloud-shared S3 bucket, or bucket/ch-s3-{uuid}; "
            "s3://bucket/ch-s3-{uuid} is also accepted"
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        default="system_table_prefixes.json",
        help="Output file name (default: system_table_prefixes.json)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=50,
        help="Number of concurrent workers (default: 50)",
    )
    parser.add_argument(
        "--context",
        action="append",
        default=None,
        dest="contexts",
        help=(
            "kubectl context to consult for live-instance metadata. Repeat "
            "for each BYOC infra that shares this S3 bucket — instances from "
            "any infra you don't list will be reported as is_alive=false. "
            "When provided, each instance entry gains is_alive / "
            "spoken_name / namespace / context fields. When omitted, those "
            "fields are not included."
        ),
    )

    args = parser.parse_args()
    try:
        bucket_name, instance_prefix = parse_s3_scan_target(args.s3_target)
    except ValueError as exc:
        parser.error(str(exc))

    collect_system_table_prefixes(
        bucket_name,
        args.output,
        args.contexts,
        args.workers,
        instance_prefix,
    )


if __name__ == "__main__":
    main()
