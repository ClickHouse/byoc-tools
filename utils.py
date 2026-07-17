"""
Common utilities for S3 prefix listing scripts.
"""

import re
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from typing import Optional, Set, List, Tuple


# UUID format used in BYOC ch-s3-{uuid} prefixes (8-4-4-4-12 hex digits).
UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def is_valid_uuid(uuid_str: str) -> bool:
    """
    Validate if a string matches UUID format (8-4-4-4-12 hex digits).
    """
    return bool(UUID_PATTERN.match(uuid_str))


def discover_ch_s3_prefixes(s3_client, bucket_name: str) -> List[str]:
    """
    Discover all top-level ch-s3-{uuid} prefixes in the bucket.

    Filters out the hex-shard prefixes (ch-s3-{3-hex}) used by user data and
    returns only full-UUID per-instance prefixes (ch-s3-{8-4-4-4-12}).
    """
    ch_s3_prefixes: List[str] = []
    continuation_token = None

    while True:
        try:
            params = {"Bucket": bucket_name, "Prefix": "ch-s3-", "Delimiter": "/"}
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**params)

            for common_prefix in response.get("CommonPrefixes", []):
                full_prefix = common_prefix["Prefix"].rstrip("/")
                if not full_prefix.startswith("ch-s3-"):
                    continue
                uuid_part = full_prefix[len("ch-s3-") :]
                if is_valid_uuid(uuid_part):
                    ch_s3_prefixes.append(full_prefix)

            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break

        except Exception as e:
            print(f"Error discovering ch-s3-* prefixes: {e}")
            break

    return sorted(ch_s3_prefixes)


def create_s3_client(max_workers: int = 50):
    """
    Create a configured S3 client with appropriate connection pool size.
    
    Args:
        max_workers: Number of concurrent workers (used to size connection pool)
    
    Returns:
        Configured boto3 S3 client
    """
    return boto3.client(
        "s3",
        config=Config(
            max_pool_connections=max_workers,
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def list_next_level_prefixes(s3_client, bucket_name: str, prefix: str) -> Set[str]:
    """
    List all next-level prefixes under a given prefix.
    For example, under 'ch-s3-000/', returns set of prefixes like {'abc', 'def', ...}
    
    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the S3 bucket
        prefix: Prefix to list under (e.g., 'ch-s3-000/')
    
    Returns:
        Set of next-level prefix names (without parent prefix)
    """
    next_level_prefixes = set()
    continuation_token = None

    while True:
        try:
            params = {"Bucket": bucket_name, "Prefix": prefix, "Delimiter": "/"}
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**params)

            # Get common prefixes (next-level directories)
            if "CommonPrefixes" in response:
                for common_prefix in response["CommonPrefixes"]:
                    # Extract the next-level prefix name
                    # e.g., 'ch-s3-000/abc/' -> 'abc'
                    full_prefix = common_prefix["Prefix"]
                    # Remove the parent prefix to get just the next level
                    relative_prefix = full_prefix[len(prefix) :]
                    # Remove trailing slash
                    if relative_prefix.endswith("/"):
                        relative_prefix = relative_prefix[:-1]
                    if relative_prefix:  # Only add non-empty prefixes
                        next_level_prefixes.add(relative_prefix)

            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break

        except ClientError as e:
            print(f"Error listing prefixes under {prefix}: {e}")
            break
        except Exception as e:
            print(f"Unexpected error listing {prefix}: {e}")
            break

    return next_level_prefixes


def sum_prefix_stats(
    s3_client, bucket_name: str, prefix: str
) -> Tuple[int, Optional[datetime]]:
    """
    Sum object sizes (bytes) and track the newest LastModified under a prefix.

    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the S3 bucket
        prefix: Prefix to sum sizes for (e.g., 'ch-s3-000/uuid/')

    Returns:
        Tuple of (total size in bytes, LastModified of the newest object,
        or None if the prefix holds no objects)
    """
    total_size = 0
    latest_modified: Optional[datetime] = None
    continuation_token = None

    while True:
        try:
            params = {"Bucket": bucket_name, "Prefix": prefix}
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**params)

            # Accumulate sizes and newest timestamp in this page
            for obj in response.get("Contents", []):
                total_size += obj.get("Size", 0)
                last_modified = obj.get("LastModified")
                if last_modified and (
                    latest_modified is None or last_modified > latest_modified
                ):
                    latest_modified = last_modified

            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break

        except ClientError as e:
            print(f"Error counting objects under {prefix}: {e}")
            break
        except Exception as e:
            print(f"Unexpected error counting {prefix}: {e}")
            break

    return total_size, latest_modified


def format_timestamp(dt: Optional[datetime]) -> Optional[str]:
    """
    Format a datetime as an ISO 8601 UTC string (e.g. '2026-07-16T09:57:00Z').

    Args:
        dt: datetime to format (or None)

    Returns:
        ISO 8601 string, or None if dt is None
    """
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_size(size_bytes: int) -> str:
    """
    Return human-readable size: TB/GB/MB/KB with two decimals, or raw bytes
    below 1 KB.

    Args:
        size_bytes: Size in bytes

    Returns:
        Human-readable size string
    """
    kb = 1024
    mb = 1024**2
    gb = 1024**3
    tb = 1024**4
    if size_bytes >= tb:
        return f"{size_bytes / tb:.2f} TB"
    if size_bytes >= gb:
        return f"{size_bytes / gb:.2f} GB"
    if size_bytes >= mb:
        return f"{size_bytes / mb:.2f} MB"
    if size_bytes >= kb:
        return f"{size_bytes / kb:.2f} KB"
    return f"{size_bytes} bytes"


def list_all_objects(s3_client, bucket_name: str, prefix: str) -> List[str]:
    """
    List all object keys under a given prefix.
    
    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the S3 bucket
        prefix: Prefix to list objects under (e.g., 'ch-s3-000/uuid/')
    
    Returns:
        List of object keys (full paths)
    """
    object_keys = []
    continuation_token = None
    
    # Ensure prefix ends with / for proper listing
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    
    while True:
        try:
            params = {"Bucket": bucket_name, "Prefix": prefix}
            if continuation_token:
                params["ContinuationToken"] = continuation_token
            
            response = s3_client.list_objects_v2(**params)
            
            # Collect object keys
            if "Contents" in response:
                for obj in response["Contents"]:
                    object_keys.append(obj["Key"])
            
            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break
                
        except ClientError as e:
            print(f"Error listing objects under {prefix}: {e}")
            break
        except Exception as e:
            print(f"Unexpected error listing {prefix}: {e}")
            break
    
    return object_keys


def print_progress(label: str, current: int, total: int, bar_length: int = 30):
    """
    Render a simple in-place progress bar.
    
    Args:
        label: Label to display before the progress bar
        current: Current progress count
        total: Total count
        bar_length: Length of the progress bar in characters
    """
    if total <= 0:
        return
    pct = min(max(current / total, 0), 1)
    filled = int(bar_length * pct)
    bar = "#" * filled + "-" * (bar_length - filled)
    sys.stdout.write(f"\r{label} [{bar}] {current}/{total} ({pct*100:.1f}%)")
    sys.stdout.flush()
    if current >= total:
        sys.stdout.write("\n")

