"""
Common utilities for S3 prefix listing scripts.
"""

import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from typing import Set


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


def sum_sizes_in_prefix(s3_client, bucket_name: str, prefix: str) -> int:
    """
    Sum object sizes (bytes) under a given prefix.
    
    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the S3 bucket
        prefix: Prefix to sum sizes for (e.g., 'ch-s3-000/uuid/')
    
    Returns:
        Total size in bytes
    """
    total_size = 0
    continuation_token = None

    while True:
        try:
            params = {"Bucket": bucket_name, "Prefix": prefix}
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**params)

            # Accumulate sizes in this page
            for obj in response.get("Contents", []):
                total_size += obj.get("Size", 0)

            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break

        except ClientError as e:
            print(f"Error counting objects under {prefix}: {e}")
            break
        except Exception as e:
            print(f"Unexpected error counting {prefix}: {e}")
            break

    return total_size


def format_size(size_bytes: int) -> str:
    """
    Return human-readable size: GB if 1G<=size<1T, TB if >=1T, else bytes.
    
    Args:
        size_bytes: Size in bytes
    
    Returns:
        Human-readable size string
    """
    gb = 1024**3
    tb = 1024**4
    if gb <= size_bytes < tb:
        return f"{size_bytes / gb:.2f} GB"
    if size_bytes >= tb:
        return f"{size_bytes / tb:.2f} TB"
    return f"{size_bytes} bytes"


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

