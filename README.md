# byoc-tools

* list data prefixes
* list backup prefixes
* list system-tables prefixes
* delete dirty prefixes

# Prerequisites

```shell
$ pip install -r requirements.txt
$ export AWS_PROFILE=XXX # switch to the correct profile
$ aws s3 ls # make sure the command can return correctly
```

# Get the dirty data prefixes

*data bucket pattern:*

    {aws_account_id}.{region}.aws.clickhouse.cloud-shared

## Get all prefixes of an account

run the command below to gain the prefixes:

```shell
$ python list_data_prefixes.py ${data_bucket} -w 100 # e.g data_bucket: xxx.us-east-2.aws.clickhouse.cloud-shared
```

the result:

```json
{
    "prefixes": [
        "ch-s3-000/09ae9cf0-31a7-4af6-8431-c553a290f097",
        ...
    ],
    "prefix_sizes_bytes": {
        "ch-s3-000/09ae9cf0-31a7-4af6-8431-c553a290f097": 4324,
        ...
    },
    "prefix_latest_object_timestamps": {
        "ch-s3-000/09ae9cf0-31a7-4af6-8431-c553a290f097": "2026-07-15T09:57:00Z",
        ...
    },
    "summary": {
        "total_unique_prefixes": 12345,
        "total_uuids_with_prefixes": 4096,
        "total_full_paths": 82892,
        "total_size_bytes": 123806743839,
        "total_size_human": "115.30 GB",
        "latest_object_timestamp": "2026-07-15T09:57:00Z"
    }
}
```

## Get existing key prefix uuid for a byoc account

Please contact the clickhouse byoc team and provide the AWS account id to them. They will give you the existing uuid list. Save the list to `non_terminated_prefixes.list`


## Get the data dirty prefixes

```shell
$ python get_final_dirty_data_prefix.py --non-terminated ~/non_terminated_prefixes.list # update the non-terminated uuid path accordingly
```


the result:

```json
{
    "dirty_paths": [
      "ch-s3-000/09ae9cf0-31a7-4af6-8431-c553a290f097",
      ...
    ],
    "dirty_paths_sizes_bytes": {
      "ch-s3-000/09ae9cf0-31a7-4af6-8431-c553a290f097": 5338,
      ...
    },
    "summary": {
      "total_dirty_paths": 82892,
      "total_dirty_size_bytes": 123806743839,
      "total_dirty_size_human": "115.30 GB"
    }
}
```


# Get the system-tables prefixes (ClickHouse system logs)

ClickHouse writes system log tables (`query_log`, `metric_log`, `crash_log`, ...)
to a separate S3 layout from user data:

    {bucket}/ch-s3-{KeyPrefix-uuid}/system-tables/mergetree/{server-pod-name}/{table-uuid}/...

## Get all system-tables prefixes for an account

```shell
$ python list_system_table_prefixes.py ${data_bucket} -w 100
```

The result groups per-pod sizes by instance (`ch-s3-{KeyPrefix}` prefix) and
sums them under a top-level `summary`:

```json
{
  "prefixes": [
    "ch-s3-{KeyPrefix-uuid}/system-tables/mergetree/c-foo-server-AAA-0",
    ...
  ],
  "table_uuid_prefixes": [
    "ch-s3-{KeyPrefix-uuid}/system-tables/mergetree/c-foo-server-AAA-0/store/abc/{table-uuid}",
    ...
  ],
  "prefix_sizes_bytes": {
    "ch-s3-{KeyPrefix-uuid}/system-tables/mergetree/c-foo-server-AAA-0": 5047762890,
    ...
  },
  "prefix_latest_object_timestamps": {
    "ch-s3-{KeyPrefix-uuid}/system-tables/mergetree/c-foo-server-AAA-0": "2026-07-15T09:57:00Z",
    ...
  },
  "table_uuid_prefix_sizes_bytes": {
    "ch-s3-{KeyPrefix-uuid}/system-tables/mergetree/c-foo-server-AAA-0/store/abc/{table-uuid}": 5040000000,
    ...
  },
  "table_uuid_prefix_latest_object_timestamps": {
    "ch-s3-{KeyPrefix-uuid}/system-tables/mergetree/c-foo-server-AAA-0/store/abc/{table-uuid}": "2026-07-15T09:57:00Z",
    ...
  },
  "by_instances": {
    "ch-s3-{KeyPrefix-uuid}": {
      "total_bytes": 56983256576,
      "total_size_human": "53.08 GB",
      "replica_count": 25,
      "latest_object_timestamp": "2026-07-15T09:57:00Z"
    },
    ...
  },
  "by_spoken_name": {
    "foo": {
      "total_bytes": 56983256576,
      "total_size_human": "53.08 GB",
      "prefix_count": 25,
      "latest_object_timestamp": "2026-07-15T09:57:00Z"
    },
    ...
  },
  "summary": {
    "total_instances": 523,
    "total_replicas": 1220,
    "total_size_bytes": 518360551746,
    "total_size_human": "482.76 GB",
    "latest_object_timestamp": "2026-07-15T09:57:00Z"
  }
}
```

The `*_latest_object_timestamps` fields hold the `LastModified` of the newest
object under each prefix (ISO 8601, UTC) — useful for judging whether a prefix
is still being written to or has gone stale.

## Identify terminated-instance orphans

Pass `--context` (repeatable) to cross-reference each `ch-s3-{KeyPrefix}/`
prefix in S3 against live `ClickHouseCluster` CRDs in those kubectl contexts.
Each instance entry gains `is_alive`, plus `spoken_name` / `namespace` /
`context` for the alive ones:

```shell
$ python list_system_table_prefixes.py ${data_bucket} \
    --context my-byoc-prod
```

```json
{
  "by_instances": {
    "ch-s3-{KeyPrefix-uuid}": {
      "total_size_human": "53.08 GB",
      "replica_count": 25,
      "is_alive": true,
      "spoken_name": "my-prod-instance",
      "namespace": "ns-my-prod-instance",
      "context": "my-byoc-prod"
    },
    "ch-s3-{terminated-uuid}": {
      "total_size_human": "1783593 bytes",
      "replica_count": 1,
      "is_alive": false
    }
  },
  "summary": {
    "alive_instance_count": 1,
    "alive_instance_size_human": "53.08 GB",
    "dead_instance_count": 1,
    "dead_instance_size_human": "1783593 bytes"
  }
}
```

> ⚠️ **If multiple BYOC infras share this S3 bucket** (typically because
> they're in the same AWS account + region), you **must** pass `--context`
> for **each** of them. Any instance whose context you don't pass will appear
> as `is_alive: false` (false orphan). For most customers there is only one
> BYOC infra per AWS account, so a single `--context` is sufficient.


# Get the dirty path of the backup

*backup bucket pattern:*

    {aws_account_id}.{region}.aws.clickhouse.cloud-backup


## Get all backup prefixes of an account

run the command below to gain the prefixes:

```shell
$ python list_backup_prefixes.py ${backup_bucket} -w 100 # e.g backup_bucket: xxx.us-east-2.aws.clickhouse.cloud-backup
```

the result:
```json
{
  "prefixes": [
     "ch-s3-03238e68-e7d3-443b-a088-b850943dfb5b/4e33a077-8509-44e1-a878-4d2f7d9a5244",
      ...
  ],
  "prefix_sizes_bytes": {
    "ch-s3-2fc4173e-f657-4a3a-9b3c-0ff806cece7c/c678aa7f-c040-4c93-ad02-3639983b4372": 79025264,
    ...
  },
  "prefix_latest_object_timestamps": {
    "ch-s3-2fc4173e-f657-4a3a-9b3c-0ff806cece7c/c678aa7f-c040-4c93-ad02-3639983b4372": "2026-07-15T09:57:00Z",
    ...
  },
  "summary": {
    "total_unique_uuids": 183,
    "total_ch_s3_prefixes": 91,
    "total_full_paths": 183,
    "total_size_bytes": 25620595561,
    "total_size_human": "23.86 GB",
    "latest_object_timestamp": "2026-07-15T09:57:00Z"
  }
}
```


## Get existing backup uuid for a byoc account

Please contact the clickhouse byoc team and provide the AWS account id to them. They will give you the existing backup uuid list. Save the list to `non_terminated_prefixes.list`


## Get the dirty backup prefixes

```shell
$ python get_final_dirty_backup_prefix.py --non-terminated ~/non_terminated_prefixes.list
```

the result:

```json
{
  "dirty_paths": [
    "ch-s3-03238e68-e7d3-443b-a088-b850943dfb5b/4e33a077-8509-44e1-a878-4d2f7d9a5244",
    ...
  ],
  "dirty_paths_sizes_bytes": {
    "ch-s3-03238e68-e7d3-443b-a088-b850943dfb5b/4e33a077-8509-44e1-a878-4d2f7d9a5244": 191019308,
  },
  "summary": {
    "total_dirty_paths": 183,
    "total_dirty_size_bytes": 25620595561,
    "total_dirty_size_human": "23.86 GB"
  }
}
```


# Delete dirty prefixes

The `delete_prefixes.py` script can delete all S3 objects under the dirty prefixes identified by `get_final_dirty_data_prefix.py` or `get_final_dirty_backup_prefix.py`.

## Safety features

- **Manual confirmation required**: The script displays a summary and requires explicit "yes" confirmation before deletion
- **Dry-run mode**: Use `--dry-run` to see what would be deleted without actually deleting
- **Batch deletion**: Uses S3 batch delete API (up to 1000 objects per request) for efficient deletion
- **Multi-threaded**: Processes multiple prefixes concurrently using ThreadPoolExecutor
- **Error handling**: Continues processing even if individual prefixes fail, logs all errors
- **Progress tracking**: Shows progress bars and detailed statistics

## Delete dirty data prefixes

```shell
$ python delete_prefixes.py ${data_bucket} -i dirty_data_result.json -w 10
```

The script will:
1. Load dirty paths from the input JSON file
2. Display a summary (total prefixes, total size, sample paths)
3. Request manual confirmation
4. Delete all objects under each prefix using batch deletion
5. Show progress and final statistics

Example output:

```
Loading dirty paths from dirty_data_result.json...
Loaded 82892 dirty paths

======================================================================
DELETION SUMMARY
======================================================================
Bucket: xxx.us-east-2.aws.clickhouse.cloud-shared
Mode: LIVE DELETION
Total prefixes to process: 82892
Total size: 123806743839 bytes (115.30 GB)

Sample prefixes (first 10):
  1. ch-s3-000/09ae9cf0-31a7-4af6-8431-c553a290f097 (5.21 KB)
  2. ch-s3-000/1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d (12.45 MB)
  ...
======================================================================

⚠️  WARNING: This will permanently delete objects from S3!
This action cannot be undone.

Type 'yes' to confirm deletion, or 'no' to cancel: yes

Starting deletion...
Deleting prefixes [##############################] 82892/82892 (100.0%)

======================================================================
DELETION RESULTS
======================================================================
Total prefixes processed: 82892
Total objects found: 1234567
Total objects deleted: 1234567
Total errors: 0

Total elapsed time: 1234.56 seconds
```

## Delete dirty backup prefixes

```shell
$ python delete_prefixes.py ${backup_bucket} -i dirty_backup_result.json -w 10
```

## Command-line options

- `bucket_name` (required): Name of the S3 bucket to delete from
- `-i, --input`: Input JSON file with dirty paths (default: `dirty_data_result.json`)
- `-w, --workers`: Number of concurrent workers (default: 10)
- `--dry-run`: Show what would be deleted without actually deleting
- `-o, --output-log`: Optional log file to save deletion results

## Examples

Dry-run to preview what will be deleted:

```shell
$ python delete_prefixes.py ${bucket} -i dirty_data_result.json --dry-run
```

Delete with custom worker count and save log:

```shell
$ python delete_prefixes.py ${bucket} -i dirty_data_result.json -w 20 -o deletion_log.json
```

## Notes

- The script deletes **all objects** under each prefix path
- Deletion is **permanent** and cannot be undone
- Always use `--dry-run` first to verify what will be deleted
- The script processes prefixes concurrently but deletes objects in batches of 1000
- If errors occur, the script will continue processing and report all errors at the end