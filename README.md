# byoc-tools

* list data prefixes
* list backup prefixes

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
    "summary": {
        "total_unique_prefixes": 12345,
        "total_uuids_with_prefixes": 4096,
        "total_full_paths": 82892,
        "total_size_bytes": 123806743839,
        "total_size_human": "115.30 GB"
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
  "summary": {
    "total_unique_uuids": 183,
    "total_ch_s3_prefixes": 91,
    "total_full_paths": 183,
    "total_size_bytes": 25620595561,
    "total_size_human": "23.86 GB"
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