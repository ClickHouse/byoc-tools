# byoc-tools

Tools that ClickHouse BYOC customers run in their own cloud accounts.

| Directory | What it does | Clouds |
|---|---|---|
| [`kill-switch/`](kill-switch/) | Disconnect ClickHouse from your account, for example during a security incident, and reconnect later. Services keep serving queries while disconnected. | AWS, GCP, Azure |
| [`s3-cleanup/`](s3-cleanup/) | Find and delete data that terminated services left behind in your BYOC S3 buckets. | AWS |

Each directory has a README with its requirements and usage.

## Layout

- One top-level directory per tool. It holds the tool's README, code and dependency files, and you run the tool from there.
- `make build` builds every tool that needs building. `make lint` runs the same checks as CI.
- Tool output files and kill-switch state directories contain account data. They are gitignored; never commit them.

## Adding a tool

- Create a top-level directory with a README, and add a row to the table above.
- Shell scripts must pass `shellcheck`, Python must compile, and Go must pass `go vet`. Add new checks to the `lint` target in the [`Makefile`](Makefile).
