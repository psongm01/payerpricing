# Azure Linux Deployment

This folder contains the files you use on Azure VMs or Azure Batch tasks.

## What To Upload To Your Repo

Upload/commit exactly this source set:

```text
AGENTS.md
README.md
requirements.txt
.gitignore
cloud/
deploy/
ingest/
utils/
```

Do not upload these to the repo:

```text
data/
.tmp/
.claude/
__pycache__/
.venv/
*.log
```

The `data/` folder is local/generated. The coordinator creates new monthly
manifests and uploads them to Azure storage. Workers download what they need.

## What Must Exist For The Coordinator

The coordinator needs these inputs available on the coordinator machine before
it runs:

```text
data/raw/nppes/npidata_pfile_*.csv
data/raw/taxonomy/nucc_taxonomy.csv
```

These can be copied to the coordinator VM, mounted from storage, or produced by
an earlier step. The coordinator now discovers UHC index URLs and builds
`plan_pricing_bridge.parquet` automatically when `TIC_UHC_MRF_SEED_URL` is set.
Workers do not need these source inputs preloaded.

## Do I Create Folders Manually?

No. The scripts create local working folders. Azure Blob/ADLS folder paths are
created implicitly by upload.

Workers create local folders under:

```text
/mnt/resource/tic-refresh/<month>/<shard>/
```

Storage gets these prefixes automatically:

```text
jobs/<month>/
jobs/<month>/shards/
jobs/<month>/status/
parquet/<month>/
```

## One-Time VM Setup

From the repo root on each VM:

```bash
bash deploy/azure-linux/setup_vm.sh
```

Copy the env template once and edit it:

```bash
cp deploy/azure-linux/env.example .env.azure
nano .env.azure
```

For production, use the VM's system-assigned managed identity. Set:

```bash
TIC_STORAGE_ROOT="https://sttic.blob.core.windows.net/tic-refresh"
```

The VM identity needs `Storage Blob Data Contributor` on the storage account or
the `tic-refresh` container/filesystem.

## Run Coordinator

```bash
set -a
. ./.env.azure
set +a
bash deploy/azure-linux/run_coordinator.sh 2026-04
```

## Run Workers

Run one shard per worker:

```bash
set -a
. ./.env.azure
set +a
bash deploy/azure-linux/run_rust_worker.sh 2026-04 shard_01 --delete-local-parquet-after-upload
```

Repeat with `shard_02` through `shard_10`, or have Azure Batch/VMSS pass the
shard id to each worker.

## Local Dry Run

You can test without Azure by setting storage to a local folder:

```bash
export TIC_STORAGE_ROOT=data/cloud-test
bash deploy/azure-linux/run_coordinator.sh 2026-04 --skip-nppes --skip-scan
bash deploy/azure-linux/run_worker.sh 2026-04 shard_01
```
