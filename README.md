# Price Transparency Linux Refresh

Linux/Azure batch pipeline for the monthly UHC/UMR Texas Transparency in
Coverage refresh.

## Files To Commit

Commit these source folders and files:

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

Do not commit `data/`. It contains local raw files, generated manifests,
parquet outputs, and large monthly artifacts.

## Cloud Entry Points

Coordinator:

```bash
python3 -m cloud.coordinator --month 2026-04 --storage-root "$TIC_STORAGE_ROOT"
```

Worker:

```bash
python3 -m cloud.worker --month 2026-04 --shard shard_01 --storage-root "$TIC_STORAGE_ROOT"
```

See [deploy/azure-linux/README.md](deploy/azure-linux/README.md) for the exact
VM setup and deployment flow.
