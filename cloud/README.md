# Linux / Azure Monthly Refresh

These entrypoints wrap the existing ingestion scripts for the monthly payer
Texas batch flow. They do not replace the validated streaming extraction logic.

## Install on Linux

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r requirements.txt
```

Production uploads use the Azure Python SDK with managed identity. For local
dry runs, `--storage-root` can be a normal directory.

## Build shards only

```bash
python3 -m cloud.shard_builder \
  --manifest data/raw/matched_pricing_urls_manifest.txt \
  --output-dir data/raw/shards \
  --shards 10
```

Shard files preserve the manifest line format:

```text
url|size_bytes|signal|reporting_entity_name
url|size_bytes|signal|reporting_entity_name|payer_code
```

## Coordinator

```bash
python3 -m cloud.coordinator \
  --month 2026-04 \
  --storage-root "https://sttic.blob.core.windows.net/tic-refresh" \
  --payers uhc
```

The coordinator:

- discovers selected payer index URLs
- builds payer-specific `plan_pricing_bridge.parquet` files from those index files
- writes Texas NPPES parquet under `data/monthly/<month>/parquet/nppes_provider`
- scans Texas-NPI-matched pricing URLs into payer manifests
- combines payer manifests into `matched_pricing_urls_manifest.txt`
- builds 10 byte-balanced shard files from the combined manifest
- uploads job artifacts under `jobs/<month>/`
- uploads NPPES provider parquet under `parquet/<month>/nppes_provider`

Set `TIC_UHC_MRF_SEED_URL` to any signed UHC public-MRF blob URL. The
coordinator uses its SAS query to list `YYYY-MM-01/` and filters blob names
containing `index`.

Run multiple payers by selecting them explicitly:

```bash
python3 -m cloud.coordinator \
  --month 2026-04 \
  --storage-root "https://sttic.blob.core.windows.net/tic-refresh" \
  --payers uhc bcbstx cigna aetna \
  --bcbstx-index-url "https://app0004702110a5prdnc868.blob.core.windows.net/toc/2026-04-21_Blue-Cross-and-Blue-Shield-of-Texas_index.json" \
  --cigna-index-url "$TIC_CIGNA_INDEX_URL" \
  --aetna-insurer-code AETNACVS_I \
  --aetna-brand-code ALICSI
```

If UHC already ran and you only want the remaining payer manifests included in
this month's Azure folder, run:

```bash
python3 -m cloud.coordinator \
  --month 2026-04 \
  --storage-root "https://sttic.blob.core.windows.net/tic-refresh" \
  --payers bcbstx cigna aetna
```

Use `--worker-command-template` if the coordinator environment can start worker
VM jobs directly.

## Worker

```bash
python3 -m cloud.worker \
  --month 2026-04 \
  --shard shard_01 \
  --storage-root "https://sttic.blob.core.windows.net/tic-refresh" \
  --work-dir /mnt/resource/tic-refresh \
  --payer-code UHC \
  --state TX
```

Each worker downloads only its assigned shard and NPPES provider parquet, then
processes one pricing file at a time through `ingest/stream_pricing.py`. It
uploads completed parquet files after each source file and writes status
artifacts under `jobs/<month>/status/`.

## Rust Worker

Use the Rust worker as the primary high-throughput pricing path:

```bash
bash deploy/azure-linux/run_rust_worker.sh 2026-05 shard_01 --delete-local-parquet-after-upload
```

The Rust worker downloads the shard and NPPES parquet from ADLS, exports NPIs to
a text file, runs `rust/stream_pricing_serde`, uploads completed `tic_price` and
`tic_provider_reference` Parquet after each pricing file, and writes the same
status markers as the Python worker. OON output is not implemented in the Rust
path yet.

When a shard row has the fifth `payer_code` column, `cloud.rust_worker` passes
that payer code to the Rust binary for that URL. Older four-column UHC-only
shards still work and fall back to the worker's `--payer-code` argument.

Status markers are written under:

```text
jobs/<month>/status/<shard>.started
jobs/<month>/status/<shard>.done
jobs/<month>/status/<shard>.failed
jobs/<month>/status/<shard>/<file-hash>.done
```
