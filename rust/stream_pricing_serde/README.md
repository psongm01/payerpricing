# stream_pricing_serde

Experimental Rust version of `ingest/stream_pricing.py` for local pricing
files. It uses `serde_json` streaming visitors over a gzip/plain JSON reader and
writes staging Parquet outputs for:

- `tic_provider_reference`
- `tic_price`

It is intentionally separate from the validated Python extractor while the Rust
path is tested and benchmarked.

## Build

```bash
cd rust/stream_pricing_serde
cargo build --release
```

## Run On A Local Downloaded File

```bash
./target/release/stream_pricing_serde \
  --file /mnt/resource/pricing/file.json.gz \
  --source-url "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/..." \
  --output /mnt/resource/rust_parquet \
  --payer-code UHC \
  --file-month 2026-05 \
  --state TX
```

`--source-url` is the value written to `source_pricing_file`; the local path is
only used for reading bytes.

## Optional NPI Filter

For filtered extraction, pass a plain text file with one NPI per line:

```bash
./target/release/stream_pricing_serde \
  --file /mnt/resource/pricing/file.json.gz \
  --source-url "https://..." \
  --target-npis-file /mnt/resource/tx_npis.txt \
  --output /mnt/resource/rust_parquet
```

If `--target-npis-file` is omitted, the tool performs a broad/full extraction.

## Current Scope

- Reads only local `.json` / `.json.gz` files.
- Preserves the original URL via `--source-url`.
- Writes staging Parquet files, not partitioned dataset folders.
- Does not emit `tic_out_of_network_allowed` yet.
- Code-prefix filtering is not implemented yet; all billing codes are kept.
