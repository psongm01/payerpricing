# AGENT.md — Linux / Azure Monthly Refresh Plan

## Purpose

This document describes the Linux-first operating plan for the monthly UHC Texas
Transparency in Coverage refresh.

---

## Scope

Monthly refresh scope:

- Payer: UHC / UMR family only
- Geography: Texas-labeled and Texas-eligible files
- Providers: Texas NPIs
- Billing codes: all codes unless explicitly narrowed later

Expected matched file volume:

- ~300 pricing files per monthly cycle
- most files are ~14 GB compressed

---

## Existing code to reuse

The Linux/Azure version should be built on top of the existing Python
ingestion scripts first.

Treat these files as the current source of truth for extraction logic:

- `ingest/stream_index.py`
- `ingest/scan_pricing_urls.py`
- `ingest/stream_pricing.py`
- `ingest/load_nppes.py`

When writing new code:

- prefer wrapping and orchestrating these scripts
- prefer Linux/path/config portability changes over parser rewrites
- do not redesign the extraction pipeline unless there is a proven blocker
- keep the existing streaming/parquet logic intact whenever possible

New code for the cloud version should focus primarily on:

- monthly coordination
- shard generation
- worker bootstrap
- upload to storage
- shard status tracking
- Linux-friendly configuration and runtime defaults

Do not casually replace working extraction behavior that has already been
validated on the Windows workflow.

---

## Operating model

The refresh is a batch job, not a continuously running service.

We will use:

- 1 coordinator job
- 10 Linux worker VMs
- Azure Blob Storage or ADLS Gen2 as durable storage
- local ephemeral disk on each worker for one-file-at-a-time staging

Workers download a single file locally, process it, upload parquet outputs, and
delete the local staged raw file.

---

## High-level monthly flow

1. Trigger monthly refresh.
2. Coordinator builds the month’s NPI and matched URL inputs.
3. Coordinator shards the matched URL list into 10 balanced shard files.
4. Coordinator uploads shard manifests to storage.
5. 10 Linux workers start.
6. Each worker processes one shard.
7. Each worker uploads parquet outputs and status markers.
8. Workers shut down or scale to zero.

---

## Coordinator responsibilities

The coordinator is the only job that should prepare the worklist.

Responsibilities:

1. Run `ingest/load_nppes.py`
   - produce Texas NPI parquet
   - taxonomy is optional

2. Run `ingest/scan_pricing_urls.py`
   - generate:
     - `matched_pricing_urls.txt`
     - `matched_pricing_urls_manifest.txt`
   - use UHC URL filters and Texas state filters

3. Create shard files
   - split matched URLs into 10 shard files
   - prefer balancing by `size_bytes` from the manifest rather than simple line count

4. Upload job artifacts to storage
   - matched URL list
   - manifest
   - shard files

5. Start worker jobs
   - assign one shard per worker

The coordinator can run as:

- a small Linux VM
- an Azure Container Instance job
- an Azure Automation / Function-triggered script
- a GitHub Actions workflow that invokes Azure resources

---

## Worker responsibilities

Workers do not discover files. They only process assigned shards.

Each worker should:

1. Read its assigned shard manifest
2. Download one matched pricing file at a time
3. Run `ingest/stream_pricing.py`
4. Upload parquet outputs after each processed file
5. Delete local staged raw files
6. Write success/failure markers
7. Exit when the shard is complete

Workers should be stateless apart from local temp files.

---

## Storage layout

Suggested Azure storage layout:

```text
jobs/
  2026-04/
    matched_pricing_urls.txt
    matched_pricing_urls_manifest.txt
    shards/
      shard_01.txt
      shard_02.txt
      ...
      shard_10.txt
    status/
      shard_01.started
      shard_01.done
      shard_02.failed

parquet/
  2026-04/
    tic_provider_reference/
    tic_price/
    tic_out_of_network_allowed/
    nppes_provider/
```

Rules:

- job manifests live under `jobs/<month>/`
- parquet outputs live under `parquet/<month>/`
- month partitioning makes reruns and rollback easier

---

## Sharding strategy

Do not shard only by line count.

Use `matched_pricing_urls_manifest.txt` and balance shards by `size_bytes`.

Goal:

- each worker gets roughly the same compressed-byte workload
- avoid one worker being stuck with several oversized files while others finish early

Simple strategy:

1. sort manifest rows by descending `size_bytes`
2. greedily assign each next file to the shard with the lowest current byte total

This is good enough for monthly batching.

---

## Linux execution expectations

The production worker environment is Linux.

Implications:

- no `D:\...` or `D:/...` hardcoded paths
- no reliance on the Windows `py` launcher
- prefer relative repo paths or environment variables
- scripts should run under `python3`

The Python logic should remain mostly portable. The main cleanup area is path
defaults and shell/runtime assumptions.

---

## Recommended runtime settings

Default worker behavior:

- local staging enabled
- filtered extraction enabled unless a broader extract is explicitly required
- upload outputs continuously
- delete staged raw files after successful upload

One worker should process:

- one pricing file at a time
- one shard sequentially

Do not keep multiple 14 GB raw files on local disk unless there is a specific
reason.

---

## Upload behavior

Do not wait until the full monthly batch is complete to upload outputs.

Preferred pattern:

1. process one pricing file
2. write parquet locally
3. upload parquet outputs to storage
4. verify upload success
5. delete local raw staged file

This reduces:

- local disk pressure
- blast radius of failures
- risk of losing many hours of work on a VM failure

---

## VM lifecycle

The 10 workers should not stay running between monthly refreshes.

Preferred lifecycle:

- create or scale out workers at the start of the monthly run
- deallocate / scale to zero when complete

Do not maintain 10 always-on VMs for a monthly-only workload.

Good infrastructure patterns:

- Azure VM Scale Set scaled to zero between runs
- Azure Batch pool created per job or resized on schedule

---

## Failure handling

The shard is the retry unit.

If a worker fails:

- mark the shard failed
- rerun that shard only

Required artifacts:

- `started` marker
- `done` marker
- `failed` marker

Workers must be idempotent:

- deterministic parquet output naming
- safe reruns
- upload overwrite rules should be explicit

---

## Monthly trigger options

Any of these can work:

- Azure Automation scheduled runbook
- Azure Function with timer trigger
- Logic App scheduled workflow
- GitHub Actions scheduled workflow

The trigger only needs to start the coordinator. The coordinator is responsible
for creating the month’s shard plan and starting workers.

---

## Suggested near-term implementation order

1. Make scripts Linux-friendly without changing their core extraction logic
2. Add a small shard-builder utility based on the matched manifest
3. Add per-file upload-to-storage support
4. Add shard status markers
5. Add coordinator startup script
6. Add worker startup script
7. Test on one Linux worker
8. Scale to 10 workers

---

## Non-goals for first cloud version

Do not overbuild the first batch system.

Avoid adding:

- complex distributed orchestration
- warehouse migration before the batch pipeline is stable
- embedded BI delivery before the data refresh is reliable

The first goal is dependable monthly refresh, not a perfect platform.

---

## Success definition

The Linux/Azure monthly pipeline is successful when:

1. A monthly trigger starts the run automatically
2. Coordinator produces shard manifests automatically
3. 10 workers can complete the batch with no manual VM setup
4. Parquet outputs are uploaded to storage automatically
5. Failed shards can be retried without rerunning the whole month
6. Workers scale to zero after completion
