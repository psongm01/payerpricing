"""Rust worker entrypoint for a single monthly pricing-file shard."""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from cloud.storage import StorageClient

log = logging.getLogger(__name__)

DATASET_DIRS = (
    "tic_provider_reference",
    "tic_price",
)


@dataclass(frozen=True)
class ShardEntry:
    url: str
    payer_code: str | None = None


def parse_shard_entry(line: str) -> ShardEntry | None:
    parts = line.rstrip("\n").split("|")
    url = parts[0].strip() if parts else ""
    if not url:
        return None
    payer_code = parts[4].strip() if len(parts) >= 5 and parts[4].strip() else None
    return ShardEntry(url=url, payer_code=payer_code)


def file_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


def run(command: list[str]) -> None:
    log.info("Running: %s", " ".join(shlex.quote(part) for part in command))
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}: {command[0]}")


def validate_parquet(path: Path) -> int:
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path)
    return parquet_file.metadata.num_rows


def upload_changed_outputs(
    storage: StorageClient,
    output_dir: Path,
    month: str,
    started_at: float,
    delete_after_upload: bool = False,
) -> int:
    uploaded = 0
    for dataset in DATASET_DIRS:
        dataset_dir = output_dir / dataset
        if not dataset_dir.exists():
            continue
        for path in dataset_dir.rglob("*.parquet"):
            if path.is_file() and path.stat().st_mtime >= started_at:
                rows = validate_parquet(path)
                log.info("Validated %s rows=%d", path, rows)
                relative = path.relative_to(output_dir).as_posix()
                storage.upload_file(path, f"parquet/{month}/{relative}", overwrite=True)
                uploaded += 1
                if delete_after_upload:
                    path.unlink()
    return uploaded


def write_one_url_shard(url: str, path: Path) -> None:
    path.write_text(url + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Process one monthly shard with the Rust pricing streamer.")
    parser.add_argument("--month", required=True, help="Month partition, e.g. 2026-05")
    parser.add_argument("--shard", required=True, help="Shard id, e.g. shard_01")
    parser.add_argument("--storage-root", default=os.getenv("TIC_STORAGE_ROOT", "data/cloud"))
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--work-dir", default="/tmp/tic-refresh")
    parser.add_argument("--rust-bin", default=None)
    parser.add_argument("--payer-code", default="UHC")
    parser.add_argument("--state", default="TX")
    parser.add_argument("--source-version", default="")
    parser.add_argument("--keep-downloaded", action="store_true")
    parser.add_argument(
        "--delete-local-parquet-after-upload",
        action="store_true",
        help="Remove local staging parquet files after successful ADLS upload.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=None,
        help="Process at most this many URLs from the shard.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    repo_root = Path(args.repo_root).resolve()
    rust_bin = Path(args.rust_bin) if args.rust_bin else repo_root / "rust" / "stream_pricing_serde" / "target" / "release" / "stream_pricing_serde"
    work_root = Path(args.work_dir) / args.month / args.shard
    shard_path = work_root / f"{args.shard}.txt"
    one_url_shard_path = work_root / "current_rust_url.txt"
    output_dir = work_root / "rust_parquet"
    staging_dir = work_root / "pricing_staging"
    nppes_dir = work_root / "nppes_provider"
    target_npis_file = work_root / "target_npis.txt"
    work_root.mkdir(parents=True, exist_ok=True)

    if not rust_bin.exists() and not args.dry_run:
        raise FileNotFoundError(f"Rust binary not found: {rust_bin}. Run cargo build --release.")

    storage = StorageClient(args.storage_root, dry_run=args.dry_run)
    status_prefix = f"jobs/{args.month}/status"

    try:
        storage.write_text(f"{status_prefix}/{args.shard}.started", f"started {time.ctime()}\n", work_root)
        storage.download_file(f"jobs/{args.month}/shards/{args.shard}.txt", shard_path)
        storage.download_prefix(f"parquet/{args.month}/nppes_provider", nppes_dir)

        if args.dry_run:
            log.info("[dry-run] export NPIs %s -> %s", nppes_dir, target_npis_file)
        else:
            run(
                [
                    args.python,
                    str(repo_root / "ingest" / "export_npis_text.py"),
                    "--nppes",
                    str(nppes_dir),
                    "--output",
                    str(target_npis_file),
                ]
            )

        entries = [
            entry
            for line in shard_path.read_text(encoding="utf-8").splitlines()
            for entry in [parse_shard_entry(line)]
            if entry is not None
        ]
        if args.limit_files is not None:
            entries = entries[: args.limit_files]
        log.info("Processing %d URL(s) from %s", len(entries), args.shard)

        for index, entry in enumerate(entries, start=1):
            url = entry.url
            payer_code = entry.payer_code or args.payer_code
            marker_key = f"{status_prefix}/{args.shard}/{file_key(url)}.done"
            if storage.exists(marker_key):
                log.info("[%d/%d] skipping completed URL: %s", index, len(entries), url.split("?")[0][:140])
                continue
            log.info("[%d/%d] payer=%s %s", index, len(entries), payer_code, url.split("?")[0][:140])
            write_one_url_shard(url, one_url_shard_path)
            started_at = time.time()

            command = [
                str(rust_bin),
                "--shard",
                str(one_url_shard_path),
                "--download-dir",
                str(staging_dir),
                "--target-npis-file",
                str(target_npis_file),
                "--output",
                str(output_dir),
                "--payer-code",
                payer_code,
                "--file-month",
                args.month,
                "--state",
                args.state,
            ]
            if args.source_version:
                command.extend(["--source-version", args.source_version])
            if args.keep_downloaded:
                command.append("--keep-downloaded")

            if args.dry_run:
                log.info("[dry-run] would run: %s", " ".join(shlex.quote(part) for part in command))
            else:
                storage.write_text(
                    f"{status_prefix}/{args.shard}.heartbeat",
                    f"running file {index}/{len(entries)} payer={payer_code} {time.ctime()}\n{url}\n",
                    work_root,
                )
                run(command)

            uploaded = upload_changed_outputs(
                storage,
                output_dir,
                args.month,
                started_at,
                delete_after_upload=args.delete_local_parquet_after_upload,
            )
            if not args.dry_run and uploaded != len(DATASET_DIRS):
                raise RuntimeError(
                    f"Expected {len(DATASET_DIRS)} parquet uploads for {url}, uploaded {uploaded}"
                )
            log.info("[%d/%d] uploaded %d parquet file(s)", index, len(entries), uploaded)
            storage.write_text(marker_key, url + "\n", work_root)
            storage.write_text(
                f"{status_prefix}/{args.shard}.heartbeat",
                f"completed file {index}/{len(entries)} payer={payer_code} {time.ctime()}\n{url}\n",
                work_root,
            )

        storage.write_text(f"{status_prefix}/{args.shard}.done", f"done {time.ctime()}\n", work_root)
    except Exception as exc:
        log.exception("Rust shard failed: %s", exc)
        storage.write_text(f"{status_prefix}/{args.shard}.failed", f"{type(exc).__name__}: {exc}\n", work_root)
        raise


if __name__ == "__main__":
    main()
