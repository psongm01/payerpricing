"""Worker entrypoint for a single monthly shard."""

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
    "tic_out_of_network_allowed",
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


def upload_changed_outputs(
    storage: StorageClient,
    output_dir: Path,
    month: str,
    started_at: float,
    delete_after_upload: bool = False,
) -> None:
    for dataset in DATASET_DIRS:
        dataset_dir = output_dir / dataset
        if not dataset_dir.exists():
            continue
        for path in dataset_dir.rglob("*"):
            if path.is_file() and path.stat().st_mtime >= started_at:
                relative = path.relative_to(output_dir).as_posix()
                storage.upload_file(path, f"parquet/{month}/{relative}", overwrite=True)
                if delete_after_upload:
                    path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description="Process one monthly pricing-file shard.")
    parser.add_argument("--month", required=True, help="Month partition, e.g. 2026-04")
    parser.add_argument("--shard", required=True, help="Shard id, e.g. shard_01")
    parser.add_argument("--storage-root", default=os.getenv("TIC_STORAGE_ROOT", "data/cloud"))
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--work-dir", default="/tmp/tic-refresh")
    parser.add_argument("--stream-remote", action="store_true")
    parser.add_argument("--full-extract", action="store_true")
    parser.add_argument("--skip-oon", action="store_true")
    parser.add_argument("--code-prefixes", nargs="*", default=[])
    parser.add_argument("--payer-code", default="UHC")
    parser.add_argument("--state", default="TX")
    parser.add_argument("--source-version", default="")
    parser.add_argument(
        "--delete-local-parquet-after-upload",
        action="store_true",
        help="Remove local staging parquet files after successful ADLS upload.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    repo_root = Path(args.repo_root).resolve()
    work_root = Path(args.work_dir) / args.month / args.shard
    shard_path = work_root / f"{args.shard}.txt"
    one_url_path = work_root / "current_url.txt"
    output_dir = work_root / "parquet"
    staging_dir = work_root / "pricing_staging"
    nppes_dir = work_root / "nppes_provider"
    work_root.mkdir(parents=True, exist_ok=True)

    storage = StorageClient(args.storage_root, dry_run=args.dry_run)
    status_prefix = f"jobs/{args.month}/status"

    try:
        storage.write_text(f"{status_prefix}/{args.shard}.started", f"started {time.ctime()}\n", work_root)
        storage.download_file(f"jobs/{args.month}/shards/{args.shard}.txt", shard_path)
        if not args.full_extract:
            storage.download_prefix(f"parquet/{args.month}/nppes_provider", nppes_dir)

        entries = [
            entry
            for line in shard_path.read_text(encoding="utf-8").splitlines()
            for entry in [parse_shard_entry(line)]
            if entry is not None
        ]
        log.info("Processing %d URL(s) from %s", len(entries), args.shard)

        for index, entry in enumerate(entries, start=1):
            url = entry.url
            payer_code = entry.payer_code or args.payer_code
            marker_key = f"{status_prefix}/{args.shard}/{file_key(url)}.done"
            log.info("[%d/%d] payer=%s %s", index, len(entries), payer_code, url[:120])
            one_url_path.write_text(url + "\n", encoding="utf-8")
            started_at = time.time()
            command = [
                args.python,
                str(repo_root / "ingest" / "stream_pricing.py"),
                "--urls",
                str(one_url_path),
                "--output",
                str(output_dir),
                "--download-dir",
                str(staging_dir),
                "--workers",
                "1",
                "--delete-downloaded",
                "--payer-code",
                payer_code,
                "--file-month",
                args.month,
                "--state",
                args.state,
            ]
            if args.source_version:
                command.extend(["--source-version", args.source_version])
            if args.stream_remote:
                command.append("--stream-remote")
            if args.full_extract:
                command.append("--full-extract")
            else:
                command.extend(["--target-npis-parquet", str(nppes_dir)])
            if args.skip_oon:
                command.append("--skip-oon")
            if args.code_prefixes:
                command.extend(["--code-prefixes", *args.code_prefixes])

            run(command)
            upload_changed_outputs(
                storage,
                output_dir,
                args.month,
                started_at,
                delete_after_upload=args.delete_local_parquet_after_upload,
            )
            storage.write_text(marker_key, url + "\n", work_root)

        storage.write_text(f"{status_prefix}/{args.shard}.done", f"done {time.ctime()}\n", work_root)
    except Exception as exc:
        log.exception("Shard failed: %s", exc)
        storage.write_text(f"{status_prefix}/{args.shard}.failed", f"{type(exc).__name__}: {exc}\n", work_root)
        raise


if __name__ == "__main__":
    main()
