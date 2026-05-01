"""Coordinator entrypoint for a Linux/Azure monthly refresh."""

from __future__ import annotations

import argparse
import glob
import logging
import os
import shlex
import subprocess
import sys
from pathlib import Path

from cloud.shard_builder import write_shards
from cloud.storage import StorageClient

log = logging.getLogger(__name__)

NON_TX_STATES = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
]


def run(command: list[str], dry_run: bool = False) -> None:
    log.info("Running: %s", " ".join(shlex.quote(part) for part in command))
    if dry_run:
        return
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}: {command[0]}")


def first_glob(pattern: str) -> str:
    matches = sorted(glob.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files match: {pattern}")
    return matches[-1]


def upload_month_artifacts(
    storage: StorageClient,
    month: str,
    index_urls: Path,
    index_manifest: Path,
    matched_urls: Path,
    manifest: Path,
    shards_dir: Path,
    nppes_out: Path,
    bridge_out: Path,
    dry_run: bool,
) -> None:
    for artifact in (index_urls, index_manifest, matched_urls, manifest):
        if not artifact.exists() and dry_run:
            continue
        if not artifact.exists():
            log.info("Skipping missing artifact: %s", artifact)
            continue
        storage.upload_file(artifact, f"jobs/{month}/{artifact.name}")
    storage.upload_prefix(shards_dir, f"jobs/{month}/shards")
    storage.upload_prefix(nppes_out, f"parquet/{month}/nppes_provider")
    storage.upload_prefix(bridge_out, f"parquet/{month}/plan_pricing_bridge")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare a monthly UHC Texas refresh job.")
    parser.add_argument("--month", required=True, help="Month partition, e.g. 2026-04")
    parser.add_argument("--storage-root", default=os.getenv("TIC_STORAGE_ROOT", "data/cloud"))
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--nppes", default="data/raw/nppes/npidata_pfile_*.csv")
    parser.add_argument("--taxonomy", default="data/raw/taxonomy/nucc_taxonomy.csv")
    parser.add_argument("--bridge", default="data/parquet/plan_pricing_bridge")
    parser.add_argument("--work-dir", default="data/monthly")
    parser.add_argument(
        "--uhc-mrf-seed-url",
        default=os.getenv("TIC_UHC_MRF_SEED_URL"),
        help="Signed UHC public-mrf blob URL used to discover monthly index URLs.",
    )
    parser.add_argument(
        "--skip-index-discovery",
        action="store_true",
        help="Do not discover index URLs or rebuild plan_pricing_bridge for this month.",
    )
    parser.add_argument("--shards", type=int, default=10)
    parser.add_argument("--scan-workers", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-nppes", action="store_true")
    parser.add_argument("--skip-scan", action="store_true")
    parser.add_argument(
        "--upload-only",
        action="store_true",
        help="Only upload existing monthly artifacts to storage; do not run discovery, NPPES, scan, or shard generation.",
    )
    parser.add_argument("--worker-command-template", default=None,
                        help="Optional shell command template to start a worker. Supports {month}, {shard}, {storage_root}.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    repo_root = Path(args.repo_root).resolve()
    month_root = repo_root / args.work_dir / args.month
    raw_dir = month_root / "raw"
    parquet_dir = month_root / "parquet"
    shards_dir = raw_dir / "shards"
    raw_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    nppes_out = parquet_dir / "nppes_provider"
    bridge_out = parquet_dir / "plan_pricing_bridge"
    index_urls = raw_dir / "index_urls.txt"
    index_manifest = raw_dir / "index_urls_manifest.txt"
    matched_urls = raw_dir / "matched_pricing_urls.txt"
    manifest = raw_dir / "matched_pricing_urls_manifest.txt"

    if args.upload_only:
        storage = StorageClient(args.storage_root, dry_run=args.dry_run)
        upload_month_artifacts(
            storage,
            args.month,
            index_urls,
            index_manifest,
            matched_urls,
            manifest,
            shards_dir,
            nppes_out,
            bridge_out,
            args.dry_run,
        )
        return

    if not args.skip_index_discovery:
        if not args.uhc_mrf_seed_url:
            raise ValueError(
                "Index discovery requires --uhc-mrf-seed-url or TIC_UHC_MRF_SEED_URL. "
                "Use --skip-index-discovery to reuse an existing bridge parquet."
            )
        run(
            [
                args.python,
                str(repo_root / "ingest" / "discover_index_urls.py"),
                "--seed-url",
                args.uhc_mrf_seed_url,
                "--month",
                args.month,
                "--output",
                str(index_urls),
                "--manifest",
                str(index_manifest),
            ],
            dry_run=args.dry_run,
        )
        run(
            [
                args.python,
                str(repo_root / "ingest" / "stream_index.py"),
                "--url-list",
                str(index_urls),
                "--output",
                str(bridge_out / "plan_pricing_bridge.parquet"),
                "--payer-code",
                "UHC",
                "--file-month",
                args.month,
                "--state",
                "TX",
            ],
            dry_run=args.dry_run,
        )

    if not args.skip_nppes:
        nppes_csv = first_glob(str(repo_root / args.nppes))
        command = [
            args.python,
            str(repo_root / "ingest" / "load_nppes.py"),
            "--nppes",
            nppes_csv,
            "--output",
            str(nppes_out),
            "--states",
            "TX",
        ]
        taxonomy = repo_root / args.taxonomy
        if taxonomy.exists():
            command.extend(["--taxonomy", str(taxonomy)])
        run(command, dry_run=args.dry_run)

    if not args.skip_scan:
        run(
            [
                args.python,
                str(repo_root / "ingest" / "scan_pricing_urls.py"),
                "--nppes",
                str(nppes_out),
                "--bridge",
                str(bridge_out if not args.skip_index_discovery else repo_root / args.bridge),
                "--output",
                str(matched_urls),
                "--url-contains",
                "uhc",
                "umr",
                "--target-states",
                "Texas",
                "TX",
                "--skip-states",
                *NON_TX_STATES,
                "--npi-only",
                "--workers",
                str(args.scan_workers),
            ],
            dry_run=args.dry_run,
        )

    if not args.dry_run:
        write_shards(manifest, shards_dir, args.shards)

    storage = StorageClient(args.storage_root, dry_run=args.dry_run)
    upload_month_artifacts(
        storage,
        args.month,
        index_urls,
        index_manifest,
        matched_urls,
        manifest,
        shards_dir,
        nppes_out,
        bridge_out,
        args.dry_run,
    )

    if args.worker_command_template:
        for shard_num in range(1, args.shards + 1):
            command = args.worker_command_template.format(
                month=args.month,
                shard=f"shard_{shard_num:02d}",
                storage_root=args.storage_root,
            )
            log.info("Starting worker command: %s", command)
            if not args.dry_run:
                subprocess.run(command, shell=True, check=False)


if __name__ == "__main__":
    main()
