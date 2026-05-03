"""Coordinator entrypoint for a Linux/Azure monthly refresh."""

from __future__ import annotations

import argparse
import glob
import logging
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from cloud.shard_builder import write_shards
from cloud.storage import StorageClient

log = logging.getLogger(__name__)

SUPPORTED_PAYERS = ("uhc", "bcbstx", "cigna", "aetna")

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


@dataclass
class PayerPaths:
    key: str
    payer_code: str
    raw_dir: Path
    bridge_dir: Path
    index_urls: Path
    index_manifest: Path
    matched_urls: Path
    matched_manifest: Path


def payer_code_for(key: str) -> str:
    return {
        "uhc": "UHC",
        "bcbstx": "BCBSTX",
        "cigna": "CIGNA",
        "aetna": "AETNA",
    }[key]


def normalize_payers(values: list[str]) -> list[str]:
    payers: list[str] = []
    for value in values:
        for part in value.split(","):
            payer = part.strip().lower()
            if not payer:
                continue
            if payer not in SUPPORTED_PAYERS:
                raise ValueError(f"Unsupported payer {payer!r}. Choose from: {', '.join(SUPPORTED_PAYERS)}")
            if payer not in payers:
                payers.append(payer)
    return payers


def make_payer_paths(raw_dir: Path, parquet_dir: Path, payer: str) -> PayerPaths:
    payer_raw = raw_dir / "payers" / payer
    return PayerPaths(
        key=payer,
        payer_code=payer_code_for(payer),
        raw_dir=payer_raw,
        bridge_dir=parquet_dir / f"plan_pricing_bridge_{payer}",
        index_urls=payer_raw / "index_urls.txt",
        index_manifest=payer_raw / "index_urls_manifest.txt",
        matched_urls=payer_raw / "matched_pricing_urls.txt",
        matched_manifest=payer_raw / "matched_pricing_urls_manifest.txt",
    )


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


def upload_multi_payer_artifacts(
    storage: StorageClient,
    month: str,
    raw_dir: Path,
    parquet_dir: Path,
    matched_urls: Path,
    manifest: Path,
    shards_dir: Path,
    nppes_out: Path,
    payer_paths: list[PayerPaths],
    dry_run: bool,
) -> None:
    for artifact in (matched_urls, manifest):
        if not artifact.exists() and dry_run:
            continue
        if artifact.exists():
            storage.upload_file(artifact, f"jobs/{month}/{artifact.name}")
        else:
            log.info("Skipping missing artifact: %s", artifact)

    storage.upload_prefix(shards_dir, f"jobs/{month}/shards")
    storage.upload_prefix(nppes_out, f"parquet/{month}/nppes_provider")
    storage.upload_prefix(raw_dir / "payers", f"jobs/{month}/payers")

    for paths in payer_paths:
        storage.upload_prefix(
            paths.bridge_dir,
            f"parquet/{month}/plan_pricing_bridge/{paths.key}",
        )


def run_discovery(args, repo_root: Path, paths: PayerPaths) -> None:
    command = [
        args.python,
        str(repo_root / "ingest" / "discover_index_urls.py"),
        "--output",
        str(paths.index_urls),
        "--manifest",
        str(paths.index_manifest),
    ]

    if paths.key == "uhc":
        if not args.uhc_mrf_seed_url:
            raise ValueError("UHC discovery requires --uhc-mrf-seed-url or TIC_UHC_MRF_SEED_URL.")
        command.extend(["--seed-url", args.uhc_mrf_seed_url, "--month", args.month])
    elif paths.key == "bcbstx":
        if not args.bcbstx_index_url:
            raise ValueError("BCBSTX discovery requires --bcbstx-index-url or TIC_BCBSTX_INDEX_URL.")
        command.extend(["--index-url", args.bcbstx_index_url])
    elif paths.key == "cigna":
        if not args.cigna_index_url:
            raise ValueError("Cigna discovery requires --cigna-index-url or TIC_CIGNA_INDEX_URL.")
        command.extend(["--index-url", args.cigna_index_url])
    elif paths.key == "aetna":
        command.extend(
            [
                "--aetna-insurer-code",
                args.aetna_insurer_code,
                "--aetna-brand-code",
                args.aetna_brand_code,
                "--aetna-file-month",
                args.month,
            ]
        )
        if args.aetna_insecure:
            command.append("--insecure")
    else:  # pragma: no cover - normalize_payers prevents this
        raise ValueError(f"Unsupported payer: {paths.key}")

    run(command, dry_run=args.dry_run)


def run_stream_index(args, repo_root: Path, paths: PayerPaths) -> None:
    run(
        [
            args.python,
            str(repo_root / "ingest" / "stream_index.py"),
            "--url-list",
            str(paths.index_urls),
            "--output",
            str(paths.bridge_dir / "plan_pricing_bridge.parquet"),
            "--payer-code",
            paths.payer_code,
            "--file-month",
            args.month,
            "--state",
            args.state,
        ],
        dry_run=args.dry_run,
    )


def run_scan(args, repo_root: Path, nppes_out: Path, paths: PayerPaths) -> None:
    command = [
        args.python,
        str(repo_root / "ingest" / "scan_pricing_urls.py"),
        "--nppes",
        str(nppes_out),
        "--bridge",
        str(paths.bridge_dir),
        "--output",
        str(paths.matched_urls),
        "--npi-only",
        "--workers",
        str(args.scan_workers),
    ]
    if paths.key == "uhc":
        command.extend(
            [
                "--url-contains",
                "uhc",
                "umr",
                "--target-states",
                "Texas",
                "TX",
                "--skip-states",
                *NON_TX_STATES,
            ]
        )
    run(command, dry_run=args.dry_run)


def combine_payer_outputs(payer_paths: list[PayerPaths], matched_urls: Path, manifest: Path) -> None:
    matched_urls.parent.mkdir(parents=True, exist_ok=True)
    with matched_urls.open("w", encoding="utf-8") as urls_fh, manifest.open("w", encoding="utf-8") as manifest_fh:
        for paths in payer_paths:
            if not paths.matched_urls.exists():
                log.info("Skipping missing payer matched URL file: %s", paths.matched_urls)
                continue
            with paths.matched_urls.open(encoding="utf-8") as fh:
                for line in fh:
                    if line.strip():
                        urls_fh.write(line)

            if not paths.matched_manifest.exists():
                log.info("Skipping missing payer manifest file: %s", paths.matched_manifest)
                continue
            with paths.matched_manifest.open(encoding="utf-8") as fh:
                for line in fh:
                    raw = line.rstrip("\n")
                    if raw.strip():
                        manifest_fh.write(f"{raw}|{paths.payer_code}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare a monthly Texas TiC refresh job.")
    parser.add_argument("--month", required=True, help="Month partition, e.g. 2026-04")
    parser.add_argument("--storage-root", default=os.getenv("TIC_STORAGE_ROOT", "data/cloud"))
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument(
        "--payers",
        nargs="+",
        default=os.getenv("TIC_PAYERS", "uhc").split(),
        help=(
            "Payers to run: uhc bcbstx cigna aetna. "
            "Accepts space-separated values or comma-separated groups. Default: uhc."
        ),
    )
    parser.add_argument("--state", default=os.getenv("TIC_STATE", "TX"))
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
        "--bcbstx-index-url",
        default=os.getenv("TIC_BCBSTX_INDEX_URL"),
        help="Direct BCBSTX table-of-contents index URL.",
    )
    parser.add_argument(
        "--cigna-index-url",
        default=os.getenv("TIC_CIGNA_INDEX_URL"),
        help="Direct Cigna table-of-contents index URL.",
    )
    parser.add_argument(
        "--aetna-insurer-code",
        default=os.getenv("TIC_AETNA_INSURER_CODE", "AETNACVS_I"),
        help="Aetna/HealthSparq insurer code. Default: AETNACVS_I.",
    )
    parser.add_argument(
        "--aetna-brand-code",
        default=os.getenv("TIC_AETNA_BRAND_CODE", "ALICSI"),
        help="Aetna/HealthSparq brand code. Default: ALICSI.",
    )
    parser.add_argument(
        "--aetna-insecure",
        action="store_true",
        default=os.getenv("TIC_AETNA_INSECURE", "").lower() in {"1", "true", "yes"},
        help="Pass --insecure to Aetna discovery if local TLS cert validation fails.",
    )
    parser.add_argument(
        "--skip-index-discovery",
        action="store_true",
        help="Do not discover index URLs or rebuild payer plan_pricing_bridge files for this month.",
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
    payers = normalize_payers(args.payers)
    log.info("Preparing month=%s for payer(s): %s", args.month, ", ".join(payers))

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
    payer_paths = [make_payer_paths(raw_dir, parquet_dir, payer) for payer in payers]

    if args.upload_only:
        storage = StorageClient(args.storage_root, dry_run=args.dry_run)
        upload_multi_payer_artifacts(
            storage,
            args.month,
            raw_dir,
            parquet_dir,
            matched_urls,
            manifest,
            shards_dir,
            nppes_out,
            payer_paths,
            args.dry_run,
        )
        return

    if not args.skip_index_discovery:
        for paths in payer_paths:
            paths.raw_dir.mkdir(parents=True, exist_ok=True)
            paths.bridge_dir.mkdir(parents=True, exist_ok=True)
            run_discovery(args, repo_root, paths)
            run_stream_index(args, repo_root, paths)

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
        for paths in payer_paths:
            if args.skip_index_discovery and paths.key == "uhc" and len(payer_paths) == 1:
                legacy_bridge = repo_root / args.bridge
                if legacy_bridge.exists() and not paths.bridge_dir.exists():
                    paths.bridge_dir.parent.mkdir(parents=True, exist_ok=True)
                    log.info("Using legacy bridge path for UHC scan: %s", legacy_bridge)
                    paths.bridge_dir = legacy_bridge
            run_scan(args, repo_root, nppes_out, paths)

    if not args.dry_run:
        combine_payer_outputs(payer_paths, matched_urls, manifest)

    if not args.dry_run:
        write_shards(manifest, shards_dir, args.shards)

    storage = StorageClient(args.storage_root, dry_run=args.dry_run)
    upload_multi_payer_artifacts(
        storage,
        args.month,
        raw_dir,
        parquet_dir,
        matched_urls,
        manifest,
        shards_dir,
        nppes_out,
        payer_paths,
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
