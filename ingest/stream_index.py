"""
Read index URLs from a text file, fetch each index JSON over HTTP, and write
plan_pricing_bridge Parquet — no files saved to disk.

Index files themselves are small (a few KB each). The huge files are the
in-network pricing files at the `location` URLs inside them; those are never
touched here.

Usage:
    python ingest/stream_index.py
    python ingest/stream_index.py --url-list index_urls.txt --output-dir data/parquet/plan_pricing_bridge
"""

import argparse
from datetime import datetime
import gzip
import io
import logging
import sys
from pathlib import Path
from typing import Iterator
from urllib.parse import urlparse, unquote
from uuid import uuid4
from zoneinfo import ZoneInfo

import ijson
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.schema import PLAN_PRICING_BRIDGE_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

TIMEOUT = (10, 120)
HEADERS = {"User-Agent": "Mozilla/5.0"}
CENTRAL_TZ = ZoneInfo("America/Chicago")


def _central_now() -> datetime:
    return datetime.now(CENTRAL_TZ)


def _default_file_month() -> str:
    return _central_now().strftime("%Y-%m")


def _make_lineage(
    payer_code: str,
    file_month: str,
    state: str,
    source_version: str,
    etl_run_id: str,
    ingested_at: str,
) -> dict[str, str]:
    return {
        "payer_code": payer_code,
        "file_month": file_month,
        "state": state,
        "source_version": source_version,
        "etl_run_id": etl_run_id,
        "ingested_at": ingested_at,
    }


def _with_lineage(row: dict, lineage: dict[str, str]) -> dict:
    row.update(lineage)
    return row


def _plan_sponsor_name(plan: dict) -> str | None:
    return plan.get("plan_sponsor_name") or plan.get("plan_sponser_name")


def _filename_from_url(url: str) -> str:
    return Path(unquote(urlparse(url).path)).name


def _fetch_bytes(url: str) -> bytes:
    """Fetch a URL and return decompressed bytes. Index files are small so buffering is fine.

    Gzip detection uses magic bytes (\x1f\x8b) rather than headers or URL extension:
    - UHC serves plain JSON with no compression
    - BCBS TX serves raw .gz files without Content-Encoding: gzip, so requests
      won't auto-decompress and header/extension checks are unreliable
    """
    with requests.get(url, stream=True, timeout=TIMEOUT, headers=HEADERS) as r:
        r.raise_for_status()
        raw = r.content

    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)

    return raw


def _get_metadata(data: bytes) -> dict:
    """First pass: pull top-level scalars from the index document."""
    meta = {}
    for prefix, event, value in ijson.parse(io.BytesIO(data)):
        if prefix == "reporting_entity_name" and event in ("string", "null"):
            meta["reporting_entity_name"] = value
        elif prefix == "reporting_entity_type" and event in ("string", "null"):
            meta["reporting_entity_type"] = value
        elif prefix == "last_updated_on" and event in ("string", "number", "null"):
            meta["last_updated_on"] = "" if value is None else str(value)
        elif prefix == "version" and event in ("string", "number", "null"):
            meta["version"] = "" if value is None else str(value)
    return meta


def _stream_rows(data: bytes, meta: dict, source_name: str, lineage: dict[str, str]) -> Iterator[dict]:
    """Second pass: stream reporting_structure, yield one row per plan × in_network_file."""
    entity_name = meta.get("reporting_entity_name")
    entity_type = meta.get("reporting_entity_type")
    last_updated_on = meta.get("last_updated_on")

    for structure in ijson.items(io.BytesIO(data), "reporting_structure.item"):
        plans = structure.get("reporting_plans") or []
        files = structure.get("in_network_files") or []

        for plan in plans:
            for nf in files:
                location = nf.get("location")
                if not location:
                    continue
                plan_sponsor_name = _plan_sponsor_name(plan)
                yield _with_lineage(
                    {
                        "reporting_entity_name": entity_name,
                        "reporting_entity_type": entity_type,
                        "last_updated_on": last_updated_on,
                        "plan_name": plan.get("plan_name"),
                        "plan_id": plan.get("plan_id"),
                        "plan_id_type": plan.get("plan_id_type"),
                        "plan_market_type": plan.get("plan_market_type"),
                        "plan_sponsor_name": plan_sponsor_name,
                        "plan_sponser_name": plan_sponsor_name,
                        "issuer_name": plan.get("issuer_name"),
                        "description": nf.get("description"),
                        "location": location,
                        "source_index_file": source_name,
                    },
                    lineage,
                )


NON_INDEX_PATTERNS = ("in-network", "in_network", "allowed-amount", "allowed_amount")

DONE_FILE = "processed_urls.txt"   # tracks which URLs have been written


def _is_index_url(url: str) -> bool:
    """Return False for URLs that are clearly pricing or allowed-amounts files, not index files."""
    name = _filename_from_url(url).lower()
    return not any(p in name for p in NON_INDEX_PATTERNS)


def _load_done(done_path: Path) -> set:
    if not done_path.exists():
        return set()
    return {line.strip() for line in done_path.read_text(encoding="utf-8").splitlines() if line.strip()}


def process_url(
    url: str,
    writer: pq.ParquetWriter,
    done_fh,
    lineage: dict[str, str],
    batch_size: int = 10_000,
) -> int:
    """Fetch one index URL, append rows to the shared writer. Returns row count."""
    source_name = _filename_from_url(url)

    if not _is_index_url(url):
        logging.debug("Skipping non-index file: %s", source_name)
        return 0

    data = _fetch_bytes(url)
    meta = _get_metadata(data)
    row_lineage = dict(lineage)
    if not row_lineage["source_version"]:
        row_lineage["source_version"] = meta.get("version", "")

    batch: list[dict] = []
    total = 0

    for row in _stream_rows(data, meta, source_name, row_lineage):
        batch.append(row)
        if len(batch) >= batch_size:
            writer.write_table(pa.Table.from_pylist(batch, schema=PLAN_PRICING_BRIDGE_SCHEMA))
            total += len(batch)
            batch.clear()

    if batch:
        writer.write_table(pa.Table.from_pylist(batch, schema=PLAN_PRICING_BRIDGE_SCHEMA))
        total += len(batch)

    if total == 0:
        logging.warning("%s produced 0 rows (no in_network_files found)", source_name)

    # Mark as done immediately so a resumed run skips it
    done_fh.write(url + "\n")
    done_fh.flush()

    return total


def main():
    parser = argparse.ArgumentParser(
        description="Fetch TiC index URLs → plan_pricing_bridge Parquet (no files saved to disk)"
    )
    parser.add_argument(
        "--url-list",
        default="index_urls.txt",
        help="Line-delimited text file of index JSON URLs",
    )
    parser.add_argument(
        "--output",
        default="data/parquet/plan_pricing_bridge/plan_pricing_bridge.parquet",
        help="Output Parquet file (single file, appended across runs)",
    )
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--payer-code", default="UHC")
    parser.add_argument("--file-month", default=None, help="Source file month as YYYY-MM")
    parser.add_argument("--state", default="TX")
    parser.add_argument(
        "--source-version",
        default="",
        help="Source data/version label. Defaults to the index JSON version when present.",
    )
    parser.add_argument("--etl-run-id", default=None)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after processing this many URLs (useful for testing)",
    )
    args = parser.parse_args()
    ingested_at = _central_now().isoformat(timespec="seconds")
    lineage = _make_lineage(
        payer_code=args.payer_code,
        file_month=args.file_month or _default_file_month(),
        state=args.state,
        source_version=args.source_version,
        etl_run_id=args.etl_run_id or str(uuid4()),
        ingested_at=ingested_at,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    done_path = output_path.with_suffix(".done.txt")

    already_done = _load_done(done_path)
    if already_done:
        logging.info("Resuming — %d URLs already processed", len(already_done))

    total_urls = total_rows = errors = 0

    with (
        pq.ParquetWriter(str(output_path), schema=PLAN_PRICING_BRIDGE_SCHEMA) as writer,
        open(done_path, "a", encoding="utf-8") as done_fh,
        open(args.url_list, encoding="utf-8") as url_fh,
    ):
        for line in url_fh:
            url = line.strip()
            if not url or url in already_done:
                continue

            total_urls += 1
            try:
                count = process_url(url, writer, done_fh, lineage, args.batch_size)
                total_rows += count
                if total_urls % 100 == 0:
                    logging.info(
                        "Progress | urls=%d | rows=%d | errors=%d",
                        total_urls, total_rows, errors,
                    )
            except Exception:
                errors += 1
                logging.exception("Failed: %s", url)

            if args.limit and total_urls >= args.limit:
                logging.info("Reached --limit %d, stopping early", args.limit)
                break

    logging.info(
        "Done | urls=%d | total_rows=%d | errors=%d",
        total_urls, total_rows, errors,
    )


if __name__ == "__main__":
    main()
