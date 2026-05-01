"""
stream_pricing.py

Streams TiC in-network pricing files from matched_pricing_urls.txt and writes
three Parquet datasets:

  - tic_provider_reference
  - tic_price
  - tic_out_of_network_allowed

This is the streaming replacement for json_to_csv.py. It uses ijson to avoid
loading full files into memory, and writes Parquet via PyArrow.

Streaming strategy per file
---------------------------
provider_references and in_network are both top-level keys in the JSON.
ijson can only stream one prefix at a time, so the main data extraction uses
two passes:

  Pass 1: stream provider_references -> tic_provider_reference Parquet
  Pass 2: stream in_network + out_of_network -> tic_price + tic_oon Parquet

To populate shared header metadata, the script also performs one lightweight
top-level scalar read before those passes. For remote URLs that means three
HTTP requests total; for local files, three sequential reads from disk.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import gzip
import logging
import re
import sys
from contextlib import contextmanager
import json
from pathlib import Path
import queue as _queue
import threading
import time
from urllib.parse import urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo

import ijson
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.schema import (  # noqa: E402
    TIC_OUT_OF_NETWORK_ALLOWED_SCHEMA,
    TIC_PRICE_SCHEMA,
    TIC_PROVIDER_REFERENCE_SCHEMA,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BATCH_SIZE = 10_000
STREAM_TIMEOUT = (15, 120)
PROGRESS_EVERY_PROVIDER_REFS = 5_000
PROGRESS_EVERY_IN_NETWORK_ITEMS = 2_000
PROGRESS_EVERY_OON_ITEMS = 2_000
HEARTBEAT_INTERVAL_SECONDS = 30
TEXT_PREGATE_CHUNK_SIZE = 8 * 1024 * 1024
TEXT_PREGATE_OVERLAP_BYTES = 64

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, */*",
}
CENTRAL_TZ = ZoneInfo("America/Chicago")

DEFAULT_CODE_PREFIXES = (
    "610", "611", "612", "613", "614", "615",
    "616", "617", "618", "619",
    "620", "621", "622", "623", "624", "625",
    "626", "627", "628", "629",
    "630", "631", "632", "633",
)


def normalize_list(value) -> str:
    """Convert a list to a pipe-delimited string with no spaces."""
    if value is None:
        return ""
    if isinstance(value, list):
        return "|".join(str(v) for v in value if v not in (None, ""))
    return str(value)


def central_now() -> datetime:
    return datetime.now(CENTRAL_TZ)


def default_file_month() -> str:
    return central_now().strftime("%Y-%m")


def make_lineage_context(
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


def with_lineage(row: dict, lineage: dict[str, str]) -> dict:
    row.update(lineage)
    return row


def lineage_with_source_version(lineage: dict[str, str], source_version: str | None) -> dict[str, str]:
    row_lineage = dict(lineage)
    if not row_lineage["source_version"]:
        row_lineage["source_version"] = source_version or ""
    return row_lineage


def safe_float(value) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def safe_int(value) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def source_name_from_url(url: str) -> str:
    """Create a stable parquet stem from host + filename, ignoring query params."""
    parsed = urlparse(url)
    host = re.sub(r"[^\w\-]", "_", parsed.netloc or "local")
    name = Path(parsed.path).name or "unknown"
    name = re.sub(r"\.json(\.gz)?$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[^\w\-]", "_", name)
    return f"{host}__{name}" if host else name


def normalize_source_url(url: str) -> str:
    """
    Normalize common malformed URL schemes from hand-edited or previously
    generated match lists.
    """
    trimmed = url.strip()
    lowered = trimmed.lower()
    if lowered.startswith("tps://"):
        repaired = "ht" + trimmed
        log.warning("Repairing malformed URL scheme: %s -> %s", trimmed[:40], repaired[:40])
        return repaired
    if lowered.startswith("ttp://"):
        repaired = "h" + trimmed
        log.warning("Repairing malformed URL scheme: %s -> %s", trimmed[:40], repaired[:40])
        return repaired
    return trimmed


def is_supported_source(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} or Path(url).exists()


def load_target_npis(nppes_dir: Path) -> set[str]:
    parquet_glob = (nppes_dir / "*.parquet").as_posix()
    con = duckdb.connect()
    try:
        result = con.execute(
            f"SELECT DISTINCT npi FROM read_parquet('{parquet_glob}') WHERE npi IS NOT NULL"
        ).fetchall()
    finally:
        con.close()
    return {str(row[0]).strip() for row in result if row[0]}


def compile_target_npi_bytes(target_npis: set[str] | None) -> list[bytes]:
    if not target_npis:
        return []
    return [npi.encode("ascii") for npi in sorted(target_npis)]


def normalize_code_prefixes(code_prefixes: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if not code_prefixes:
        return ()
    return tuple(str(prefix).strip() for prefix in code_prefixes if str(prefix).strip())


def billing_code_matches(billing_code: str, code_prefixes: tuple[str, ...]) -> bool:
    if not code_prefixes:
        return True
    code = str(billing_code or "").strip()
    return any(code.startswith(prefix) for prefix in code_prefixes)


def provider_refs_intersect(provider_references, allowed_group_ids: set[int]) -> bool:
    if not allowed_group_ids:
        return False
    for ref in provider_references or []:
        ref_int = safe_int(ref)
        if ref_int is not None and ref_int in allowed_group_ids:
            return True
    return False


def download_to_local(url: str, download_dir: Path, source_name: str) -> Path:
    """
    Download a remote pricing file once so subsequent parsing passes run locally.
    The downloaded file is intended to be temporary and deleted after parsing.
    """
    parsed = urlparse(url)
    suffixes = Path(parsed.path).suffixes
    suffix = "".join(suffixes[-2:]) if suffixes[-2:] == [".json", ".gz"] else "".join(suffixes[-1:])
    if not suffix:
        suffix = ".json"

    download_dir.mkdir(parents=True, exist_ok=True)
    destination = download_dir / f"{source_name}{suffix}"
    temp_destination = download_dir / f"{source_name}.partial"

    if destination.exists():
        log.info("  Reusing downloaded file: %s", destination)
        return destination

    if temp_destination.exists():
        temp_destination.unlink()

    log.info("  Downloading to local staging: %s", destination)
    with requests.get(url, stream=True, timeout=STREAM_TIMEOUT, headers=HEADERS) as resp:
        resp.raise_for_status()
        with temp_destination.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)

    temp_destination.replace(destination)
    return destination


def iter_decompressed_chunks(path: Path, chunk_size: int = TEXT_PREGATE_CHUNK_SIZE):
    overlap = b""
    if path.suffix.lower() == ".gz" or path.name.lower().endswith(".json.gz"):
        fh = gzip.open(path, "rb")
    else:
        fh = path.open("rb")

    with fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            combined = overlap + chunk
            yield combined
            overlap = combined[-TEXT_PREGATE_OVERLAP_BYTES:]


def pregate_file_for_npis(path: Path, target_npi_bytes: list[bytes]) -> tuple[bool, str | None]:
    if not target_npi_bytes:
        return True, None
    for chunk in iter_decompressed_chunks(path):
        for npi in target_npi_bytes:
            if npi in chunk:
                return True, npi.decode("ascii")
    return False, None


def get_pass_state_path(output_dir: Path, source_name: str) -> Path:
    return output_dir / "_pass_state" / f"{source_name}.json"


def get_done_marker_path(output_dir: Path, source_name: str, pass_name: str) -> Path:
    return output_dir / "_done" / f"{source_name}.{pass_name}.done"


def mark_pass_done(output_dir: Path, source_name: str, pass_name: str):
    done_path = get_done_marker_path(output_dir, source_name, pass_name)
    done_path.parent.mkdir(parents=True, exist_ok=True)
    done_path.write_text("done\n", encoding="utf-8")


def is_pass_done(output_dir: Path, source_name: str, pass_name: str) -> bool:
    return get_done_marker_path(output_dir, source_name, pass_name).exists()


def write_pass_state(
    output_dir: Path,
    source_name: str,
    source_url: str,
    matched_group_ids: set[int],
    provider_rows: int,
):
    state_path = get_pass_state_path(output_dir, source_name)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_name": source_name,
        "source_url": source_url,
        "provider_rows": provider_rows,
        "matched_group_ids": sorted(matched_group_ids),
    }
    state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def read_pass_state(output_dir: Path, source_name: str) -> dict | None:
    state_path = get_pass_state_path(output_dir, source_name)
    if not state_path.exists():
        return None
    return json.loads(state_path.read_text(encoding="utf-8"))


class _ByteCounter:
    """Wraps a raw binary file and counts compressed bytes consumed (for gz progress)."""
    __slots__ = ("_fh", "bytes_read")

    def __init__(self, fh):
        self._fh = fh
        self.bytes_read = 0

    def read(self, n=-1):
        data = self._fh.read(n)
        self.bytes_read += len(data)
        return data

    def readable(self):
        return True

    def close(self):
        self._fh.close()

    @property
    def closed(self):
        return self._fh.closed


class HeartbeatLogger:
    """Emit periodic 'still running' logs while a long streaming loop is active."""

    def __init__(self, message_factory, interval_seconds: int = HEARTBEAT_INTERVAL_SECONDS):
        self._message_factory = message_factory
        self._interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        def _run():
            while not self._stop_event.wait(self._interval_seconds):
                try:
                    log.info(self._message_factory())
                except Exception as exc:  # pragma: no cover
                    log.warning("Heartbeat logging failed: %s", exc)

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=1)


class _PrefixedStream:
    """Glue a small peek buffer back onto a live stream."""

    def __init__(self, prefix: bytes, stream):
        self._prefix = prefix
        self._pos = 0
        self._stream = stream

    def read(self, n=-1):
        if self._pos < len(self._prefix):
            head = self._prefix[self._pos :]
            if n == -1:
                self._pos = len(self._prefix)
                return head + self._stream.read()
            if n <= len(head):
                self._pos += n
                return head[:n]
            self._pos = len(self._prefix)
            return head + self._stream.read(n - len(head))
        return self._stream.read(n)

    def readable(self):
        return True


def _wrap_stream(raw_stream, decode_content: bool):
    if decode_content and hasattr(raw_stream, "decode_content"):
        raw_stream.decode_content = True
    peek = raw_stream.read(2)
    stream = _PrefixedStream(peek, raw_stream)
    if peek == b"\x1f\x8b":
        return gzip.open(stream, "rb")
    return stream


@contextmanager
def open_stream(url: str):
    """
    Open a streaming binary file-like object for a URL or local path.
    Handles raw gzip via magic bytes, matching scan_pricing_urls.py.
    """
    if url.startswith(("http://", "https://")):
        with requests.get(url, stream=True, timeout=STREAM_TIMEOUT, headers=HEADERS) as resp:
            resp.raise_for_status()
            stream = _wrap_stream(resp.raw, decode_content=True)
            try:
                yield stream
            finally:
                stream.close()
    else:
        with open(url, "rb") as fh:
            stream = _wrap_stream(fh, decode_content=False)
            try:
                yield stream
            finally:
                stream.close()


def write_batch(writer_ref: list, batch: list, schema: pa.Schema, output_path: Path):
    """Write a batch to a ParquetWriter, creating it if needed."""
    if not batch:
        return
    t0 = time.time()
    table = pa.Table.from_pylist(batch, schema=schema)
    is_new = writer_ref[0] is None
    if is_new:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        writer_ref[0] = pq.ParquetWriter(str(output_path), schema=schema)
    writer_ref[0].write_table(table)
    elapsed = time.time() - t0
    if elapsed > 2.0 or is_new:
        log.info(
            "  Parquet write: %s rows → %s (%.1fs%s)",
            f"{len(batch):,}",
            output_path.name,
            elapsed,
            " [first write]" if is_new else "",
        )


def flush_batch_if_needed(
    writer_ref: list,
    batch: list,
    schema: pa.Schema,
    output_path: Path,
    async_writer: AsyncParquetWriter | None = None,
):
    if len(batch) >= BATCH_SIZE:
        if async_writer is not None:
            async_writer.write(batch, schema, output_path)
        else:
            write_batch(writer_ref, batch, schema, output_path)
        batch.clear()


class AsyncParquetWriter:
    """Writes Parquet batches in a background thread so the parser is never blocked by I/O."""

    def __init__(self):
        self._q: _queue.Queue = _queue.Queue()
        self._writers: dict[str, pq.ParquetWriter] = {}
        self._error: BaseException | None = None
        self._rows_written = 0
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._worker, daemon=True, name="parquet-writer")
        self._thread.start()

    def write(self, batch: list, schema: pa.Schema, output_path: Path) -> None:
        """Enqueue a batch for async writing. Returns immediately."""
        if not batch:
            return
        self._q.put((list(batch), schema, output_path))

    def _worker(self):
        while True:
            item = self._q.get()
            if item is None:
                self._q.task_done()
                break
            batch, schema, output_path = item
            try:
                t0 = time.time()
                table = pa.Table.from_pylist(batch, schema=schema)
                key = str(output_path)
                is_new = key not in self._writers
                if is_new:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    self._writers[key] = pq.ParquetWriter(str(output_path), schema=schema)
                self._writers[key].write_table(table)
                elapsed = time.time() - t0
                with self._lock:
                    self._rows_written += len(batch)
                if elapsed > 2.0 or is_new:
                    log.info(
                        "  Parquet write: %s rows → %s (%.1fs%s)",
                        f"{len(batch):,}", output_path.name, elapsed,
                        " [first write]" if is_new else "",
                    )
            except Exception as exc:
                self._error = exc
                log.error("Parquet write error: %s", exc, exc_info=True)
            finally:
                self._q.task_done()

    def flush(self):
        """Block until all queued writes have completed."""
        self._q.join()
        if self._error:
            raise self._error

    def close(self) -> int:
        """Flush all pending writes, close all Parquet files, return total rows written."""
        self._q.put(None)
        self._thread.join()
        for writer in self._writers.values():
            try:
                writer.close()
            except Exception:
                pass
        if self._error:
            raise self._error
        return self._rows_written


def get_header_fields(url: str) -> dict[str, str]:
    """Read top-level scalar metadata before the first array begins."""
    header = {}
    with open_stream(url) as stream:
        for prefix, event, value in ijson.parse(stream):
            if prefix in (
                "reporting_entity_name",
                "reporting_entity_type",
                "last_updated_on",
                "version",
            ):
                header[prefix] = value or ""
            if event == "start_array":
                break
    return header


def emit_provider_reference_rows(
    source_pricing_file: str,
    provider_group_id: int | None,
    network_name_list: list[str],
    provider_groups: list[dict],
    writer_ref: list,
    batch: list,
    output_path: Path,
    target_npis: set[str] | None,
    filtered_only: bool,
    keep_full_matched_provider_group: bool,
    lineage: dict[str, str],
    async_writer: AsyncParquetWriter | None = None,
) -> tuple[int, set[int]]:
    total = 0
    matched_group_ids: set[int] = set()
    network_name = normalize_list(network_name_list)

    if not provider_groups:
        if not filtered_only:
            batch.append(
                with_lineage(
                    {
                    "source_pricing_file": source_pricing_file,
                    "provider_group_id": provider_group_id,
                    "network_name": network_name,
                    "tin_type": "",
                    "tin_value": "",
                    "npi": "",
                    "business_name": "",
                    },
                    lineage,
                )
            )
            total += 1
            flush_batch_if_needed(
                writer_ref,
                batch,
                TIC_PROVIDER_REFERENCE_SCHEMA,
                output_path,
                async_writer=async_writer,
            )
        return total, matched_group_ids

    item_has_target_npi = False
    if target_npis is not None:
        for group in provider_groups:
            for npi in group.get("npis", []):
                if npi in target_npis:
                    item_has_target_npi = True
                    break
            if item_has_target_npi:
                break

    for group in provider_groups:
        npis = group.get("npis", [])
        matched_npis = []
        if target_npis is not None:
            matched_npis = [npi for npi in npis if npi in target_npis]
            if matched_npis and provider_group_id is not None:
                matched_group_ids.add(provider_group_id)
        else:
            matched_npis = list(npis)

        if (
            filtered_only
            and target_npis is not None
            and not matched_npis
            and not (keep_full_matched_provider_group and item_has_target_npi)
        ):
            continue

        if not npis:
            if not filtered_only:
                batch.append(
                    with_lineage(
                        {
                        "source_pricing_file": source_pricing_file,
                        "provider_group_id": provider_group_id,
                        "network_name": network_name,
                        "tin_type": group.get("tin_type", ""),
                        "tin_value": group.get("tin_value", ""),
                        "npi": "",
                        "business_name": group.get("business_name", ""),
                        },
                        lineage,
                    )
                )
                total += 1
        else:
            if (
                filtered_only
                and target_npis is not None
                and keep_full_matched_provider_group
                and item_has_target_npi
            ):
                npi_values = npis
            else:
                npi_values = matched_npis if filtered_only and target_npis is not None else npis

            for npi in npi_values:
                batch.append(
                    with_lineage(
                        {
                        "source_pricing_file": source_pricing_file,
                        "provider_group_id": provider_group_id,
                        "network_name": network_name,
                        "tin_type": group.get("tin_type", ""),
                        "tin_value": group.get("tin_value", ""),
                        "npi": npi,
                        "business_name": group.get("business_name", ""),
                        },
                        lineage,
                    )
                )
                total += 1

        flush_batch_if_needed(
            writer_ref,
            batch,
            TIC_PROVIDER_REFERENCE_SCHEMA,
            output_path,
            async_writer=async_writer,
        )

    return total, matched_group_ids


def stream_provider_references_targeted(
    url: str,
    source_pricing_file: str,
    output_path: Path,
    target_npis: set[str] | None,
    keep_full_matched_provider_group: bool,
    lineage: dict[str, str],
) -> tuple[int, set[int]]:
    """
    Event-based provider_references parser that keeps only minimal state for the
    current provider_references.item and emits rows only for matched items.
    """
    writer_ref = [None]
    batch = []
    total = 0
    matched_group_ids: set[int] = set()
    refs_scanned = 0
    started_at = time.time()

    current_provider_group_id: int | None = None
    current_network_names: list[str] = []
    current_provider_groups: list[dict] = []
    current_group: dict | None = None

    def provider_heartbeat_message():
        elapsed = time.time() - started_at
        return (
            "    Provider heartbeat: elapsed_sec="
            f"{elapsed:,.0f} refs_scanned={refs_scanned:,} "
            f"rows_written={total:,} matched_groups={len(matched_group_ids):,}"
        )

    heartbeat = HeartbeatLogger(provider_heartbeat_message).start()

    try:
        with open_stream(url) as stream:
            for prefix, event, value in ijson.parse(stream):
                if prefix == "provider_references.item" and event == "start_map":
                    current_provider_group_id = None
                    current_network_names = []
                    current_provider_groups = []
                    current_group = None
                    continue

                if prefix == "provider_references.item.provider_group_id" and event in (
                    "number",
                    "string",
                    "null",
                ):
                    current_provider_group_id = safe_int(value)
                    continue

                if prefix == "provider_references.item.network_name.item" and event in ("string", "number"):
                    current_network_names.append(str(value))
                    continue

                if prefix == "provider_references.item.provider_groups.item" and event == "start_map":
                    current_group = {
                        "tin_type": "",
                        "tin_value": "",
                        "business_name": "",
                        "npis": [],
                    }
                    continue

                if prefix == "provider_references.item.provider_groups.item.tin.type" and event in ("string", "null"):
                    if current_group is not None:
                        current_group["tin_type"] = value or ""
                    continue

                if prefix == "provider_references.item.provider_groups.item.tin.value" and event in ("string", "number", "null"):
                    if current_group is not None:
                        current_group["tin_value"] = "" if value is None else str(value)
                    continue

                if prefix == "provider_references.item.provider_groups.item.tin.business_name" and event in ("string", "null"):
                    if current_group is not None:
                        current_group["business_name"] = value or ""
                    continue

                if prefix == "provider_references.item.provider_groups.item.npi.item" and event in ("string", "number"):
                    if current_group is not None:
                        current_group["npis"].append(str(value).strip())
                    continue

                if prefix == "provider_references.item.provider_groups.item" and event == "end_map":
                    if current_group is not None:
                        current_provider_groups.append(current_group)
                        current_group = None
                    continue

                if prefix == "provider_references.item" and event == "end_map":
                    refs_scanned += 1
                    emitted_rows, emitted_groups = emit_provider_reference_rows(
                        source_pricing_file=source_pricing_file,
                        provider_group_id=current_provider_group_id,
                        network_name_list=current_network_names,
                        provider_groups=current_provider_groups,
                        writer_ref=writer_ref,
                        batch=batch,
                        output_path=output_path,
                        target_npis=target_npis,
                        filtered_only=True,
                        keep_full_matched_provider_group=keep_full_matched_provider_group,
                        lineage=lineage,
                    )
                    total += emitted_rows
                    matched_group_ids.update(emitted_groups)

                    if refs_scanned % PROGRESS_EVERY_PROVIDER_REFS == 0:
                        log.info(
                            "    Provider progress: refs_scanned=%s rows_written=%s matched_groups=%s",
                            f"{refs_scanned:,}",
                            f"{total:,}",
                            f"{len(matched_group_ids):,}",
                        )
    finally:
        heartbeat.stop()

    if batch:
        write_batch(writer_ref, batch, TIC_PROVIDER_REFERENCE_SCHEMA, output_path)
    if writer_ref[0]:
        writer_ref[0].close()

    return total, matched_group_ids


def stream_provider_references(
    url: str,
    source_pricing_file: str,
    output_path: Path,
    target_npis: set[str] | None = None,
    filtered_only: bool = False,
    keep_full_matched_provider_group: bool = False,
    lineage: dict[str, str] | None = None,
) -> tuple[int, set[int]]:
    """Stream provider_references rows to Parquet and return matching provider group ids."""
    lineage = lineage or make_lineage_context("", "", "", "", "", "")
    if filtered_only and target_npis is not None:
        return stream_provider_references_targeted(
            url=url,
            source_pricing_file=source_pricing_file,
            output_path=output_path,
            target_npis=target_npis,
            keep_full_matched_provider_group=keep_full_matched_provider_group,
            lineage=lineage,
        )

    writer_ref = [None]
    batch = []
    total = 0
    matched_group_ids: set[int] = set()
    refs_scanned = 0
    started_at = time.time()

    def provider_heartbeat_message():
        elapsed = time.time() - started_at
        if filtered_only:
            return (
                "    Provider heartbeat: elapsed_sec="
                f"{elapsed:,.0f} refs_scanned={refs_scanned:,} "
                f"rows_written={total:,} matched_groups={len(matched_group_ids):,}"
            )
        return (
            "    Provider heartbeat: elapsed_sec="
            f"{elapsed:,.0f} refs_scanned={refs_scanned:,} rows_written={total:,}"
        )

    heartbeat = HeartbeatLogger(provider_heartbeat_message).start()

    try:
        with open_stream(url) as stream:
            for ref in ijson.items(stream, "provider_references.item"):
                refs_scanned += 1
                provider_group_id = safe_int(ref.get("provider_group_id"))
                network_name = normalize_list(ref.get("network_name", ""))
                provider_groups = ref.get("provider_groups", []) or []

                if not provider_groups:
                    if not filtered_only:
                        batch.append(
                            with_lineage(
                                {
                                "source_pricing_file": source_pricing_file,
                                "provider_group_id": provider_group_id,
                                "network_name": network_name,
                                "tin_type": "",
                                "tin_value": "",
                                "npi": "",
                                "business_name": "",
                                },
                                lineage,
                            )
                        )
                        total += 1
                        flush_batch_if_needed(
                            writer_ref,
                            batch,
                            TIC_PROVIDER_REFERENCE_SCHEMA,
                            output_path,
                        )
                else:
                    item_has_target_npi = False
                    if target_npis is not None:
                        for group in provider_groups:
                            for npi in group.get("npi", []) or []:
                                if str(npi).strip() in target_npis:
                                    item_has_target_npi = True
                                    break
                            if item_has_target_npi:
                                break

                    for group in provider_groups:
                        tin = group.get("tin", {}) or {}
                        tin_type = tin.get("type", "")
                        tin_value = tin.get("value", "")
                        business_name = tin.get("business_name", "")
                        npis = group.get("npi", []) or []
                        matched_npis = []
                        if target_npis is not None:
                            matched_npis = [
                                str(npi).strip()
                                for npi in npis
                                if str(npi).strip() in target_npis
                            ]
                            if matched_npis and provider_group_id is not None:
                                matched_group_ids.add(provider_group_id)
                        else:
                            matched_npis = [str(npi).strip() for npi in npis]

                        if (
                            filtered_only
                            and target_npis is not None
                            and not matched_npis
                            and not (
                                keep_full_matched_provider_group
                                and item_has_target_npi
                            )
                        ):
                            continue

                        if not npis:
                            if not filtered_only:
                                batch.append(
                                    with_lineage(
                                        {
                                        "source_pricing_file": source_pricing_file,
                                        "provider_group_id": provider_group_id,
                                        "network_name": network_name,
                                        "tin_type": tin_type,
                                        "tin_value": tin_value,
                                        "npi": "",
                                        "business_name": business_name,
                                        },
                                        lineage,
                                    )
                                )
                                total += 1
                        else:
                            npi_values = (
                                [str(npi).strip() for npi in npis]
                                if (
                                    filtered_only
                                    and target_npis is not None
                                    and keep_full_matched_provider_group
                                    and item_has_target_npi
                                )
                                else (
                                    matched_npis
                                    if filtered_only and target_npis is not None
                                    else [str(npi).strip() for npi in npis]
                                )
                            )
                            for npi in npi_values:
                                batch.append(
                                    with_lineage(
                                        {
                                        "source_pricing_file": source_pricing_file,
                                        "provider_group_id": provider_group_id,
                                        "network_name": network_name,
                                        "tin_type": tin_type,
                                        "tin_value": tin_value,
                                        "npi": str(npi),
                                        "business_name": business_name,
                                        },
                                        lineage,
                                    )
                                )
                                total += 1
                        flush_batch_if_needed(
                            writer_ref,
                            batch,
                            TIC_PROVIDER_REFERENCE_SCHEMA,
                            output_path,
                        )

                if refs_scanned % PROGRESS_EVERY_PROVIDER_REFS == 0:
                    if filtered_only:
                        log.info(
                            "    Provider progress: refs_scanned=%s rows_written=%s matched_groups=%s",
                            f"{refs_scanned:,}",
                            f"{total:,}",
                            f"{len(matched_group_ids):,}",
                        )
                    else:
                        log.info(
                            "    Provider progress: refs_scanned=%s rows_written=%s",
                            f"{refs_scanned:,}",
                            f"{total:,}",
                        )
    finally:
        heartbeat.stop()

    if batch:
        write_batch(writer_ref, batch, TIC_PROVIDER_REFERENCE_SCHEMA, output_path)
    if writer_ref[0]:
        writer_ref[0].close()

    return total, matched_group_ids


def stream_in_network(
    url: str,
    source_pricing_file: str,
    output_path: Path,
    header: dict[str, str],
    allowed_group_ids: set[int] | None = None,
    code_prefixes: tuple[str, ...] = (),
    filtered_only: bool = False,
    lineage: dict[str, str] | None = None,
) -> int:
    """Stream in_network rows to Parquet."""
    lineage = lineage or make_lineage_context("", "", "", "", "", "")
    writer_ref = [None]
    batch = []
    total = 0
    items_scanned = 0
    started_at = time.time()

    def price_heartbeat_message():
        elapsed = time.time() - started_at
        return (
            "    Price heartbeat: elapsed_sec="
            f"{elapsed:,.0f} items_scanned={items_scanned:,} rows_written={total:,}"
        )

    heartbeat = HeartbeatLogger(price_heartbeat_message).start()

    try:
        with open_stream(url) as stream:
            for item in ijson.items(stream, "in_network.item"):
                items_scanned += 1
                item_base = {
                    "source_pricing_file": source_pricing_file,
                    "reporting_entity_name": header.get("reporting_entity_name", ""),
                    "reporting_entity_type": header.get("reporting_entity_type", ""),
                    "last_updated_on": header.get("last_updated_on", ""),
                    "version": header.get("version", ""),
                    "billing_code": item.get("billing_code", "") or "",
                    "billing_code_type": item.get("billing_code_type", "") or "",
                    "billing_code_type_version": item.get("billing_code_type_version", "") or "",
                    "name": item.get("name", "") or "",
                    "description": item.get("description", "") or "",
                    "negotiation_arrangement": item.get("negotiation_arrangement", "") or "",
                    "severity_of_illness": item.get("severity_of_illness", "") or "",
                    **lineage,
                }

                if filtered_only and not billing_code_matches(item_base["billing_code"], code_prefixes):
                    continue

                negotiated_rates = item.get("negotiated_rates", []) or []
                if not negotiated_rates:
                    if not filtered_only:
                        batch.append(
                            {
                                **item_base,
                                "provider_references": "",
                                "negotiated_type": "",
                                "negotiated_rate": None,
                                "expiration_date": "",
                                "billing_class": "",
                                "service_code": "",
                                "billing_code_modifier": "",
                                "additional_information": "",
                                "estimated_amount": None,
                                "setting": "",
                            }
                        )
                        total += 1
                        flush_batch_if_needed(writer_ref, batch, TIC_PRICE_SCHEMA, output_path)
                else:
                    for rate in negotiated_rates:
                        provider_refs_raw = rate.get("provider_references", []) or []
                        if filtered_only and not provider_refs_intersect(
                            provider_refs_raw,
                            allowed_group_ids or set(),
                        ):
                            continue

                        provider_refs = normalize_list(provider_refs_raw)
                        negotiated_prices = rate.get("negotiated_prices", []) or []

                        if not negotiated_prices:
                            batch.append(
                                {
                                    **item_base,
                                    "provider_references": provider_refs,
                                    "negotiated_type": "",
                                    "negotiated_rate": None,
                                    "expiration_date": "",
                                    "billing_class": "",
                                    "service_code": "",
                                    "billing_code_modifier": "",
                                    "additional_information": "",
                                    "estimated_amount": None,
                                    "setting": "",
                                }
                            )
                            total += 1
                        else:
                            for price in negotiated_prices:
                                batch.append(
                                    {
                                        **item_base,
                                        "provider_references": provider_refs,
                                        "negotiated_type": price.get("negotiated_type", "") or "",
                                        "negotiated_rate": safe_float(price.get("negotiated_rate")),
                                        "expiration_date": price.get("expiration_date", "") or "",
                                        "billing_class": price.get("billing_class", "") or "",
                                        "service_code": normalize_list(price.get("service_code", [])),
                                        "billing_code_modifier": normalize_list(
                                            price.get("billing_code_modifier", [])
                                        ),
                                        "additional_information": price.get(
                                            "additional_information", ""
                                        )
                                        or "",
                                        "estimated_amount": safe_float(
                                            price.get("estimated_amount")
                                        ),
                                        "setting": price.get("setting", "") or "",
                                    }
                                )
                                total += 1
                        flush_batch_if_needed(writer_ref, batch, TIC_PRICE_SCHEMA, output_path)

                if items_scanned % PROGRESS_EVERY_IN_NETWORK_ITEMS == 0:
                    log.info(
                        "    Price progress: items_scanned=%s rows_written=%s",
                        f"{items_scanned:,}",
                        f"{total:,}",
                    )
    finally:
        heartbeat.stop()

    if batch:
        write_batch(writer_ref, batch, TIC_PRICE_SCHEMA, output_path)
    if writer_ref[0]:
        writer_ref[0].close()

    return total


def stream_out_of_network(
    url: str,
    source_pricing_file: str,
    output_path: Path,
    header: dict[str, str],
) -> int:
    """Stream out_of_network allowed-amount rows to Parquet."""
    writer_ref = [None]
    batch = []
    total = 0
    items_scanned = 0

    with open_stream(url) as stream:
        for item in ijson.items(stream, "out_of_network.item"):
            items_scanned += 1
            item_base = {
                "source_pricing_file": source_pricing_file,
                "reporting_entity_name": header.get("reporting_entity_name", ""),
                "reporting_entity_type": header.get("reporting_entity_type", ""),
                "last_updated_on": header.get("last_updated_on", ""),
                "version": header.get("version", ""),
                "billing_code": item.get("billing_code", "") or "",
                "billing_code_type": item.get("billing_code_type", "") or "",
                "billing_code_type_version": item.get("billing_code_type_version", "") or "",
                "name": item.get("name", "") or "",
                "description": item.get("description", "") or "",
            }

            for allowed_amount in item.get("allowed_amounts", []) or []:
                tin = allowed_amount.get("tin", {}) or {}
                base_row = {
                    **item_base,
                    "tin_type": tin.get("type", "") or "",
                    "tin_value": tin.get("value", "") or "",
                    "service_code": normalize_list(allowed_amount.get("service_code", [])),
                    "billing_class": allowed_amount.get("billing_class", "") or "",
                }

                payments = allowed_amount.get("payments", []) or []
                if not payments:
                    batch.append(
                        {
                            **base_row,
                            "allowed_amount": None,
                            "billed_charge": None,
                            "npi": "",
                        }
                    )
                    total += 1
                    flush_batch_if_needed(
                        writer_ref,
                        batch,
                        TIC_OUT_OF_NETWORK_ALLOWED_SCHEMA,
                        output_path,
                    )
                    continue

                for payment in payments:
                    providers = payment.get("providers", []) or []
                    payment_base = {
                        **base_row,
                        "allowed_amount": safe_float(payment.get("allowed_amount")),
                    }

                    if not providers:
                        batch.append(
                            {
                                **payment_base,
                                "billed_charge": None,
                                "npi": "",
                            }
                        )
                        total += 1
                    else:
                        for provider in providers:
                            batch.append(
                                {
                                    **payment_base,
                                    "billed_charge": safe_float(
                                        provider.get("billed_charge")
                                    ),
                                    "npi": normalize_list(provider.get("npi", [])),
                                }
                            )
                            total += 1

                    flush_batch_if_needed(
                        writer_ref,
                        batch,
                        TIC_OUT_OF_NETWORK_ALLOWED_SCHEMA,
                        output_path,
                    )

            if items_scanned % PROGRESS_EVERY_OON_ITEMS == 0:
                log.info(
                    "    OON progress: items_scanned=%s rows_written=%s",
                    f"{items_scanned:,}",
                    f"{total:,}",
                )

    if batch:
        write_batch(
            writer_ref,
            batch,
            TIC_OUT_OF_NETWORK_ALLOWED_SCHEMA,
            output_path,
        )
    if writer_ref[0]:
        writer_ref[0].close()

    return total


def stream_single_pass(
    url: str,
    source_pricing_file: str,
    provider_ref_output: Path,
    price_output: Path,
    target_npis: set[str] | None,
    filtered_only: bool,
    code_prefixes: tuple[str, ...],
    keep_full_matched_provider_group: bool,
    lineage: dict[str, str],
) -> tuple[int, set[int], int, dict[str, str]]:
    """
    Single-pass streaming parser that handles provider_references and in_network
    in one sequential read of the file.

    Requires provider_references to precede in_network in the file (standard TiC
    structure). Matched group IDs collected during the first section are immediately
    available to filter the second section without a second file read.

    Returns: (provider_rows, matched_group_ids, price_rows, header)
    """
    # --- Writers ---
    _async_writer = AsyncParquetWriter()
    pr_writer_ref = [None]   # kept for emit_provider_reference_rows signature compat
    pr_batch: list[dict] = []
    price_writer_ref = [None]
    price_batch: list[dict] = []

    # --- Counters / accumulators ---
    provider_rows = 0
    price_rows = 0
    refs_scanned = 0
    items_scanned = 0
    matched_group_ids: set[int] = set()

    # --- Header (collected from top-level scalars before first array) ---
    header: dict[str, str] = {}
    header_needed = {"reporting_entity_name", "reporting_entity_type", "last_updated_on", "version"}
    row_lineage = dict(lineage)

    # --- Section flags ---
    in_provider_refs = False
    provider_refs_seen = False  # True once provider_references array has started
    in_in_network = False

    # --- Provider-reference item state ---
    current_ref_id: int | None = None
    current_ref_network_names: list[str] = []
    current_ref_groups: list[dict] = []
    current_ref_group: dict | None = None

    # --- In-network item state ---
    in_item = False
    item_skip = False          # True when billing_code filter rejects this item
    item_billing_code = ""
    item_billing_code_type = ""
    item_billing_code_type_version = ""
    item_name = ""
    item_description = ""
    item_negotiation_arrangement = ""
    item_severity_of_illness = ""

    # --- Rate-level state ---
    in_rate = False
    rate_provider_refs: list = []

    # --- Price-level state ---
    in_price = False
    price_negotiated_type = ""
    price_negotiated_rate: float | None = None
    price_expiration_date = ""
    price_billing_class = ""
    price_service_codes: list[str] = []
    price_modifiers: list[str] = []
    price_additional_info = ""
    price_estimated_amount: float | None = None
    price_setting = ""

    started_at = time.time()

    # For local files wrap the raw compressed file in a byte counter so the
    # heartbeat can show compressed-bytes progress (file % complete).
    _counter: _ByteCounter | None = None
    _file_size: int | None = None
    _is_local = not url.startswith(("http://", "https://"))
    if _is_local:
        try:
            _file_size = Path(url).stat().st_size
        except OSError:
            pass

    def heartbeat_msg():
        elapsed = time.time() - started_at
        if _file_size and _counter:
            mb_read = _counter.bytes_read / (1024 * 1024)
            mb_total = _file_size / (1024 * 1024)
            progress = f" | {mb_read:,.0f}/{mb_total:,.0f} MB"
        else:
            progress = ""
        return (
            f"    [single-pass] elapsed={elapsed:,.0f}s{progress} "
            f"refs={refs_scanned:,} matched_groups={len(matched_group_ids):,} "
            f"in_network_items={items_scanned:,} price_rows={price_rows:,}"
        )

    heartbeat = HeartbeatLogger(heartbeat_msg).start()

    @contextmanager
    def _open_counted():
        """Open the URL as an ijson-parseable binary stream with byte counting for local files."""
        nonlocal _counter
        if _is_local and _file_size:
            raw_fh = open(url, "rb")  # noqa: SIM115
            try:
                _counter = _ByteCounter(raw_fh)
                is_gz = url.lower().endswith(".gz")
                inner = gzip.open(_counter, "rb") if is_gz else _counter
                try:
                    yield inner
                finally:
                    inner.close()
            finally:
                raw_fh.close()
        else:
            with open_stream(url) as s:
                yield s

    try:
        with _open_counted() as stream:
            for prefix, event, value in ijson.parse(stream):

                # ── Header scalars (before first array) ──────────────────────
                if header_needed and prefix in header_needed and event in ("string", "number", "null"):
                    header[prefix] = "" if value is None else str(value)
                    if prefix == "version" and not row_lineage["source_version"]:
                        row_lineage["source_version"] = header[prefix]
                    header_needed.discard(prefix)
                    continue

                # ── Section boundaries ────────────────────────────────────────
                if prefix == "provider_references" and event == "start_array":
                    in_provider_refs = True
                    provider_refs_seen = True
                    continue

                if prefix == "provider_references" and event == "end_array":
                    in_provider_refs = False
                    continue

                if prefix == "in_network" and event == "start_array":
                    in_in_network = True
                    if not provider_refs_seen:
                        log.warning(
                            "  in_network appeared before provider_references — "
                            "provider_group_id filtering will be skipped"
                        )
                    continue

                if prefix == "in_network" and event == "end_array":
                    break  # nothing left we need

                # ── Provider-references section ───────────────────────────────
                if in_provider_refs:
                    if prefix == "provider_references.item" and event == "start_map":
                        current_ref_id = None
                        current_ref_network_names = []
                        current_ref_groups = []
                        current_ref_group = None
                        continue

                    if prefix == "provider_references.item.provider_group_id" and event in ("number", "string", "null"):
                        current_ref_id = safe_int(value)
                        continue

                    if prefix == "provider_references.item.network_name.item" and event in ("string", "number"):
                        current_ref_network_names.append(str(value))
                        continue

                    if prefix == "provider_references.item.provider_groups.item" and event == "start_map":
                        current_ref_group = {"tin_type": "", "tin_value": "", "business_name": "", "npis": []}
                        continue

                    if prefix == "provider_references.item.provider_groups.item.tin.type" and event in ("string", "null"):
                        if current_ref_group is not None:
                            current_ref_group["tin_type"] = value or ""
                        continue

                    if prefix == "provider_references.item.provider_groups.item.tin.value" and event in ("string", "number", "null"):
                        if current_ref_group is not None:
                            current_ref_group["tin_value"] = "" if value is None else str(value)
                        continue

                    if prefix == "provider_references.item.provider_groups.item.tin.business_name" and event in ("string", "null"):
                        if current_ref_group is not None:
                            current_ref_group["business_name"] = value or ""
                        continue

                    if prefix == "provider_references.item.provider_groups.item.npi.item" and event in ("string", "number"):
                        if current_ref_group is not None:
                            current_ref_group["npis"].append(str(value).strip())
                        continue

                    if prefix == "provider_references.item.provider_groups.item" and event == "end_map":
                        if current_ref_group is not None:
                            current_ref_groups.append(current_ref_group)
                            current_ref_group = None
                        continue

                    if prefix == "provider_references.item" and event == "end_map":
                        refs_scanned += 1
                        emitted_rows, emitted_groups = emit_provider_reference_rows(
                            source_pricing_file=source_pricing_file,
                            provider_group_id=current_ref_id,
                            network_name_list=current_ref_network_names,
                            provider_groups=current_ref_groups,
                            writer_ref=pr_writer_ref,
                            batch=pr_batch,
                            output_path=provider_ref_output,
                            target_npis=target_npis,
                            filtered_only=filtered_only,
                            keep_full_matched_provider_group=keep_full_matched_provider_group,
                            lineage=row_lineage,
                            async_writer=_async_writer,
                        )
                        provider_rows += emitted_rows
                        matched_group_ids.update(emitted_groups)

                        if refs_scanned % PROGRESS_EVERY_PROVIDER_REFS == 0:
                            log.info(
                                "    [single-pass] refs=%s matched_groups=%s",
                                f"{refs_scanned:,}", f"{len(matched_group_ids):,}",
                            )
                    continue

                # ── In-network section ────────────────────────────────────────
                if not in_in_network:
                    continue

                # Fast-skip remainder of a rejected item
                if item_skip:
                    if prefix == "in_network.item" and event == "end_map":
                        in_item = False
                        item_skip = False
                    continue

                if prefix == "in_network.item" and event == "start_map":
                    items_scanned += 1
                    in_item = True
                    item_skip = False
                    item_billing_code = ""
                    item_billing_code_type = ""
                    item_billing_code_type_version = ""
                    item_name = ""
                    item_description = ""
                    item_negotiation_arrangement = ""
                    item_severity_of_illness = ""
                    in_rate = False
                    rate_provider_refs = []
                    in_price = False
                    continue

                if not in_item:
                    continue

                # Item scalar fields
                if prefix == "in_network.item.billing_code":
                    item_billing_code = str(value or "")
                    if filtered_only and not billing_code_matches(item_billing_code, code_prefixes):
                        item_skip = True
                    continue

                if prefix == "in_network.item.billing_code_type":
                    item_billing_code_type = str(value or "")
                    continue

                if prefix == "in_network.item.billing_code_type_version":
                    item_billing_code_type_version = str(value or "")
                    continue

                if prefix == "in_network.item.name":
                    item_name = str(value or "")
                    continue

                if prefix == "in_network.item.description":
                    item_description = str(value or "")
                    continue

                if prefix == "in_network.item.negotiation_arrangement":
                    item_negotiation_arrangement = str(value or "")
                    continue

                if prefix == "in_network.item.severity_of_illness":
                    item_severity_of_illness = str(value or "")
                    continue

                if prefix == "in_network.item" and event == "end_map":
                    in_item = False
                    if items_scanned % PROGRESS_EVERY_IN_NETWORK_ITEMS == 0:
                        log.info(
                            "    [single-pass] in_network_items=%s price_rows=%s",
                            f"{items_scanned:,}", f"{price_rows:,}",
                        )
                    continue

                # Rate level
                if prefix == "in_network.item.negotiated_rates.item" and event == "start_map":
                    in_rate = True
                    rate_provider_refs = []
                    continue

                if prefix == "in_network.item.negotiated_rates.item.provider_references.item" and event in ("number", "string"):
                    rate_provider_refs.append(value)
                    continue

                if prefix == "in_network.item.negotiated_rates.item" and event == "end_map":
                    in_rate = False
                    continue

                # Price level
                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item" and event == "start_map":
                    in_price = True
                    price_negotiated_type = ""
                    price_negotiated_rate = None
                    price_expiration_date = ""
                    price_billing_class = ""
                    price_service_codes = []
                    price_modifiers = []
                    price_additional_info = ""
                    price_estimated_amount = None
                    price_setting = ""
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.negotiated_type":
                    price_negotiated_type = str(value or "")
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.negotiated_rate":
                    price_negotiated_rate = safe_float(value)
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.expiration_date":
                    price_expiration_date = str(value or "")
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.billing_class":
                    price_billing_class = str(value or "")
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.service_code.item":
                    price_service_codes.append(str(value))
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.billing_code_modifier.item":
                    price_modifiers.append(str(value))
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.additional_information":
                    price_additional_info = str(value or "")
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.estimated_amount":
                    price_estimated_amount = safe_float(value)
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.setting":
                    price_setting = str(value or "")
                    continue

                if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item" and event == "end_map":
                    # Apply filters and emit
                    if filtered_only and not provider_refs_intersect(rate_provider_refs, matched_group_ids):
                        continue
                    price_batch.append({
                        "source_pricing_file": source_pricing_file,
                        "reporting_entity_name": header.get("reporting_entity_name", ""),
                        "reporting_entity_type": header.get("reporting_entity_type", ""),
                        "last_updated_on": header.get("last_updated_on", ""),
                        "version": header.get("version", ""),
                        "billing_code": item_billing_code,
                        "billing_code_type": item_billing_code_type,
                        "billing_code_type_version": item_billing_code_type_version,
                        "name": item_name,
                        "description": item_description,
                        "negotiation_arrangement": item_negotiation_arrangement,
                        "severity_of_illness": item_severity_of_illness,
                        "provider_references": normalize_list(rate_provider_refs),
                        "negotiated_type": price_negotiated_type,
                        "negotiated_rate": price_negotiated_rate,
                        "expiration_date": price_expiration_date,
                        "billing_class": price_billing_class,
                        "service_code": normalize_list(price_service_codes),
                        "billing_code_modifier": normalize_list(price_modifiers),
                        "additional_information": price_additional_info,
                        "estimated_amount": price_estimated_amount,
                        "setting": price_setting,
                        **row_lineage,
                    })
                    price_rows += 1
                    flush_batch_if_needed(price_writer_ref, price_batch, TIC_PRICE_SCHEMA, price_output, async_writer=_async_writer)
                    continue

    finally:
        heartbeat.stop()

    if pr_batch:
        _async_writer.write(pr_batch, TIC_PROVIDER_REFERENCE_SCHEMA, provider_ref_output)
    if price_batch:
        _async_writer.write(price_batch, TIC_PRICE_SCHEMA, price_output)
    _async_writer.close()

    log.info(
        "    [single-pass] complete: refs=%s matched_groups=%s items=%s price_rows=%s elapsed=%.0fs",
        f"{refs_scanned:,}", f"{len(matched_group_ids):,}",
        f"{items_scanned:,}", f"{price_rows:,}",
        time.time() - started_at,
    )
    return provider_rows, matched_group_ids, price_rows, header


def process_url(
    url: str,
    output_dir: Path,
    download_first: bool = True,
    download_dir: Path | None = None,
    keep_downloaded: bool = False,
    target_npis: set[str] | None = None,
    code_prefixes: tuple[str, ...] = (),
    filtered_only: bool = True,
    skip_oon: bool = False,
    pregate_npis: bool = True,
    target_npi_bytes: list[bytes] | None = None,
    keep_full_matched_provider_group: bool = False,
    two_pass: bool = False,
    source_url: str | None = None,
    lineage: dict[str, str] | None = None,
) -> bool:
    """Process one pricing file URL: single-pass by default, two-pass if --two-pass."""
    lineage = lineage or make_lineage_context("", "", "", "", "", "")
    canonical_url = source_url or url
    source_name = source_name_from_url(canonical_url)
    parse_source = url
    downloaded_path: Path | None = None

    provider_ref_path = output_dir / "tic_provider_reference" / f"{source_name}.parquet"
    price_path = output_dir / "tic_price" / f"{source_name}.parquet"
    oon_path = output_dir / "tic_out_of_network_allowed" / f"{source_name}.parquet"
    state_path = get_pass_state_path(output_dir, source_name)
    pass1_done = is_pass_done(output_dir, source_name, "pass1")
    pass2_done = is_pass_done(output_dir, source_name, "pass2")

    if pass1_done and pass2_done:
        log.info("  Already processed, skipping")
        return True

    try:
        if download_first and url.startswith(("http://", "https://")):
            if download_dir is None:
                raise ValueError("download_dir is required when download_first=True")
            downloaded_path = download_to_local(url, download_dir, source_name)
            parse_source = str(downloaded_path)

        if pregate_npis and filtered_only and not pass1_done:
            pregate_path = Path(parse_source)
            if pregate_path.exists():
                has_npi_text, matched_npi = pregate_file_for_npis(
                    pregate_path,
                    target_npi_bytes or [],
                )
                if not has_npi_text:
                    log.info("  NPI pre-gate: no target NPI text found, skipping file")
                    write_pass_state(output_dir, source_name, canonical_url, set(), 0)
                    mark_pass_done(output_dir, source_name, "pass1")
                    if not pass2_done:
                        mark_pass_done(output_dir, source_name, "pass2")
                    return True
                log.info("  NPI pre-gate matched: %s", matched_npi or "unknown")

        matched_group_ids: set[int] = set()
        provider_rows = 0

        # ── Single-pass: both sections in one file read ───────────────────────
        if not two_pass and not pass1_done and not pass2_done:
            log.info("  Single-pass: provider_references + in_network")
            provider_rows, matched_group_ids, price_rows, header = stream_single_pass(
                url=parse_source,
                source_pricing_file=canonical_url,
                provider_ref_output=provider_ref_path,
                price_output=price_path,
                target_npis=target_npis,
                filtered_only=filtered_only,
                code_prefixes=code_prefixes,
                keep_full_matched_provider_group=keep_full_matched_provider_group,
                lineage=lineage,
            )
            write_pass_state(output_dir, source_name, canonical_url, matched_group_ids, provider_rows)
            mark_pass_done(output_dir, source_name, "pass1")
            mark_pass_done(output_dir, source_name, "pass2")
            log.info(
                "  Single-pass done: %s provider rows, %s matched groups, %s price rows",
                f"{provider_rows:,}", f"{len(matched_group_ids):,}", f"{price_rows:,}",
            )

            if filtered_only or skip_oon:
                if skip_oon and not filtered_only:
                    log.info("  Skipping out_of_network due to --skip-oon")
            else:
                log.info("  Pass OON: out_of_network (if present)")
                header = header or get_header_fields(parse_source)
                oon_rows = stream_out_of_network(parse_source, canonical_url, oon_path, header)
                if oon_rows:
                    log.info("  OON done: %s allowed-amount rows", f"{oon_rows:,}")
                else:
                    log.info("  No out_of_network section in this file")
            return True

        # ── Two-pass fallback (--two-pass flag, or resuming after pass1 done) ─
        if not pass1_done:
            log.info("  Pass 1: provider_references")
            pass1_header = get_header_fields(parse_source)
            provider_lineage = lineage_with_source_version(
                lineage,
                pass1_header.get("version", ""),
            )
            provider_rows, matched_group_ids = stream_provider_references(
                parse_source,
                canonical_url,
                provider_ref_path,
                target_npis=target_npis,
                filtered_only=filtered_only,
                keep_full_matched_provider_group=keep_full_matched_provider_group,
                lineage=provider_lineage,
            )
            write_pass_state(output_dir, source_name, canonical_url, matched_group_ids, provider_rows)
            mark_pass_done(output_dir, source_name, "pass1")
            log.info("  Pass 1 done: %s provider rows", f"{provider_rows:,}")
            log.info("  Pass 1 state written: %s", state_path.name)
        else:
            state = read_pass_state(output_dir, source_name)
            if state is None and filtered_only:
                log.error("  Pass 2 requires pass-state file: %s", state_path)
                return False
            if state is not None:
                matched_group_ids = {safe_int(value) for value in state.get("matched_group_ids", [])}
                matched_group_ids.discard(None)
                provider_rows = int(state.get("provider_rows", 0))
                log.info(
                    "  Loaded pass 1 state: provider_rows=%s matched_groups=%s",
                    f"{provider_rows:,}",
                    f"{len(matched_group_ids):,}",
                )
            elif pass1_done:
                log.error("  Pass 1 done marker exists but pass-state file is missing: %s", state_path)
                return False

        if filtered_only:
            log.info("  Matched provider groups: %s", f"{len(matched_group_ids):,}")
            if not matched_group_ids:
                log.info("  No matching provider groups; skipping price extraction")
                if not pass2_done:
                    mark_pass_done(output_dir, source_name, "pass2")
                return True

        if not pass2_done:
            header = get_header_fields(parse_source)
            price_lineage = lineage_with_source_version(lineage, header.get("version", ""))

            log.info("  Pass 2: in_network prices")
            price_rows = stream_in_network(
                parse_source,
                canonical_url,
                price_path,
                header,
                allowed_group_ids=matched_group_ids,
                code_prefixes=code_prefixes,
                filtered_only=filtered_only,
                lineage=price_lineage,
            )
            mark_pass_done(output_dir, source_name, "pass2")
            log.info("  Pass 2 done: %s price rows", f"{price_rows:,}")

            if filtered_only or skip_oon:
                if skip_oon and not filtered_only:
                    log.info("  Skipping out_of_network due to --skip-oon")
                elif filtered_only:
                    log.info("  Skipping out_of_network in filtered-only mode")
            else:
                log.info("  Pass 2b: out_of_network (if present)")
                oon_rows = stream_out_of_network(parse_source, canonical_url, oon_path, header)
                if oon_rows:
                    log.info("  OON done: %s allowed-amount rows", f"{oon_rows:,}")
                else:
                    log.info("  No out_of_network section in this file")
        elif pass2_done:
            log.info("  Pass 2 already completed, skipping")

        return True
    except requests.exceptions.RequestException as exc:
        log.error("  HTTP error: %s", exc)
        return False
    except Exception as exc:  # pragma: no cover - runtime parser/network failures
        log.error("  Failed: %s", exc)
        return False
    finally:
        if downloaded_path and downloaded_path.exists() and not keep_downloaded:
            try:
                downloaded_path.unlink()
                log.info("  Deleted staged download: %s", downloaded_path.name)
            except OSError as exc:
                log.warning("  Could not delete staged download %s: %s", downloaded_path, exc)


def iter_urls(urls_path: Path, limit: int | None):
    seen = 0
    with urls_path.open(encoding="utf-8") as fh:
        for raw_line in fh:
            url = normalize_source_url(raw_line)
            if not url:
                continue
            if not is_supported_source(url):
                log.error("Skipping unsupported source entry: %s", url[:120])
                continue
            yield seen + 1, url
            seen += 1
            if limit is not None and seen >= limit:
                break


def count_urls(urls_path: Path, limit: int | None) -> int:
    total = 0
    with urls_path.open(encoding="utf-8") as fh:
        for raw_line in fh:
            url = normalize_source_url(raw_line)
            if not url or not is_supported_source(url):
                continue
            total += 1
            if limit is not None and total >= limit:
                break
    return total


def load_urls(urls_path: Path, limit: int | None) -> list[tuple[int, str]]:
    return list(iter_urls(urls_path, limit))


def process_url_worker(payload: dict) -> tuple[int, str, bool]:
    index = payload["index"]
    url = payload["url"]
    ok = process_url(
        url=url,
        output_dir=Path(payload["output_dir"]),
        download_first=payload["download_first"],
        download_dir=Path(payload["download_dir"]) if payload["download_dir"] else None,
        keep_downloaded=payload["keep_downloaded"],
        target_npis=set(payload["target_npis"]) if payload["target_npis"] is not None else None,
        code_prefixes=tuple(payload["code_prefixes"]),
        filtered_only=payload["filtered_only"],
        skip_oon=payload["skip_oon"],
        pregate_npis=payload["pregate_npis"],
        target_npi_bytes=[bytes(value, "ascii") for value in payload["target_npi_bytes"]]
        if payload["target_npi_bytes"] is not None
        else None,
        keep_full_matched_provider_group=payload["keep_full_matched_provider_group"],
        two_pass=payload["two_pass"],
        lineage=dict(payload["lineage"]),
    )
    return index, url, ok


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Stream TiC pricing files from matched_pricing_urls.txt and write "
            "tic_price, tic_provider_reference, and "
            "tic_out_of_network_allowed Parquet datasets."
        )
    )
    parser.add_argument(
        "--urls",
        default="data/raw/matched_pricing_urls.txt",
        help="Path to matched_pricing_urls.txt (output of scan_pricing_urls.py)",
    )
    parser.add_argument(
        "--output",
        default="data/parquet/",
        help="Root output directory for Parquet datasets (default: data/parquet/)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of files to process (useful for testing)",
    )
    parser.add_argument(
        "--download-first",
        action="store_true",
        help=(
            "Legacy no-op alias; local download-first is now the default for remote URLs."
        ),
    )
    parser.add_argument(
        "--stream-remote",
        action="store_true",
        help="Do not stage remote files locally first; parse directly from the URL stream.",
    )
    parser.add_argument(
        "--download-dir",
        default="data/raw/pricing_staging",
        help="Local staging folder used with --download-first",
    )
    parser.add_argument(
        "--keep-downloaded",
        action="store_true",
        help="Legacy alias for --retain-downloads",
    )
    parser.add_argument(
        "--retain-downloads",
        action="store_true",
        help="Keep staged downloads after processing",
    )
    parser.add_argument(
        "--delete-downloaded",
        action="store_true",
        help="Delete staged downloads after processing",
    )
    parser.add_argument(
        "--target-npis-parquet",
        default=None,
        help="Directory containing filtered NPPES parquet used to define target NPIs",
    )
    parser.add_argument(
        "--code-prefixes",
        nargs="*",
        default=[],
        help=(
            "Billing code prefixes to include (e.g. 610 611 99). "
            "Default: empty — all billing codes are kept. "
            "Example for neurology codes: --code-prefixes 610 611 612 613 614 615 "
            "616 617 618 619 620 621 622 623 624 625 626 627 628 629 630 631 632 633"
        ),
    )
    parser.add_argument(
        "--filtered-only",
        action="store_true",
        help="Legacy alias; filtered extraction is now the default behavior.",
    )
    parser.add_argument(
        "--full-extract",
        action="store_true",
        help="Disable filtered extraction and write broad/full provider and price outputs.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of pricing files to process in parallel (default: 1)",
    )
    parser.add_argument(
        "--skip-oon",
        action="store_true",
        help="Skip writing tic_out_of_network_allowed and only build provider/price outputs.",
    )
    parser.add_argument(
        "--pregate-npis",
        action="store_true",
        help="Legacy alias; NPI pre-gating is now the default behavior.",
    )
    parser.add_argument(
        "--no-pregate-npis",
        action="store_true",
        help="Disable the default NPI text pre-gate before structured provider parsing.",
    )
    parser.add_argument(
        "--keep-full-matched-provider-group",
        action="store_true",
        help=(
            "In filtered pass 1, if any target NPI appears anywhere inside a "
            "provider_references item, keep all provider rows from that matched "
            "provider-group item, including sibling NPIs."
        ),
    )
    parser.add_argument(
        "--two-pass",
        action="store_true",
        help=(
            "Use the legacy two-pass strategy (separate provider_references and in_network reads) "
            "instead of the default single-pass. Useful for resuming after a failed pass 1."
        ),
    )
    parser.add_argument(
        "--file",
        default=None,
        help=(
            "Process a single local file directly, skipping --urls and all download logic. "
            "Example: --file \"C:/Users/you/TiC/myfile.json.gz\""
        ),
    )
    parser.add_argument(
        "--source-url",
        default=None,
        help=(
            "Override the value stored in source_pricing_file for --file mode. "
            "Use this to record the original URL when running on a locally cached file. "
            "Example: --source-url \"https://transparency-in-coverage.uhc.com/.../file.json.gz\""
        ),
    )
    parser.add_argument("--payer-code", default="UHC")
    parser.add_argument("--file-month", default=None, help="Source file month as YYYY-MM")
    parser.add_argument("--state", default="TX")
    parser.add_argument(
        "--source-version",
        default="",
        help="Source data/version label. Defaults to the TiC JSON version when present.",
    )
    parser.add_argument("--etl-run-id", default=None)
    args = parser.parse_args()
    ingested_at = central_now().isoformat(timespec="seconds")
    lineage = make_lineage_context(
        payer_code=args.payer_code,
        file_month=args.file_month or default_file_month(),
        state=args.state,
        source_version=args.source_version,
        etl_run_id=args.etl_run_id or str(uuid4()),
        ingested_at=ingested_at,
    )

    output_dir = Path(args.output)
    download_dir = Path(args.download_dir)
    code_prefixes = normalize_code_prefixes(args.code_prefixes)
    target_npis: set[str] | None = None
    target_npi_bytes: list[bytes] | None = None
    filtered_only = not args.full_extract
    pregate_npis = not args.no_pregate_npis

    if filtered_only:
        if not args.target_npis_parquet:
            log.error("Filtered extraction requires --target-npis-parquet")
            sys.exit(1)
        target_npis_dir = Path(args.target_npis_parquet)
        if not target_npis_dir.exists():
            log.error("Target NPI parquet dir not found: %s", target_npis_dir)
            sys.exit(1)
        target_npis = load_target_npis(target_npis_dir)
        if not target_npis:
            log.error("No target NPIs found in %s", target_npis_dir)
            sys.exit(1)
        target_npi_bytes = compile_target_npi_bytes(target_npis)

    # ── Single-file mode (--file) ─────────────────────────────────────────────
    if args.file:
        local_file = Path(args.file)
        if not local_file.exists():
            log.error("File not found: %s", local_file)
            sys.exit(1)
        log.info("Single-file mode: %s", local_file)
        if args.source_url:
            log.info("Source URL override: %s", args.source_url)
        ok = process_url(
            url=str(local_file),
            output_dir=output_dir,
            download_first=False,       # already local — never download
            download_dir=download_dir,
            keep_downloaded=True,       # it's the user's file — never delete it
            target_npis=target_npis,
            code_prefixes=code_prefixes,
            filtered_only=filtered_only,
            skip_oon=args.skip_oon,
            pregate_npis=pregate_npis,
            target_npi_bytes=target_npi_bytes,
            keep_full_matched_provider_group=args.keep_full_matched_provider_group,
            two_pass=args.two_pass,
            source_url=args.source_url,
            lineage=lineage,
        )
        log.info("Done. %s", "ok" if ok else "FAILED")
        sys.exit(0 if ok else 1)

    # ── URL-list mode ─────────────────────────────────────────────────────────
    if args.retain_downloads and args.delete_downloaded:
        log.error("Choose only one of --retain-downloads or --delete-downloaded")
        sys.exit(1)

    if args.keep_downloaded:
        keep_downloaded = True
    elif args.retain_downloads:
        keep_downloaded = True
    elif args.delete_downloaded:
        keep_downloaded = False
    else:
        keep_downloaded = False

    download_first = not args.stream_remote

    urls_path = Path(args.urls)
    if not urls_path.exists():
        log.error("URLs file not found: %s", urls_path)
        sys.exit(1)

    total_urls = count_urls(urls_path, args.limit)
    if total_urls == 0:
        log.error("No URLs found in %s", urls_path)
        sys.exit(1)

    log.info("Processing %d pricing file URLs", total_urls)
    log.info("Output directory: %s", output_dir)
    log.info("Workers: %d", args.workers)
    if download_first:
        log.info("Local staging mode enabled")
        log.info("Download staging directory: %s", download_dir)
        log.info("Retain staged downloads: %s", keep_downloaded)
    else:
        log.info("Remote streaming mode enabled")
    if filtered_only:
        log.info("Filtered extraction enabled")
        log.info("Loaded %s target NPIs", f"{len(target_npis or set()):,}")
        log.info("Code prefixes: %s", ", ".join(code_prefixes) if code_prefixes else "ALL")
    else:
        log.info("Full extract mode enabled")
    if pregate_npis:
        log.info("NPI pre-gate enabled")
    else:
        log.info("NPI pre-gate disabled")

    success = 0
    failed = 0
    url_entries = load_urls(urls_path, args.limit)

    if args.workers <= 1:
        for index, url in url_entries:
            log.info("[%d/%d] %s", index, total_urls, url[:90])
            if process_url(
                url,
                output_dir,
                download_first=download_first,
                download_dir=download_dir,
                keep_downloaded=keep_downloaded,
                target_npis=target_npis,
                code_prefixes=code_prefixes,
                filtered_only=filtered_only,
                skip_oon=args.skip_oon,
                pregate_npis=pregate_npis,
                target_npi_bytes=target_npi_bytes,
                keep_full_matched_provider_group=args.keep_full_matched_provider_group,
                two_pass=args.two_pass,
                lineage=lineage,
            ):
                success += 1
            else:
                failed += 1
    else:
        job_payloads = [
            {
                "index": index,
                "url": url,
                "output_dir": str(output_dir),
                "download_first": download_first,
                "download_dir": str(download_dir),
                "keep_downloaded": keep_downloaded,
                "target_npis": sorted(target_npis) if target_npis is not None else None,
                "code_prefixes": list(code_prefixes),
                "filtered_only": filtered_only,
                "skip_oon": args.skip_oon,
                "pregate_npis": pregate_npis,
                "target_npi_bytes": [value.decode("ascii") for value in (target_npi_bytes or [])],
                "keep_full_matched_provider_group": args.keep_full_matched_provider_group,
                "two_pass": args.two_pass,
                "lineage": lineage,
            }
            for index, url in url_entries
        ]

        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(process_url_worker, payload): payload
                for payload in job_payloads
            }
            for future in as_completed(futures):
                payload = futures[future]
                index = payload["index"]
                url = payload["url"]
                try:
                    _, _, ok = future.result()
                    if ok:
                        success += 1
                    else:
                        failed += 1
                    log.info(
                        "[%d/%d] complete status=%s %s",
                        index,
                        total_urls,
                        "ok" if ok else "failed",
                        url[:90],
                    )
                except Exception as exc:
                    failed += 1
                    log.error("[%d/%d] worker failed: %s", index, total_urls, exc)

    log.info("Done. %d succeeded, %d failed", success, failed)


if __name__ == "__main__":
    main()
