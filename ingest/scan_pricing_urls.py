"""
scan_pricing_urls.py

Scans TiC pricing file URLs from plan_pricing_bridge for relevance using
two signals:

  Signal 1 — NPI match:   provider_references contains an NPI from nppes_provider
  Signal 2 — Code match:  in_network contains a billing code matching target prefixes

Three pre-filtering layers eliminate obviously irrelevant URLs before any
HTTP request is made:

  Layer 1 — --skip-keywords:     skip URLs containing these strings
                                  e.g. acupuncture chiro dental vision
  Layer 2 — --url-must-contain:  skip URLs that contain NONE of these strings
                                  e.g. surgical medical ppo network
  Layer 3 — --skip-states:       skip URLs containing other state names/codes
                                  e.g. Mississippi Alabama if targeting TX only

These filters are all optional and runtime-configurable so the script works
for any specialty and state combination.

Usage
-----
    python scan_pricing_urls.py \
      --nppes           data/parquet/nppes_provider/ \
      --bridge          data/parquet/plan_pricing_bridge/ \
      --output          data/raw/matched_pricing_urls.txt \
      [--url-contains   uhc cigna] \
      [--skip-keywords  acupuncture chiro dental vision massage naturopath pharmacy] \
      [--url-must-contain surgical medical ppo network] \
      [--skip-states    Mississippi Alabama Arkansas Louisiana] \
      [--code-prefixes  610 611 612 613 620 621 622 623 630 631 632 633] \
      [--require-both] \
      [--workers        20] \
      [--limit          100]

Notes
-----
- All three pre-filter layers are applied before any HTTP request.
- --skip-keywords and --url-must-contain match against the full URL string.
- --skip-states matches state names AND common 2-letter codes (_MS_, _AL_ etc).
- Re-runs are safe: already-matched URLs are skipped.
"""

import argparse
import gzip
import logging
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import duckdb
import ijson
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

STREAM_TIMEOUT = (15, 90)   # (connect_timeout, read_timeout) in seconds

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, */*",
}

_thread_local = threading.local()

# Default CPT prefixes for Neurological Surgery — override with --code-prefixes
DEFAULT_CODE_PREFIXES = ()

# All US state names and codes for --skip-states matching
STATE_CODES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
}


# ---------------------------------------------------------------------------
# Load NPI set from NPPES Parquet
# ---------------------------------------------------------------------------

def load_target_npis(nppes_dir: Path) -> set:
    parquet_glob = (nppes_dir / "*.parquet").as_posix()
    con = duckdb.connect()
    result = con.execute(
        f"SELECT DISTINCT npi FROM read_parquet('{parquet_glob}') WHERE npi IS NOT NULL"
    ).fetchall()
    npis = {row[0] for row in result}
    log.info("Loaded %s target NPIs from NPPES parquet", f"{len(npis):,}")
    return npis


# ---------------------------------------------------------------------------
# Load + filter pricing URLs entirely inside DuckDB — never pull 23M rows to Python
# ---------------------------------------------------------------------------

def write_filtered_urls(
    bridge_dir: Path,
    staging_path: Path,
    url_contains: list,
    skip_keywords: list,
    must_contain: list,
    skip_states: list,
    target_states: list,
    already_matched: set,
) -> int:
    """
    Run all filter layers inside DuckDB and write surviving URLs to a staging
    text file — one URL|plan_name|entity_name per line.  Nothing is returned
    to Python memory; the file is read lazily during scanning.

    Returns the count of URLs written.
    """
    parquet_glob = (bridge_dir / "*.parquet").as_posix()
    n_files = len(list(bridge_dir.glob("*.parquet")))
    log.info("Reading %d parquet file(s) from %s ...", n_files, bridge_dir)

    con = duckdb.connect()
    conditions = ["location IS NOT NULL", "location != ''"]

    if url_contains:
        clauses = " OR ".join(f"location ILIKE '%{k}%'" for k in url_contains)
        conditions.append(f"({clauses})")
        log.info("Filter: url-contains  %s", url_contains)

    if skip_keywords:
        clauses = " AND ".join(f"location NOT ILIKE '%{k}%'" for k in skip_keywords)
        conditions.append(f"({clauses})")
        log.info("Filter: skip-keywords %s", skip_keywords)

    if must_contain:
        clauses = " OR ".join(f"location ILIKE '%{k}%'" for k in must_contain)
        conditions.append(f"({clauses})")
        log.info("Filter: must-contain  %s", must_contain)

    # Skip-states: drop rows whose URL mentions a skip-state but NOT a target-state.
    # Use ILIKE for state names; for 2-letter codes use regexp_matches to avoid
    # SQL ILIKE treating _ as a single-char wildcard.
    if skip_states:
        skip_terms = []
        for name in skip_states:
            code = STATE_CODES.get(name, name.upper()[:2])
            skip_terms.append(f"location ILIKE '%{name}%'")
            # regexp_matches is safe — no wildcard ambiguity
            skip_terms.append(
                f"regexp_matches(location, '[-_]{re.escape(code)}[-_]', 'i')"
            )
        has_skip = " OR ".join(skip_terms)

        if target_states:
            target_terms = []
            for name in target_states:
                code = STATE_CODES.get(name, name.upper()[:2])
                target_terms.append(f"location ILIKE '%{name}%'")
                target_terms.append(
                    f"regexp_matches(location, '[-_]{re.escape(code)}[-_]', 'i')"
                )
            has_target = " OR ".join(target_terms)
            conditions.append(f"NOT (({has_skip}) AND NOT ({has_target}))")
        else:
            conditions.append(f"NOT ({has_skip})")
        log.info("Filter: skip-states   %s", skip_states)

    where_clause = " AND ".join(conditions)
    sql = f"""
        SELECT location,
               MIN(plan_name)              AS plan_name,
               MIN(reporting_entity_name)  AS reporting_entity_name
        FROM read_parquet('{parquet_glob}')
        WHERE {where_clause}
        GROUP BY location
        ORDER BY location
    """

    log.info("Running DuckDB pre-filter query ...")
    staging_path.parent.mkdir(parents=True, exist_ok=True)

    # Always rebuild staging from scratch — never carry over a previous run's filters
    if staging_path.exists():
        staging_path.unlink()
        log.info("Cleared previous staging file")

    written = 0
    result = con.execute(sql)
    with staging_path.open("w", encoding="utf-8") as fh:
        while True:
            batch = result.fetchmany(10_000)
            if not batch:
                break
            for (location, plan_name, entity_name) in batch:
                if location in already_matched:
                    continue
                plan_name   = (plan_name   or "").replace("|", " ")
                entity_name = (entity_name or "").replace("|", " ")
                fh.write(f"{location}|{plan_name}|{entity_name}\n")
                written += 1

    log.info("Pre-filter complete: %s URLs written to staging file", f"{written:,}")
    return written


def iter_staging(staging_path: Path):
    """Yield (location, plan_name, entity_name) lazily from the staging file."""
    with staging_path.open(encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("|", 2)
            if len(parts) == 3:
                yield {"location": parts[0], "plan_name": parts[1], "reporting_entity_name": parts[2]}


def write_size_filtered_staging(
    input_staging_path: Path,
    output_staging_path: Path,
    max_bytes: int,
    workers: int,
) -> int:
    """
    Rebuild scan_staging.txt so it reflects post-HEAD size filtering, not just
    the DuckDB URL pre-filter.
    """
    entries = list(iter_staging(input_staging_path))
    kept_entries = []

    def check_entry(entry: dict):
        size_bytes = get_remote_size_bytes(entry["location"])
        return entry, size_bytes

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(check_entry, entry) for entry in entries]
        for future in as_completed(futures):
            entry, size_bytes = future.result()
            if size_bytes is not None and size_bytes <= max_bytes:
                kept_entries.append(entry)

    kept_entries.sort(key=lambda entry: entry["location"])
    if output_staging_path.exists():
        output_staging_path.unlink()

    with output_staging_path.open("w", encoding="utf-8") as fh:
        for entry in kept_entries:
            plan_name = (entry.get("plan_name") or "").replace("|", " ")
            entity_name = (entry.get("reporting_entity_name") or "").replace("|", " ")
            fh.write(f"{entry['location']}|{plan_name}|{entity_name}\n")

    log.info(
        "Size filter complete: %s URLs <= %s bytes written to staging file",
        f"{len(kept_entries):,}",
        f"{max_bytes:,}",
    )
    return len(kept_entries)


# ---------------------------------------------------------------------------
# HTTP streaming helpers
# ---------------------------------------------------------------------------

class _PrefixedStream:
    """Glue two bytes we already peeked back onto a live socket stream.

    This lets ijson stream through the response without ever buffering the
    whole file — the two peeked bytes are served first, then reads fall
    straight through to the socket.
    """
    def __init__(self, prefix: bytes, stream):
        self._prefix = prefix
        self._pos = 0
        self._stream = stream

    def read(self, n=-1):
        if self._pos < len(self._prefix):
            head = self._prefix[self._pos:]
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


def _stream_body(resp: requests.Response):
    """Return a streaming body, decompressing gzip if needed — no buffering.

    Peeks at 2 bytes to detect raw gzip (magic \x1f\x8b).  Those bytes are
    glued back via _PrefixedStream so the full stream reaches ijson intact.
    urllib3's decode_content=True handles Content-Encoding: gzip transparently,
    so after that path the content is already plain JSON.
    """
    resp.raw.decode_content = True
    peek = resp.raw.read(2)
    stream = _PrefixedStream(peek, resp.raw)
    if peek == b"\x1f\x8b":
        return gzip.open(stream, "rb")
    return stream


def _get_session() -> requests.Session:
    """Reuse HTTP connections within each worker thread."""
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        _thread_local.session = session
    return session


def get_remote_size_bytes(url: str) -> Optional[int]:
    """Return compressed file size in bytes from HTTP HEAD when available."""
    try:
        with _get_session().head(url, timeout=STREAM_TIMEOUT, allow_redirects=True) as resp:
            resp.raise_for_status()
            content_length = resp.headers.get("Content-Length")
            if content_length:
                return int(content_length)
    except requests.exceptions.RequestException as e:
        log.warning("HTTP error (HEAD) %s: %s", url[:70], e)
    except Exception as e:
        log.warning("Header parse error %s: %s", url[:70], e)
    return None


def check_npi_match(url: str, target_npis: set) -> bool:
    try:
        with _get_session().get(url, stream=True, timeout=STREAM_TIMEOUT) as resp:
            resp.raise_for_status()
            stream = _stream_body(resp)
            for npi_val in ijson.items(
                stream, "provider_references.item.provider_groups.item.npi.item"
            ):
                if str(npi_val).strip() in target_npis:
                    return True
    except requests.exceptions.RequestException as e:
        log.warning("HTTP error (NPI) %s: %s", url[:70], e)
    except Exception as e:
        log.warning("Parse error (NPI) %s: %s", url[:70], e)
    return False


def check_code_match(url: str, code_prefixes: tuple) -> bool:
    if not code_prefixes:
        return True
    try:
        with _get_session().get(url, stream=True, timeout=STREAM_TIMEOUT) as resp:
            resp.raise_for_status()
            stream = _stream_body(resp)
            for item in ijson.items(stream, "in_network.item"):
                code = str(item.get("billing_code", "") or "").strip()
                if any(code.startswith(p) for p in code_prefixes):
                    return True
    except requests.exceptions.RequestException as e:
        log.warning("HTTP error (code) %s: %s", url[:70], e)
    except Exception as e:
        log.warning("Parse error (code) %s: %s", url[:70], e)
    return False


def evaluate_url(
    entry: dict,
    target_npis: set,
    code_prefixes: tuple,
    require_both: bool,
    npi_only: bool,
    code_only: bool = False,
    max_bytes: int | None = None,
) -> tuple:
    """Returns (url, name, matched: bool, signal: str)"""
    url  = entry["location"]
    name = entry.get("reporting_entity_name", "")

    log.info("→ scanning: %s", url.split("?")[0][-80:])

    if max_bytes is not None:
        size_bytes = get_remote_size_bytes(url)
        if size_bytes is None:
            return url, name, False, "no size"
        if size_bytes > max_bytes:
            return url, name, False, f"too large ({size_bytes:,} bytes)"

    if npi_only:
        has_npi = check_npi_match(url, target_npis)
        log.info("  NPI=%s  %s", has_npi, url.split("?")[0][-60:])
        return url, name, has_npi, "NPI" if has_npi else "no NPI match"

    if code_only:
        has_code = check_code_match(url, code_prefixes)
        log.info("  code=%s %s", has_code, url.split("?")[0][-60:])
        return url, name, has_code, "code" if has_code else "no code match"

    has_npi = check_npi_match(url, target_npis)
    log.info("  NPI=%s  %s", has_npi, url.split("?")[0][-60:])

    if require_both:
        if not has_npi:
            return url, name, False, "no NPI match"
        has_code = check_code_match(url, code_prefixes)
        matched  = has_npi and has_code
        signal   = "NPI+code" if matched else "NPI=yes, code=no"
    else:
        has_code = check_code_match(url, code_prefixes) if not has_npi else True
        matched  = has_npi or has_code
        parts    = (["NPI"] if has_npi else []) + (["code"] if has_code else [])
        signal   = "+".join(parts) if parts else "no match"

    return url, name, matched, signal


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Scan TiC pricing URLs for relevant providers using NPI and "
            "billing code signals, with URL pre-filtering to reduce HTTP requests."
        )
    )
    parser.add_argument("--nppes",   default="data/parquet/nppes_provider/",
                        help="Directory containing nppes_provider.parquet (skipped if --npis covers all targets)")
    parser.add_argument("--npis", nargs="*", default=[], metavar="NPI",
                        help=(
                            "One or more NPIs to target directly, e.g. --npis 1234567890. "
                            "Added to (or replaces) the NPPES-derived NPI set. "
                            "If provided and --nppes dir is absent, NPPES loading is skipped entirely."
                        ))
    parser.add_argument("--bridge",  default="data/parquet/plan_pricing_bridge/",
                        help="Directory containing plan_pricing_bridge.parquet")
    parser.add_argument("--output",  default="data/raw/matched_pricing_urls.txt",
                        help="Output text file of matched URLs")
    parser.add_argument("--resume", action="store_true",
                        help="Append to an existing matched_pricing_urls.txt instead of rebuilding it fresh")

    # Pre-filter arguments (no HTTP cost)
    parser.add_argument("--url-contains", nargs="*", default=[], metavar="KEYWORD",
                        help="Only consider URLs containing these substrings e.g. --url-contains uhc")
    parser.add_argument("--skip-keywords", nargs="*", default=[], metavar="KEYWORD",
                        help=(
                            "Skip URLs containing these substrings (case-insensitive). "
                            "e.g. --skip-keywords acupuncture chiro dental vision massage"
                        ))
    parser.add_argument("--url-must-contain", nargs="*", default=[], metavar="KEYWORD",
                        help=(
                            "Skip URLs that contain NONE of these substrings. "
                            "e.g. --url-must-contain surgical medical ppo"
                        ))
    parser.add_argument("--skip-states", nargs="*", default=[], metavar="STATE",
                        help=(
                            "Skip URLs that appear to be for these states (name or 2-letter code). "
                            "e.g. --skip-states Mississippi Alabama Louisiana Arkansas"
                        ))
    parser.add_argument("--target-states", nargs="*", default=[], metavar="STATE",
                        help=(
                            "Your target states — URLs mentioning these are never skipped. "
                            "e.g. --target-states Texas TX"
                        ))

    # HTTP scan arguments
    parser.add_argument("--code-prefixes", nargs="*", default=list(DEFAULT_CODE_PREFIXES),
                        metavar="PREFIX",
                        help="Billing code prefixes to match. Omit to disable code matching.")
    parser.add_argument("--require-both", action="store_true",
                        help="Require BOTH NPI match AND code match (more precise, two HTTP passes)")
    parser.add_argument("--npi-only", action="store_true",
                        help=(
                            "Match on NPI presence only and skip all in_network code scans. "
                            "Fastest option for very large pricing files."
                        ))
    parser.add_argument("--code-only", action="store_true",
                        help=(
                            "Match on billing-code presence only and skip all NPI checks. "
                            "Useful when you want state-filtered pricing files regardless of provider list."
                        ))
    parser.add_argument("--workers", type=int, default=10,
                        help="Parallel scan workers (default: 10)")
    parser.add_argument("--max-bytes", type=int, default=None,
                        help="Skip files larger than this many compressed bytes using HTTP HEAD")
    parser.add_argument("--max-gb", type=float, default=None,
                        help="Skip files larger than this many decimal GB using HTTP HEAD")
    parser.add_argument("--limit",   type=int, default=None,
                        help="Max URLs to scan after pre-filtering (for testing)")
    args = parser.parse_args()

    nppes_dir   = Path(args.nppes)
    bridge_dir  = Path(args.bridge)
    output_path = Path(args.output)
    manifest_path = output_path.with_name(f"{output_path.stem}_manifest.txt")

    if not bridge_dir.exists():
        log.error("Bridge dir not found: %s", bridge_dir)
        sys.exit(1)
    if args.npi_only and args.code_only:
        log.error("Choose only one of --npi-only or --code-only")
        sys.exit(1)
    if args.require_both and args.code_only:
        log.error("--require-both cannot be combined with --code-only")
        sys.exit(1)

    already_matched = set()
    if args.resume and output_path.exists():
        already_matched = {
            line.strip()
            for line in output_path.read_text().splitlines()
            if line.strip()
        }
        log.info("Resuming -- %d URLs already matched", len(already_matched))
    elif output_path.exists():
        output_path.unlink()
        log.info("Cleared previous output file: %s", output_path)
    if not args.resume and manifest_path.exists():
        manifest_path.unlink()
        log.info("Cleared previous manifest file: %s", manifest_path)

    # Build target NPI set: NPPES parquet + any explicit --npis overrides
    explicit_npis = {npi.strip() for npi in args.npis if npi.strip()}
    if nppes_dir.exists():
        target_npis = load_target_npis(nppes_dir)
        if explicit_npis:
            target_npis |= explicit_npis
            log.info("Added %d explicit NPI(s) from --npis; total: %d", len(explicit_npis), len(target_npis))
    elif explicit_npis:
        target_npis = explicit_npis
        log.info("NPPES dir not found — using %d explicit NPI(s) from --npis", len(target_npis))
    else:
        log.error("No NPIs available: NPPES dir not found (%s) and --npis not provided", nppes_dir)
        sys.exit(1)

    code_prefixes = tuple(args.code_prefixes)
    max_bytes = args.max_bytes
    if args.max_gb is not None:
        max_bytes = int(args.max_gb * 1_000_000_000)

    if not target_npis:
        log.error("No NPIs loaded -- check nppes_provider parquet or provide --npis")
        sys.exit(1)

    staging_path = output_path.parent / "scan_staging.txt"
    prefilter_staging_path = output_path.parent / "scan_prefilter.txt"
    prefilter_count = write_filtered_urls(
        bridge_dir=bridge_dir,
        staging_path=prefilter_staging_path,
        url_contains=args.url_contains or [],
        skip_keywords=args.skip_keywords or [],
        must_contain=args.url_must_contain or [],
        skip_states=args.skip_states or [],
        target_states=args.target_states or [],
        already_matched=already_matched,
    )
    total_to_scan = prefilter_count
    if max_bytes is not None:
        total_to_scan = write_size_filtered_staging(
            input_staging_path=prefilter_staging_path,
            output_staging_path=staging_path,
            max_bytes=max_bytes,
            workers=args.workers,
        )
    else:
        if staging_path.exists():
            staging_path.unlink()
        prefilter_staging_path.replace(staging_path)

    log.info(
        "Starting HTTP scan: %s URLs | workers=%d | require_both=%s | npi_only=%s | code_prefixes=%d | max_bytes=%s",
        f"{total_to_scan:,}", args.workers, args.require_both, args.npi_only, len(code_prefixes),
        f"{max_bytes:,}" if max_bytes is not None else "NONE",
    )

    matched_count = 0
    scanned = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Submit futures lazily — only args.workers * 4 in flight at once so the
    # full URL list never loads into Python memory simultaneously.
    queue_size = args.workers * 4

    output_mode = "a" if args.resume else "w"
    with output_path.open(output_mode, encoding="utf-8") as out_f, manifest_path.open(output_mode, encoding="utf-8") as manifest_f:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            url_iter = iter_staging(staging_path)
            if args.limit:
                from itertools import islice
                url_iter = islice(url_iter, args.limit)

            futures = {}
            # Seed initial batch
            for entry in url_iter:
                if len(futures) >= queue_size:
                    break
                futures[executor.submit(
                    evaluate_url,
                    entry,
                    target_npis,
                    code_prefixes,
                    args.require_both,
                    args.npi_only,
                    args.code_only,
                    max_bytes,
                )] = entry

            while futures:
                for future in as_completed(futures):
                    entry = futures.pop(future)
                    scanned += 1
                    try:
                        url, name, matched, signal = future.result()
                        if matched:
                            size_bytes = get_remote_size_bytes(url)
                            out_f.write(url + "\n")
                            out_f.flush()
                            manifest_f.write(
                                f"{url}|{'' if size_bytes is None else size_bytes}|{signal}|{(name or '').replace('|', ' ')}\n"
                            )
                            manifest_f.flush()
                            matched_count += 1
                            log.info("[%d/%d] MATCH (%s): %s", scanned, total_to_scan, signal, name)
                        else:
                            log.info("[%d/%d] skip  (%s): %s", scanned, total_to_scan, signal, name)
                    except Exception as e:
                        log.error("[%d/%d] error: %s", scanned, total_to_scan, e)

                    # Refill from the iterator
                    try:
                        next_entry = next(url_iter)
                        futures[executor.submit(
                            evaluate_url,
                            next_entry,
                            target_npis,
                            code_prefixes,
                            args.require_both,
                            args.npi_only,
                            args.code_only,
                            max_bytes,
                        )] = next_entry
                    except StopIteration:
                        pass
                    break  # re-enter as_completed with updated futures

    total = len(already_matched) + matched_count
    if args.resume:
        log.info("Done. %d new matches. %d total in %s", matched_count, total, output_path)
    else:
        log.info("Done. %d matches written to %s", matched_count, output_path)
    log.info("Manifest written to %s", manifest_path)


if __name__ == "__main__":
    main()
