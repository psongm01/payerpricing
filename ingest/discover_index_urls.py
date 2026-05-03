"""
Discover TiC MRF index URLs.

Supported modes:
- UHC-style Azure Blob listing from one signed seed URL.
- Direct index URLs for payers that publish a single known table-of-contents
  index file, such as BCBSTX, Cigna, and Aetna.

The output is an index_urls.txt file consumed by stream_index.py. When
--manifest is provided, the script also writes url|size_bytes.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit
import xml.etree.ElementTree as ET

import requests
from urllib3.exceptions import InsecureRequestWarning

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

TIMEOUT = (15, 120)
KNOWN_BLOB_HOSTS = {
    "uhc-tic-mrf.azureedge.net": "mrfstorageprod.blob.core.windows.net",
}

HEADERS = {"User-Agent": "Mozilla/5.0"}
AETNA_PUBLIC_SERVICE_BASE = "https://health1.aetna.com/healthsparq/public/service"


def parse_seed_url(seed_url: str) -> tuple[str, str, str, dict[str, str]]:
    parts = urlsplit(seed_url)
    path_parts = [part for part in parts.path.split("/") if part]
    if len(path_parts) < 2:
        raise ValueError("Seed URL must include a container and blob path")
    container = path_parts[0]
    blob_path = "/".join(path_parts[1:])
    prefix = "/".join(blob_path.split("/")[:-1])
    if prefix:
        prefix += "/"
    netloc = KNOWN_BLOB_HOSTS.get(parts.netloc.lower(), parts.netloc)
    base_url = urlunsplit(("https", netloc, "", "", ""))
    sas_params = dict(parse_qsl(parts.query, keep_blank_values=True))
    return base_url, container, prefix, sas_params


def month_prefix(month: str) -> str:
    return f"{month}-01/"


def build_container_list_url(
    base_url: str,
    container: str,
    sas_params: dict[str, str],
    prefix: str,
    marker: str = "",
) -> str:
    params = {
        **sas_params,
        "restype": "container",
        "comp": "list",
        "prefix": prefix,
    }
    if marker:
        params["marker"] = marker
    return f"{base_url}/{container}?{urlencode(params)}"


def blob_url(base_url: str, container: str, blob_name: str, sas_params: dict[str, str]) -> str:
    quoted_name = "/".join(requests.utils.quote(part, safe="") for part in blob_name.split("/"))
    return f"{base_url}/{container}/{quoted_name}?{urlencode(sas_params)}"


def is_index_blob(name: str) -> bool:
    filename = Path(name).name.lower()
    return "index" in filename and (filename.endswith(".json") or filename.endswith(".json.gz"))


def _local_path_from_url(url: str) -> Path | None:
    parts = urlsplit(url)
    if parts.scheme == "file":
        return Path(unquote(parts.path.lstrip("/")))
    if len(parts.scheme) == 1 and url[1:3] in (":\\", ":/"):
        return Path(url)
    if not parts.scheme:
        return Path(url)
    return None


def get_index_size_bytes(url: str, verify_tls: bool = True) -> int:
    local_path = _local_path_from_url(url)
    if local_path is not None:
        return local_path.stat().st_size if local_path.exists() else 0

    try:
        response = requests.head(
            url,
            timeout=TIMEOUT,
            headers=HEADERS,
            allow_redirects=True,
            verify=verify_tls,
        )
        response.raise_for_status()
        content_length = response.headers.get("Content-Length")
        if content_length:
            return int(content_length)
        etag = response.headers.get("ETag", "")
        # Some CDNs omit Content-Length after compression negotiation but expose
        # the origin size as the final hex component of weak ETags.
        etag_match = etag.rstrip('"').rsplit("-", 1)
        if len(etag_match) == 2:
            try:
                return int(etag_match[1], 16)
            except ValueError:
                pass
    except requests.RequestException as exc:
        log.warning("Could not fetch index size with HEAD: %s | %s", url[:100], exc)
    except ValueError as exc:
        log.warning("Invalid Content-Length for index URL: %s | %s", url[:100], exc)
    return 0


def iter_direct_index_urls(urls: list[str], keep_non_index: bool, verify_tls: bool):
    for url in urls:
        url = url.strip()
        if not url:
            continue
        if not keep_non_index and not is_index_blob(urlsplit(url).path or url):
            log.warning("Skipping URL that does not look like an index file: %s", url)
            continue
        yield url, get_index_size_bytes(url, verify_tls=verify_tls)


def load_url_list(path: Path) -> list[str]:
    with path.open(encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip()]


def _aetna_login(
    session: requests.Session,
    insurer_code: str,
    brand_code: str,
    verify_tls: bool,
) -> None:
    params = {
        "_": "0",
        "insurerCode": insurer_code,
        "brandCode": brand_code,
    }
    response = session.get(
        f"{AETNA_PUBLIC_SERVICE_BASE}/login",
        params=params,
        timeout=TIMEOUT,
        headers=HEADERS,
        verify=verify_tls,
    )
    response.raise_for_status()


def _aetna_metadata_url(
    session: requests.Session,
    insurer_code: str,
    brand_code: str,
    verify_tls: bool,
) -> str:
    response = session.post(
        f"{AETNA_PUBLIC_SERVICE_BASE}/v2/mrf/all",
        json={"insurerCode": insurer_code, "brandCode": brand_code},
        timeout=TIMEOUT,
        headers=HEADERS,
        verify=verify_tls,
    )
    response.raise_for_status()
    data = response.json()
    metadata_url = data.get("url")
    if not metadata_url:
        raise RuntimeError(f"Aetna MRF catalog did not return a metadata URL: {data}")
    return metadata_url


def iter_aetna_index_urls(
    insurer_code: str,
    brand_code: str,
    file_schema: str = "TABLE_OF_CONTENTS",
    file_month: str | None = None,
    verify_tls: bool = True,
):
    session = requests.Session()
    session.headers.update(HEADERS)
    _aetna_login(session, insurer_code, brand_code, verify_tls)
    metadata_url = _aetna_metadata_url(session, insurer_code, brand_code, verify_tls)
    log.info("Reading Aetna metadata catalog: %s", metadata_url)

    response = session.get(metadata_url, timeout=TIMEOUT, verify=verify_tls)
    response.raise_for_status()
    data = response.json()
    files = data.get("files") or []

    base_url = metadata_url.rsplit("/", 1)[0]
    seen: set[str] = set()
    target_schema = file_schema.upper()
    for row in files:
        if str(row.get("fileSchema", "")).upper() != target_schema:
            continue
        if file_month:
            last_updated = str(row.get("lastUpdatedOn") or "")
            file_path_for_month = str(row.get("filePath") or "")
            if not (last_updated.startswith(file_month) or file_path_for_month.startswith(file_month)):
                continue
        file_path = row.get("filePath")
        if not file_path:
            continue
        url = f"{base_url}/{file_path.lstrip('/')}"
        if url in seen:
            continue
        seen.add(url)
        yield url, get_index_size_bytes(url, verify_tls=verify_tls)


def iter_blobs(
    base_url: str,
    container: str,
    sas_params: dict[str, str],
    prefix: str,
):
    marker = ""
    while True:
        list_url = build_container_list_url(base_url, container, sas_params, prefix, marker)
        response = requests.get(list_url, timeout=TIMEOUT, headers=HEADERS)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        for blob in root.findall("./Blobs/Blob"):
            name_node = blob.find("Name")
            size_node = blob.find("./Properties/Content-Length")
            if name_node is None or not name_node.text:
                continue
            yield name_node.text, int(size_node.text or 0) if size_node is not None else 0

        marker_node = root.find("NextMarker")
        marker = marker_node.text if marker_node is not None and marker_node.text else ""
        if not marker:
            break


def main() -> None:
    parser = argparse.ArgumentParser(description="Build index_urls.txt for TiC MRF index files.")
    parser.add_argument(
        "--seed-url",
        default=None,
        help="Any signed UHC public-mrf blob URL for the target storage account/container.",
    )
    parser.add_argument(
        "--index-url",
        action="append",
        default=[],
        help=(
            "Direct index URL. Repeat for multiple payers/files. Use this for "
            "BCBSTX, Cigna, Aetna, or any payer with known index URLs."
        ),
    )
    parser.add_argument(
        "--index-url-list",
        default=None,
        help="Optional text file containing direct index URLs, one per line.",
    )
    parser.add_argument(
        "--aetna-insurer-code",
        default=None,
        help="Discover Aetna/HealthSparq MRF index URLs for this insurer code, e.g. AETNACVS_I.",
    )
    parser.add_argument(
        "--aetna-brand-code",
        default=None,
        help="Discover Aetna/HealthSparq MRF index URLs for this brand code, e.g. ALICSI.",
    )
    parser.add_argument(
        "--aetna-file-schema",
        default="TABLE_OF_CONTENTS",
        help="Aetna metadata fileSchema to emit. Default: TABLE_OF_CONTENTS.",
    )
    parser.add_argument(
        "--aetna-file-month",
        default=None,
        help="Optional Aetna metadata month filter as YYYY-MM, matched against lastUpdatedOn or filePath.",
    )
    parser.add_argument(
        "--month",
        default=None,
        help="Month as YYYY-MM. The default blob prefix becomes YYYY-MM-01/.",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Override blob prefix. Default: YYYY-MM-01/.",
    )
    parser.add_argument(
        "--output",
        default="index_urls.txt",
        help="Output text file with one index URL per line.",
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help="Optional manifest output with url|size_bytes.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after this many index URLs, useful for smoke tests.",
    )
    parser.add_argument(
        "--keep-non-index",
        action="store_true",
        help="Keep direct URLs even when the filename does not contain index.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification for discovery HTTP requests.",
    )
    args = parser.parse_args()
    verify_tls = not args.insecure
    if args.insecure:
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

    direct_urls = list(args.index_url)
    if args.index_url_list:
        direct_urls.extend(load_url_list(Path(args.index_url_list)))
    aetna_mode = bool(args.aetna_insurer_code or args.aetna_brand_code)

    mode_count = sum(bool(value) for value in (direct_urls, args.seed_url, aetna_mode))
    if mode_count > 1:
        parser.error("Use only one discovery mode: --seed-url, direct --index-url/--index-url-list, or --aetna-insurer-code/--aetna-brand-code.")

    if mode_count == 0:
        parser.error("Provide --seed-url, --index-url, --index-url-list, or Aetna insurer/brand arguments.")

    if args.seed_url and not args.month and args.prefix is None:
        parser.error("--month is required with --seed-url unless --prefix is provided.")

    if aetna_mode and not (args.aetna_insurer_code and args.aetna_brand_code):
        parser.error("Aetna discovery requires both --aetna-insurer-code and --aetna-brand-code.")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest) if args.manifest else None
    if manifest_path:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

    if direct_urls:
        log.info("Writing direct index URL(s)")
        index_rows = iter_direct_index_urls(direct_urls, args.keep_non_index, verify_tls)
        empty_message = "No direct index URLs written"
    elif aetna_mode:
        log.info(
            "Discovering Aetna index URLs: insurer=%s brand=%s schema=%s",
            args.aetna_insurer_code,
            args.aetna_brand_code,
            args.aetna_file_schema,
        )
        index_rows = iter_aetna_index_urls(
            args.aetna_insurer_code,
            args.aetna_brand_code,
            args.aetna_file_schema,
            args.aetna_file_month,
            verify_tls,
        )
        empty_message = "No Aetna index URLs discovered"
    else:
        base_url, container, seed_prefix, sas_params = parse_seed_url(args.seed_url)
        prefix = args.prefix if args.prefix is not None else month_prefix(args.month)
        if not prefix:
            prefix = seed_prefix

        log.info("Listing blobs: account=%s container=%s prefix=%s", base_url, container, prefix)
        index_rows = (
            (blob_url(base_url, container, name, sas_params), size_bytes)
            for name, size_bytes in iter_blobs(base_url, container, sas_params, prefix)
            if is_index_blob(name)
        )
        empty_message = f"No index URLs found under prefix {prefix}"

    count = 0
    with output_path.open("w", encoding="utf-8") as out_fh:
        manifest_fh = manifest_path.open("w", encoding="utf-8") if manifest_path else None
        try:
            for url, size_bytes in index_rows:
                out_fh.write(url + "\n")
                if manifest_fh:
                    manifest_fh.write(f"{url}|{size_bytes}\n")
                count += 1
                if args.limit and count >= args.limit:
                    break
        finally:
            if manifest_fh:
                manifest_fh.close()

    log.info("Wrote %s index URL(s) to %s", f"{count:,}", output_path)
    if count == 0:
        raise RuntimeError(empty_message)


if __name__ == "__main__":
    main()
