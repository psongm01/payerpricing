#!/usr/bin/env python
r"""
Create index_urls.txt from the UHC public MRF Azure Blob listing on Windows.

This is a standalone, standard-library-only version of
ingest/discover_index_urls.py. It uses one signed UHC public-MRF blob URL as a
seed, lists the monthly blob prefix, filters for index JSON files, and writes
one signed index URL per line.

Example PowerShell:

    python .\discover_index_urls_windows.py `
      --seed-url "https://mrfstorageprod.blob.core.windows.net/public-mrf/2026-04-01/2026-04-01_UnitedHealthcare-Insurance-Company_index.json?sv=..." `
      --month 2026-04 `
      --output .\data\raw\index_urls.txt `
      --manifest .\data\raw\index_urls_manifest.txt
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

TIMEOUT_SECONDS = 120


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

    base_url = urlunsplit((parts.scheme, parts.netloc, "", "", ""))
    sas_params = dict(parse_qsl(parts.query, keep_blank_values=True))
    if "sig" not in sas_params:
        raise ValueError("Seed URL does not appear to include a SAS token")

    return base_url, container, prefix, sas_params


def default_month_prefix(month: str) -> str:
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


def signed_blob_url(base_url: str, container: str, blob_name: str, sas_params: dict[str, str]) -> str:
    quoted_name = "/".join(quote(part, safe="") for part in blob_name.split("/"))
    return f"{base_url}/{container}/{quoted_name}?{urlencode(sas_params)}"


def is_index_blob(blob_name: str) -> bool:
    filename = Path(blob_name).name.lower()
    return "index" in filename and (filename.endswith(".json") or filename.endswith(".json.gz"))


def fetch_xml(url: str) -> ET.Element:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return ET.fromstring(response.read())


def iter_blobs(
    base_url: str,
    container: str,
    sas_params: dict[str, str],
    prefix: str,
):
    marker = ""
    while True:
        list_url = build_container_list_url(base_url, container, sas_params, prefix, marker)
        root = fetch_xml(list_url)

        for blob in root.findall("./Blobs/Blob"):
            name_node = blob.find("Name")
            size_node = blob.find("./Properties/Content-Length")
            if name_node is None or not name_node.text:
                continue
            size_bytes = int(size_node.text or 0) if size_node is not None else 0
            yield name_node.text, size_bytes

        marker_node = root.find("NextMarker")
        marker = marker_node.text if marker_node is not None and marker_node.text else ""
        if not marker:
            break


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create index_urls.txt from UHC public MRF Azure Blob listing."
    )
    parser.add_argument(
        "--seed-url",
        required=True,
        help="Any signed UHC public-mrf blob URL with list permission.",
    )
    parser.add_argument(
        "--month",
        required=True,
        help="Month as YYYY-MM. Default prefix becomes YYYY-MM-01/.",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Optional blob prefix override. Default: YYYY-MM-01/.",
    )
    parser.add_argument(
        "--output",
        default="index_urls.txt",
        help="Output text file. Default: index_urls.txt",
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help="Optional manifest file with url|size_bytes.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after this many URLs, useful for smoke tests.",
    )
    args = parser.parse_args()

    base_url, container, seed_prefix, sas_params = parse_seed_url(args.seed_url)
    prefix = args.prefix if args.prefix is not None else default_month_prefix(args.month)
    if not prefix:
        prefix = seed_prefix

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    manifest_path = Path(args.manifest) if args.manifest else None
    if manifest_path:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("Listing blobs from %s/%s with prefix %s", base_url, container, prefix)

    count = 0
    with output_path.open("w", encoding="utf-8") as output_fh:
        manifest_fh = manifest_path.open("w", encoding="utf-8") if manifest_path else None
        try:
            for blob_name, size_bytes in iter_blobs(base_url, container, sas_params, prefix):
                if not is_index_blob(blob_name):
                    continue
                url = signed_blob_url(base_url, container, blob_name, sas_params)
                output_fh.write(url + "\n")
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
        raise RuntimeError(f"No index URLs found under prefix {prefix}")


if __name__ == "__main__":
    main()
