"""Build byte-balanced worker shard files from matched URL manifests."""

from __future__ import annotations

import argparse
import heapq
import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ManifestRow:
    url: str
    size_bytes: int
    raw_line: str


def parse_manifest(path: Path) -> list[ManifestRow]:
    rows: list[ManifestRow] = []
    with path.open(encoding="utf-8") as fh:
        for line_number, raw in enumerate(fh, start=1):
            raw_line = raw.rstrip("\n")
            if not raw_line.strip():
                continue
            parts = raw_line.split("|", 3)
            if len(parts) < 2:
                raise ValueError(f"Manifest line {line_number} is missing size_bytes: {raw_line[:120]}")
            try:
                size_bytes = int(parts[1] or 0)
            except ValueError as exc:
                raise ValueError(f"Manifest line {line_number} has invalid size_bytes: {parts[1]!r}") from exc
            rows.append(ManifestRow(url=parts[0], size_bytes=size_bytes, raw_line=raw_line))
    return rows


def build_shards(rows: list[ManifestRow], shard_count: int) -> list[list[ManifestRow]]:
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    heap: list[tuple[int, int, list[ManifestRow]]] = [(0, idx, []) for idx in range(shard_count)]
    heapq.heapify(heap)
    for row in sorted(rows, key=lambda item: item.size_bytes, reverse=True):
        total, idx, shard_rows = heapq.heappop(heap)
        shard_rows.append(row)
        heapq.heappush(heap, (total + row.size_bytes, idx, shard_rows))
    by_index = sorted(heap, key=lambda item: item[1])
    return [item[2] for item in by_index]


def write_shards(manifest_path: Path, output_dir: Path, shard_count: int = 10) -> list[Path]:
    rows = parse_manifest(manifest_path)
    shards = build_shards(rows, shard_count)
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for idx, shard_rows in enumerate(shards, start=1):
        path = output_dir / f"shard_{idx:02d}.txt"
        with path.open("w", encoding="utf-8") as fh:
            for row in shard_rows:
                fh.write(row.raw_line + "\n")
        total_bytes = sum(row.size_bytes for row in shard_rows)
        log.info("Wrote %s | files=%d | bytes=%s", path, len(shard_rows), f"{total_bytes:,}")
        written.append(path)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Create byte-balanced monthly shard files.")
    parser.add_argument("--manifest", default="data/raw/matched_pricing_urls_manifest.txt")
    parser.add_argument("--output-dir", default="data/raw/shards")
    parser.add_argument("--shards", type=int, default=10)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    write_shards(Path(args.manifest), Path(args.output_dir), args.shards)


if __name__ == "__main__":
    main()
