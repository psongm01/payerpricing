"""Export distinct NPIs from nppes_provider parquet to a text file."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser(description="Export distinct NPIs from parquet to one-NPI-per-line text.")
    parser.add_argument("--nppes", required=True, help="Directory containing nppes_provider parquet files.")
    parser.add_argument("--output", required=True, help="Output text file.")
    args = parser.parse_args()

    nppes_dir = Path(args.nppes)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_glob = (nppes_dir / "*.parquet").as_posix()

    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT DISTINCT npi
            FROM read_parquet('{parquet_glob}')
            WHERE npi IS NOT NULL AND npi != ''
            ORDER BY npi
            """
        ).fetchall()
    finally:
        con.close()

    with output_path.open("w", encoding="utf-8") as fh:
        for (npi,) in rows:
            fh.write(f"{npi}\n")

    print(f"Wrote {len(rows):,} NPIs to {output_path}")


if __name__ == "__main__":
    main()
