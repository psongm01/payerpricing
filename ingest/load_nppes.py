"""
load_nppes.py

Loads the NPPES dissemination CSV and NUCC taxonomy crosswalk into a filtered
Parquet file for use as a provider lookup when scanning TiC pricing files.

Workflow
--------
1. Download NPPES zip from:
     https://download.cms.gov/nppes/NPI_Files.html
   Extract the main CSV (e.g. npidata_pfile_20050523-20260413.csv) — do NOT
   load it whole; this script streams it row by row.

2. Download the NUCC taxonomy CSV from:
     https://nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57
   Save as data/raw/nppes/nucc_taxonomy.csv

3. Run:
     python load_nppes.py \\
       --nppes    data/raw/nppes/npidata_pfile_*.csv \\
       --taxonomy data/raw/nppes/nucc_taxonomy.csv \\
       --output   data/parquet/nppes_provider/ \\
       [--states TX] \\
       [--specialties Neurology "Internal Medicine"]

   Without --states or --specialties, all active providers are written.
   With filters, only matching providers are written — much smaller output,
   faster to join against TiC data.

4. The output Parquet is then used by scan_pricing_urls.py to find which
   pricing file URLs in plan_pricing_bridge contain relevant providers,
   before downloading any TiC files.

Output schema
-------------
  npi                     string   10-digit NPI
  entity_type             string   "Individual" or "Organization"
  org_name                string   legal business name (Type 2)
  last_name               string   (Type 1)
  first_name              string   (Type 1)
  practice_state          string   2-letter state code (practice location)
  mailing_state           string   2-letter state code (mailing address)
  taxonomy_code           string   primary taxonomy code
  taxonomy_grouping       string   e.g. "Allopathic & Osteopathic Physicians"
  taxonomy_classification string   e.g. "Neurology"
  taxonomy_specialization string   e.g. "Neurocritical Care" (may be empty)
  display_name            string   human-readable label from NUCC
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

SCHEMA = pa.schema([
    pa.field("npi",                     pa.string()),
    pa.field("entity_type",             pa.string()),
    pa.field("org_name",                pa.string()),
    pa.field("last_name",               pa.string()),
    pa.field("first_name",              pa.string()),
    pa.field("practice_state",          pa.string()),
    pa.field("mailing_state",           pa.string()),
    pa.field("taxonomy_code",           pa.string()),
    pa.field("taxonomy_grouping",       pa.string()),
    pa.field("taxonomy_classification", pa.string()),
    pa.field("taxonomy_specialization", pa.string()),
    pa.field("display_name",            pa.string()),
])

BATCH_SIZE = 50_000


# ---------------------------------------------------------------------------
# NUCC taxonomy crosswalk loader
# ---------------------------------------------------------------------------

def load_taxonomy_crosswalk(taxonomy_csv: Path | None) -> dict:
    """
    Returns a dict keyed by taxonomy code:
      { "2084N0400X": {
          "grouping": "Allopathic & Osteopathic Physicians",
          "classification": "Neurology",
          "specialization": "",
          "display_name": "Neurologist"
        }, ... }

    NUCC CSV columns (version 25.1+):
      Code, Grouping, Classification, Specialization, Definition, Notes,
      Display Name, Section
    """
    if taxonomy_csv is None:
        log.info("No taxonomy crosswalk provided; taxonomy fields will be blank")
        return {}

    crosswalk = {}
    with taxonomy_csv.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("Code", "").strip()
            if not code:
                continue
            crosswalk[code] = {
                "grouping":       row.get("Grouping", "").strip(),
                "classification": row.get("Classification", "").strip(),
                "specialization": row.get("Specialization", "").strip(),
                "display_name":   row.get("Display Name", "").strip(),
            }
    log.info("Loaded %d taxonomy codes from crosswalk", len(crosswalk))
    return crosswalk


# ---------------------------------------------------------------------------
# NPPES column helpers
# The NPPES CSV has taxonomy slots _1 through _15, each with:
#   Healthcare Provider Taxonomy Code_N
#   Healthcare Provider Primary Taxonomy Switch_N   ("Y" or "X" for primary)
# ---------------------------------------------------------------------------

TAXONOMY_SLOTS = range(1, 16)


def get_primary_taxonomy(row: dict) -> str:
    """Return the taxonomy code marked as primary (Switch = 'Y' or 'X')."""
    for n in TAXONOMY_SLOTS:
        switch = row.get(f"Healthcare Provider Primary Taxonomy Switch_{n}", "").strip()
        if switch in ("Y", "X"):
            code = row.get(f"Healthcare Provider Taxonomy Code_{n}", "").strip()
            if code:
                return code
    # Fall back to slot 1 if nothing is flagged primary
    return row.get("Healthcare Provider Taxonomy Code_1", "").strip()


def matches_specialties(taxonomy_info: dict, specialty_filters: list[str]) -> bool:
    """
    True if any of the specialty_filters appear (case-insensitive) in the
    taxonomy grouping, classification, specialization, or display_name.
    """
    if not specialty_filters:
        return True
    haystack = " | ".join([
        taxonomy_info.get("grouping", ""),
        taxonomy_info.get("classification", ""),
        taxonomy_info.get("specialization", ""),
        taxonomy_info.get("display_name", ""),
    ]).lower()
    return any(s.lower() in haystack for s in specialty_filters)


# ---------------------------------------------------------------------------
# Main streaming loader
# ---------------------------------------------------------------------------

def load_nppes(
    nppes_csv: Path,
    taxonomy_crosswalk: dict,
    output_dir: Path,
    state_filters: list[str],
    specialty_filters: list[str],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "nppes_provider.parquet"

    # Normalise state filters to uppercase 2-letter codes
    state_filters_upper = [s.upper().strip() for s in state_filters]

    writer = None
    batch = []
    total_read = 0
    total_written = 0

    log.info("Streaming NPPES file: %s", nppes_csv)
    log.info("State filter:    %s", state_filters_upper or "ALL")
    log.info("Specialty filter: %s", specialty_filters or "ALL")

    with nppes_csv.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_read += 1

            if total_read % 500_000 == 0:
                log.info("  Read %s rows, kept %s so far", f"{total_read:,}", f"{total_written:,}")

            # Skip deactivated — deactivated rows only have NPI + deactivation date
            if row.get("NPI Deactivation Date", "").strip():
                continue

            # --- State filter ---
            # Use practice location state preferentially; fall back to mailing
            practice_state = row.get(
                "Provider Business Practice Location Address State Name", ""
            ).strip().upper()
            mailing_state = row.get(
                "Provider Business Mailing Address State Name", ""
            ).strip().upper()
            effective_state = practice_state or mailing_state

            if state_filters_upper and effective_state not in state_filters_upper:
                continue

            # --- Taxonomy / specialty filter ---
            taxonomy_code = get_primary_taxonomy(row)
            taxonomy_info = taxonomy_crosswalk.get(taxonomy_code, {
                "grouping": "", "classification": "",
                "specialization": "", "display_name": "",
            })

            if specialty_filters and not matches_specialties(taxonomy_info, specialty_filters):
                continue

            # --- Entity type ---
            entity_type_code = row.get("Entity Type Code", "").strip()
            entity_type = "Individual" if entity_type_code == "1" else "Organization"

            record = {
                "npi":                     row.get("NPI", "").strip(),
                "entity_type":             entity_type,
                "org_name":                row.get("Provider Organization Name (Legal Business Name)", "").strip(),
                "last_name":               row.get("Provider Last Name (Legal Name)", "").strip(),
                "first_name":              row.get("Provider First Name", "").strip(),
                "practice_state":          practice_state,
                "mailing_state":           mailing_state,
                "taxonomy_code":           taxonomy_code,
                "taxonomy_grouping":       taxonomy_info.get("grouping", ""),
                "taxonomy_classification": taxonomy_info.get("classification", ""),
                "taxonomy_specialization": taxonomy_info.get("specialization", ""),
                "display_name":            taxonomy_info.get("display_name", ""),
            }

            batch.append(record)
            total_written += 1

            if len(batch) >= BATCH_SIZE:
                table = pa.Table.from_pylist(batch, schema=SCHEMA)
                if writer is None:
                    writer = pq.ParquetWriter(output_path, schema=SCHEMA)
                writer.write_table(table)
                batch.clear()

    # Flush remaining
    if batch:
        table = pa.Table.from_pylist(batch, schema=SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(output_path, schema=SCHEMA)
        writer.write_table(table)

    if writer:
        writer.close()

    log.info("Done. Read %s rows, wrote %s providers to %s",
             f"{total_read:,}", f"{total_written:,}", output_path)

    if total_written == 0:
        log.warning("No providers matched your filters — check state/specialty values.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Stream NPPES dissemination CSV into a filtered Parquet provider lookup. "
            "Use --states and --specialties to pre-filter before TiC scanning."
        )
    )
    parser.add_argument(
        "--nppes",
        required=True,
        help="Path to the extracted NPPES CSV (e.g. npidata_pfile_*.csv)",
    )
    parser.add_argument(
        "--taxonomy",
        default=None,
        help="Optional path to NUCC taxonomy CSV (nucc_taxonomy.csv)",
    )
    parser.add_argument(
        "--output",
        default="data/parquet/nppes_provider/",
        help="Output directory for nppes_provider.parquet (default: data/parquet/nppes_provider/)",
    )
    parser.add_argument(
        "--states",
        nargs="*",
        default=[],
        help="2-letter state codes to include, e.g. --states TX CA. Omit for all states.",
    )
    parser.add_argument(
        "--specialties",
        nargs="*",
        default=[],
        help=(
            "Specialty keyword(s) to match against NUCC taxonomy fields. "
            "Case-insensitive substring match. "
            "e.g. --specialties Neurology 'Internal Medicine'. Omit for all specialties."
        ),
    )
    args = parser.parse_args()

    nppes_csv = Path(args.nppes)
    if not nppes_csv.exists():
        log.error("NPPES file not found: %s", nppes_csv)
        sys.exit(1)

    taxonomy_csv = Path(args.taxonomy) if args.taxonomy else None
    if taxonomy_csv is not None and not taxonomy_csv.exists():
        log.error("Taxonomy file not found: %s", taxonomy_csv)
        sys.exit(1)

    taxonomy_crosswalk = load_taxonomy_crosswalk(taxonomy_csv)

    load_nppes(
        nppes_csv=nppes_csv,
        taxonomy_crosswalk=taxonomy_crosswalk,
        output_dir=Path(args.output),
        state_filters=args.states,
        specialty_filters=args.specialties,
    )


if __name__ == "__main__":
    main()
