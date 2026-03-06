"""
Ingest Device42 inventory exports from inputs/device42/.
Reads CSV or Excel (.xlsx / .xls) files, normalizes server names using
identical logic to ingest_azure_migrate.py, and outputs
outputs/inventory_clean.parquet.

Supported Device42 column name variants for server/machine name:
    "name", "device_name", "Device Name", "hostname", "Host Name",
    "server_name", "Server Name", "Computer Name", "Asset Name"
"""

import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
INPUT_DIR = ROOT / "inputs" / "device42"
OUTPUT_FILE = ROOT / "outputs" / "inventory_clean.parquet"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingest_inventory")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Ordered list of candidate server-name column names (case-insensitive match).
# Must be identical logic to ingest_azure_migrate.py.
SERVER_NAME_CANDIDATES = [
    "machine name",
    "computer name",
    "server name",
    "name",
    "server_name",
    "machinename",
    "computername",
    "servername",
    "hostname",
    "host name",
    "device name",
    "device_name",
    "asset name",
    "asset_name",
]

NORMALIZED_SERVER_COL = "server_name_normalized"


# ---------------------------------------------------------------------------
# Helpers  (identical normalization logic to ingest_azure_migrate.py)
# ---------------------------------------------------------------------------

def normalize_server_name(series: pd.Series) -> pd.Series:
    """Lowercase and strip whitespace from a server-name Series."""
    return series.astype(str).str.strip().str.lower()


def detect_server_name_col(columns: list[str]) -> str:
    """
    Return the first column whose lower-stripped name matches a known candidate.
    Raises ValueError if none found.
    """
    col_map = {c.strip().lower(): c for c in columns}
    for candidate in SERVER_NAME_CANDIDATES:
        if candidate in col_map:
            log.info("Detected server name column: '%s'", col_map[candidate])
            return col_map[candidate]
    raise ValueError(
        f"Could not find a server name column. "
        f"Tried: {SERVER_NAME_CANDIDATES}. "
        f"Available columns: {list(columns)}"
    )


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def read_csv_file(path: Path) -> pd.DataFrame:
    log.info("Reading CSV: %s", path)
    try:
        df = pd.read_csv(path, dtype=str)
        log.info("  Rows: %d, Columns: %d", len(df), len(df.columns))
        return df
    except Exception as exc:
        log.error("Failed to read CSV %s: %s", path, exc)
        raise


def read_excel_file(path: Path) -> pd.DataFrame:
    """
    Read an Excel workbook. If there is a sheet named 'inventory' (case-
    insensitive), use it; otherwise use the first sheet.
    """
    log.info("Reading Excel: %s", path)
    try:
        xl = pd.ExcelFile(path)
        sheet_names_lower = {s.strip().lower(): s for s in xl.sheet_names}
        target_sheet = sheet_names_lower.get("inventory", xl.sheet_names[0])
        log.info("  Using sheet: '%s'", target_sheet)
        df = xl.parse(target_sheet, dtype=str)
        log.info("  Rows: %d, Columns: %d", len(df), len(df.columns))
        return df
    except Exception as exc:
        log.error("Failed to read Excel %s: %s", path, exc)
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ingest() -> pd.DataFrame:
    if not INPUT_DIR.exists():
        log.warning("Input directory does not exist: %s", INPUT_DIR)
        log.warning("Returning empty DataFrame.")
        return pd.DataFrame()

    csv_files = sorted(INPUT_DIR.glob("*.csv"))
    xlsx_files = sorted(INPUT_DIR.glob("*.xlsx"))
    xls_files = sorted(INPUT_DIR.glob("*.xls"))
    all_files = csv_files + xlsx_files + xls_files

    if not all_files:
        log.warning("No CSV or Excel files found in %s", INPUT_DIR)
        log.warning("Returning empty DataFrame.")
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []

    for path in csv_files:
        try:
            df = read_csv_file(path)
            col = detect_server_name_col(df.columns.tolist())
            df[NORMALIZED_SERVER_COL] = normalize_server_name(df[col])
            df["_source_file"] = path.name
            frames.append(df)
        except Exception as exc:
            log.error("Skipping %s due to error: %s", path.name, exc)

    for path in xlsx_files + xls_files:
        try:
            df = read_excel_file(path)
            col = detect_server_name_col(df.columns.tolist())
            df[NORMALIZED_SERVER_COL] = normalize_server_name(df[col])
            df["_source_file"] = path.name
            frames.append(df)
        except Exception as exc:
            log.error("Skipping %s due to error: %s", path.name, exc)

    if not frames:
        log.error("All input files failed to load. Returning empty DataFrame.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info("Combined rows: %d", len(combined))

    # Deduplicate on normalized server name, keeping first occurrence
    before = len(combined)
    combined = combined.drop_duplicates(subset=[NORMALIZED_SERVER_COL])
    dropped = before - len(combined)
    if dropped:
        log.info("Dropped %d duplicate server name rows.", dropped)

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUTPUT_FILE, index=False)
    log.info("Wrote %d rows to %s", len(combined), OUTPUT_FILE)

    return combined


if __name__ == "__main__":
    result = ingest()
    if result.empty:
        log.warning("Output is empty — no data was written.")
        sys.exit(1)
    log.info("Done.")
