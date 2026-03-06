"""
Ingest Azure Migrate exports (CSV and/or JSON) from inputs/azure_migrate/.
Normalizes server names and outputs a clean DataFrame to outputs/servers_clean.parquet.

Supported Azure Migrate column name variants for server/machine name:
    CSV:  "Machine Name", "Computer Name", "Server Name", "name", "server_name"
    JSON: "machineName", "name", "computerName", "serverName"
"""

import json
import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
INPUT_DIR = ROOT / "inputs" / "azure_migrate"
OUTPUT_FILE = ROOT / "outputs" / "servers_clean.parquet"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingest_azure_migrate")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Ordered list of candidate server-name column names (case-insensitive match)
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
]

NORMALIZED_SERVER_COL = "server_name_normalized"


# ---------------------------------------------------------------------------
# Helpers
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


def read_json_file(path: Path) -> pd.DataFrame:
    log.info("Reading JSON: %s", path)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        # Azure Migrate JSON can be a list of records or a dict with a key
        # containing the list (e.g. {"value": [...]} or {"machines": [...]}).
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # Try common wrapper keys first
            for key in ("value", "machines", "servers", "items", "data"):
                if key in data and isinstance(data[key], list):
                    log.info("  Unwrapping JSON key '%s'", key)
                    df = pd.DataFrame(data[key])
                    break
            else:
                # Flat dict — single record
                df = pd.DataFrame([data])
        else:
            raise ValueError(f"Unexpected JSON structure in {path}")

        log.info("  Rows: %d, Columns: %d", len(df), len(df.columns))
        return df
    except Exception as exc:
        log.error("Failed to read JSON %s: %s", path, exc)
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
    json_files = sorted(INPUT_DIR.glob("*.json"))
    all_files = csv_files + json_files

    if not all_files:
        log.warning("No CSV or JSON files found in %s", INPUT_DIR)
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

    for path in json_files:
        try:
            df = read_json_file(path)
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
