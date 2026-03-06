"""
Build application-to-server mapping by joining Azure Migrate servers
(outputs/servers_clean.parquet) against the Device42 inventory
(outputs/inventory_clean.parquet).

Join strategy:
  1. Exact match on server_name_normalized.
  2. For servers still unmatched, fuzzy match via rapidfuzz (WRatio scorer).
     Matches below FUZZY_THRESHOLD are recorded as unmatched.

Output columns in outputs/app_server_mapping.parquet:
    server_name          – normalized server name from Azure Migrate
    application_name     – from Device42 inventory (null if not found / no app col)
    match_type           – "exact" | "fuzzy" | "none"
    match_confidence     – 1.0 (exact), 0.0–1.0 (fuzzy score / 100), 0.0 (none)
    matched_inventory_name – inventory server name that was matched (diagnostic)

A summary report is written to outputs/summaries/match_report.md.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
SERVERS_FILE = ROOT / "outputs" / "servers_clean.parquet"
INVENTORY_FILE = ROOT / "outputs" / "inventory_clean.parquet"
MAPPING_FILE = ROOT / "outputs" / "app_server_mapping.parquet"
REPORT_FILE = ROOT / "outputs" / "summaries" / "match_report.md"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("build_app_server_mapping")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
NORMALIZED_COL = "server_name_normalized"
FUZZY_THRESHOLD = 80.0  # minimum rapidfuzz WRatio score (0–100) to accept

# Candidate column names for the application field in the inventory,
# checked in priority order (case-insensitive).
APP_NAME_CANDIDATES = [
    "application_name",
    "application",
    "app_name",
    "app",
    "business_application",
    "service_name",
    "service",
    "workload",
    "workload_name",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        log.error("Required file not found: %s", path)
        log.error("Run the relevant ingest script first.")
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
        log.info("Loaded %s: %d rows from %s", label, len(df), path)
        return df
    except Exception as exc:
        log.error("Failed to read %s (%s): %s", label, path, exc)
        return pd.DataFrame()


def detect_app_col(columns: list[str]) -> str | None:
    """Return the first matching application column name, or None."""
    col_map = {c.strip().lower(): c for c in columns}
    for candidate in APP_NAME_CANDIDATES:
        if candidate in col_map:
            col = col_map[candidate]
            log.info("Detected application name column: '%s'", col)
            return col
    log.warning(
        "No application name column found in inventory. "
        "Tried: %s. Available: %s",
        APP_NAME_CANDIDATES,
        list(columns),
    )
    return None


def _validate_normalized_col(df: pd.DataFrame, label: str) -> bool:
    if NORMALIZED_COL not in df.columns:
        log.error(
            "'%s' column missing from %s. "
            "Re-run the ingest script to regenerate the parquet.",
            NORMALIZED_COL,
            label,
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def exact_join(
    servers: pd.DataFrame,
    inventory: pd.DataFrame,
    app_col: str | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Left-merge servers onto inventory on NORMALIZED_COL.

    Returns:
        matched   – rows from servers that found an exact inventory entry
        unmatched – rows from servers with no exact match
    """
    inv_cols = [NORMALIZED_COL] + ([app_col] if app_col else [])
    inv_slim = inventory[inv_cols].drop_duplicates(subset=[NORMALIZED_COL])

    merged = servers[[NORMALIZED_COL]].merge(
        inv_slim,
        on=NORMALIZED_COL,
        how="left",
        indicator=True,
    )

    matched_mask = merged["_merge"] == "both"
    matched = merged[matched_mask].drop(columns=["_merge"]).copy()
    unmatched = servers[[NORMALIZED_COL]][~matched_mask.values].copy()

    log.info(
        "Exact match: %d matched, %d unmatched (of %d total)",
        len(matched),
        len(unmatched),
        len(servers),
    )
    return matched, unmatched


def fuzzy_match(
    unmatched_names: list[str],
    inventory_names: list[str],
    app_lookup: dict[str, str],  # inventory_name -> application_name
    threshold: float,
) -> pd.DataFrame:
    """
    For each name in unmatched_names, find the best match in inventory_names
    using rapidfuzz WRatio. Returns a DataFrame with columns:
        server_name, matched_inventory_name, application_name, match_confidence
    """
    try:
        from rapidfuzz import fuzz, process as rf_process
    except ImportError:
        log.error(
            "rapidfuzz is not installed. Fuzzy matching skipped. "
            "Install with: pip install rapidfuzz"
        )
        return pd.DataFrame(
            columns=[
                "server_name",
                "matched_inventory_name",
                "application_name",
                "match_confidence",
            ]
        )

    if not inventory_names:
        log.warning("Inventory name list is empty; skipping fuzzy match.")
        return pd.DataFrame(
            columns=[
                "server_name",
                "matched_inventory_name",
                "application_name",
                "match_confidence",
            ]
        )

    rows = []
    for name in unmatched_names:
        result = rf_process.extractOne(
            name,
            inventory_names,
            scorer=fuzz.WRatio,
            score_cutoff=threshold,
        )
        if result is not None:
            best_match, score, _ = result
            rows.append(
                {
                    "server_name": name,
                    "matched_inventory_name": best_match,
                    "application_name": app_lookup.get(best_match),
                    "match_confidence": round(score / 100.0, 4),
                }
            )
        else:
            rows.append(
                {
                    "server_name": name,
                    "matched_inventory_name": None,
                    "application_name": None,
                    "match_confidence": 0.0,
                }
            )

    df = pd.DataFrame(rows)
    fuzzy_hits = (df["matched_inventory_name"].notna()).sum()
    log.info(
        "Fuzzy match (threshold=%.0f): %d matched, %d unmatched (of %d candidates)",
        threshold,
        fuzzy_hits,
        len(df) - fuzzy_hits,
        len(df),
    )
    return df


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def build_mapping(servers: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    app_col = detect_app_col(inventory.columns.tolist())

    # --- Exact join ---
    matched_exact, unmatched = exact_join(servers, inventory, app_col)
    matched_exact = matched_exact.rename(
        columns={NORMALIZED_COL: "server_name", app_col: "application_name"}
        if app_col
        else {NORMALIZED_COL: "server_name"}
    )
    if "application_name" not in matched_exact.columns:
        matched_exact["application_name"] = None
    matched_exact["match_type"] = "exact"
    matched_exact["match_confidence"] = 1.0
    matched_exact["matched_inventory_name"] = matched_exact["server_name"]

    # --- Fuzzy join for unmatched ---
    inventory_names = inventory[NORMALIZED_COL].dropna().tolist()
    app_lookup: dict[str, str] = {}
    if app_col:
        app_lookup = (
            inventory[[NORMALIZED_COL, app_col]]
            .dropna(subset=[NORMALIZED_COL])
            .drop_duplicates(subset=[NORMALIZED_COL])
            .set_index(NORMALIZED_COL)[app_col]
            .to_dict()
        )

    fuzzy_df = fuzzy_match(
        unmatched_names=unmatched[NORMALIZED_COL].tolist(),
        inventory_names=inventory_names,
        app_lookup=app_lookup,
        threshold=FUZZY_THRESHOLD,
    )

    fuzzy_matched = fuzzy_df[fuzzy_df["matched_inventory_name"].notna()].copy()
    fuzzy_matched["match_type"] = "fuzzy"

    fuzzy_unmatched = fuzzy_df[fuzzy_df["matched_inventory_name"].isna()].copy()
    fuzzy_unmatched["match_type"] = "none"

    # --- Combine ---
    mapping = pd.concat(
        [matched_exact, fuzzy_matched, fuzzy_unmatched],
        ignore_index=True,
        sort=False,
    )

    # Ensure required columns are present
    for col in ["server_name", "application_name", "match_type", "match_confidence",
                "matched_inventory_name"]:
        if col not in mapping.columns:
            mapping[col] = None

    # Final column order
    mapping = mapping[
        ["server_name", "application_name", "match_type", "match_confidence",
         "matched_inventory_name"]
    ]

    return mapping


# ---------------------------------------------------------------------------
# Match report
# ---------------------------------------------------------------------------

def write_match_report(
    mapping: pd.DataFrame,
    n_servers: int,
    n_inventory: int,
    report_path: Path,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)

    exact = mapping[mapping["match_type"] == "exact"]
    fuzzy = mapping[mapping["match_type"] == "fuzzy"]
    none_ = mapping[mapping["match_type"] == "none"]

    n_exact = len(exact)
    n_fuzzy = len(fuzzy)
    n_none = len(none_)
    n_total = len(mapping)

    pct = lambda n: f"{n / n_total * 100:.1f}%" if n_total else "0.0%"

    # Fuzzy score distribution buckets
    buckets = {
        "80–84": fuzzy[(fuzzy["match_confidence"] >= 0.80) & (fuzzy["match_confidence"] < 0.85)],
        "85–89": fuzzy[(fuzzy["match_confidence"] >= 0.85) & (fuzzy["match_confidence"] < 0.90)],
        "90–94": fuzzy[(fuzzy["match_confidence"] >= 0.90) & (fuzzy["match_confidence"] < 0.95)],
        "95–99": fuzzy[(fuzzy["match_confidence"] >= 0.95) & (fuzzy["match_confidence"] < 1.00)],
    }

    # Sample unmatched (up to 20)
    unmatched_sample = none_["server_name"].head(20).tolist()

    # Servers with no application name even after matching
    no_app = mapping[
        mapping["match_type"].isin(["exact", "fuzzy"])
        & mapping["application_name"].isna()
    ]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        "# App–Server Match Quality Report",
        f"\n_Generated: {now}_\n",
        "## Summary",
        "",
        "| Metric | Count |",
        "| --- | --- |",
        f"| Azure Migrate servers (input) | {n_servers} |",
        f"| Device42 inventory entries (input) | {n_inventory} |",
        f"| Total mapping rows | {n_total} |",
        f"| Exact matches | {n_exact} ({pct(n_exact)}) |",
        f"| Fuzzy matches (≥{int(FUZZY_THRESHOLD)} score) | {n_fuzzy} ({pct(n_fuzzy)}) |",
        f"| No match | {n_none} ({pct(n_none)}) |",
        f"| Matched but no application name | {len(no_app)} |",
        "",
        "## Fuzzy Match Score Distribution",
        "",
        "| Score band | Count |",
        "| --- | --- |",
    ]

    for band, df_band in buckets.items():
        lines.append(f"| {band} | {len(df_band)} |")

    lines += [
        "",
        "## Unmatched Servers",
        "",
        f"These {n_none} servers from Azure Migrate had no match in the Device42 "
        "inventory (exact or fuzzy).",
        "",
    ]

    if unmatched_sample:
        lines.append("Sample (up to 20):")
        lines.append("")
        for name in unmatched_sample:
            lines.append(f"- `{name}`")
        if n_none > 20:
            lines.append(f"- _… and {n_none - 20} more (see mapping parquet)_")
    else:
        lines.append("_None — all servers were matched._")

    lines += [
        "",
        "## Matched Servers with No Application Name",
        "",
        f"{len(no_app)} server(s) were matched to an inventory entry but the "
        "inventory row contained no application name. This may indicate:",
        "",
        "- The application column was not detected (check `APP_NAME_CANDIDATES` "
          "in the script).",
        "- The Device42 export does not include application data for these servers.",
        "",
    ]

    if not no_app.empty:
        sample_no_app = no_app["server_name"].head(10).tolist()
        lines.append("Sample:")
        lines.append("")
        for name in sample_no_app:
            lines.append(f"- `{name}`")

    lines += [
        "",
        "## Notes",
        "",
        f"- Fuzzy matching uses `rapidfuzz.fuzz.WRatio` scorer with a threshold "
          f"of **{int(FUZZY_THRESHOLD)}**.",
        "- `match_confidence` is normalized to 0.0–1.0 (exact = 1.0).",
        "- Full results are in `outputs/app_server_mapping.parquet`.",
    ]

    report_path.write_text("\n".join(lines), encoding="utf-8")
    log.info("Wrote match report to %s", report_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> pd.DataFrame:
    servers = load_parquet(SERVERS_FILE, "servers_clean")
    inventory = load_parquet(INVENTORY_FILE, "inventory_clean")

    if servers.empty:
        log.error("servers_clean is empty or missing — cannot build mapping.")
        return pd.DataFrame()

    if inventory.empty:
        log.warning(
            "inventory_clean is empty or missing. "
            "Mapping will have no application names."
        )
        # Build skeleton mapping with no matches
        inventory = pd.DataFrame(columns=[NORMALIZED_COL])

    if not _validate_normalized_col(servers, "servers_clean"):
        return pd.DataFrame()
    if NORMALIZED_COL not in inventory.columns:
        log.warning(
            "'%s' missing from inventory_clean; treating all servers as unmatched.",
            NORMALIZED_COL,
        )
        inventory = pd.DataFrame(columns=[NORMALIZED_COL])

    n_servers = len(servers)
    n_inventory = len(inventory)

    mapping = build_mapping(servers, inventory)

    # Write parquet
    MAPPING_FILE.parent.mkdir(parents=True, exist_ok=True)
    mapping.to_parquet(MAPPING_FILE, index=False)
    log.info("Wrote %d mapping rows to %s", len(mapping), MAPPING_FILE)

    # Write report
    write_match_report(mapping, n_servers, n_inventory, REPORT_FILE)

    return mapping


if __name__ == "__main__":
    result = main()
    if result.empty:
        log.warning("Mapping is empty.")
        sys.exit(1)
    log.info("Done.")
