"""
Load cleaned server/application data into a local Neo4j instance.

Data sources:
    outputs/servers_clean.parquet      → (:Server) nodes
    outputs/app_server_mapping.parquet → (:Application) nodes + [:RUNS_ON] relationships
    outputs/inventory_clean.parquet    → enriches Application nodes (owner, etc.)
    inputs/azure_migrate/              → dependency CSVs → [:COMMUNICATES_WITH]

Idempotent: uses MERGE throughout; safe to re-run without creating duplicates.
Credentials are read from a .env file — never hardcoded.

Required .env keys:
    NEO4J_URI       e.g. bolt://localhost:7687
    NEO4J_USERNAME  e.g. neo4j
    NEO4J_PASSWORD  your password
    NEO4J_DATABASE  (optional, defaults to "neo4j")
"""

import csv
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase, Driver

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
SERVERS_FILE = ROOT / "outputs" / "servers_clean.parquet"
MAPPING_FILE = ROOT / "outputs" / "app_server_mapping.parquet"
INVENTORY_FILE = ROOT / "outputs" / "inventory_clean.parquet"
AZURE_MIGRATE_DIR = ROOT / "inputs" / "azure_migrate"
ENV_FILE = ROOT / ".env"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("load_neo4j")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BATCH_SIZE = 500

# Server property column candidates (case-insensitive); first match wins.
_SERVER_PROP_CANDIDATES: dict[str, list[str]] = {
    "ip": ["ip addresses", "ip address", "ip_addresses", "ip", "ipaddress"],
    "os": ["operating system", "os", "os name", "operating_system", "os_name"],
    "environment": ["environment", "tag: environment", "env", "tag_environment"],
    "cores": ["cores", "number of cores", "cpu_cores", "vcpus"],
    "memory_mb": ["memory (mb)", "memory_mb", "ram (mb)", "memory"],
    "storage_gb": ["storage (gb)", "storage_gb", "disk (gb)", "disk size (gb)"],
}

# Application property column candidates from inventory.
_APP_PROP_CANDIDATES: dict[str, list[str]] = {
    "owner": ["owner", "app_owner", "application owner", "business_owner"],
    "business_unit": ["business_unit", "department", "division", "business unit"],
    "environment": ["environment", "env", "app_environment"],
    "tier": ["tier", "app_tier", "criticality"],
}

# Network dependency column candidates.
_DEP_SOURCE_CANDIDATES = [
    "source machine", "source server", "source", "source computer",
    "client name", "source_machine", "source_server", "sourcemachine",
]
_DEP_DEST_CANDIDATES = [
    "destination machine", "destination server", "destination", "target",
    "server name", "dest", "destination_machine", "target_server",
    "destinationmachine",
]
_DEP_PORT_CANDIDATES = [
    "destination port", "port", "dest port", "destination_port", "dstport",
]
_DEP_PROTOCOL_CANDIDATES = [
    "protocol", "transport protocol", "transport_protocol",
]
_DEP_PROCESS_CANDIDATES = [
    "destination process", "process name", "process", "dest process",
    "destination_process",
]


# ---------------------------------------------------------------------------
# Run counters
# ---------------------------------------------------------------------------
@dataclass
class Counters:
    nodes_created: int = 0
    relationships_created: int = 0
    properties_set: int = 0
    batches: int = 0
    skipped_rows: int = 0
    extra: dict = field(default_factory=dict)

    def absorb(self, summary) -> None:
        c = summary.counters
        self.nodes_created += c.nodes_created
        self.relationships_created += c.relationships_created
        self.properties_set += c.properties_set
        self.batches += 1

    def log_section(self, label: str) -> None:
        log.info(
            "%s — nodes created: %d, relationships created: %d, properties set: %d",
            label,
            self.nodes_created,
            self.relationships_created,
            self.properties_set,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_env() -> dict[str, str]:
    if ENV_FILE.exists():
        load_dotenv(ENV_FILE)
        log.info("Loaded .env from %s", ENV_FILE)
    else:
        log.warning(".env not found at %s — falling back to environment variables", ENV_FILE)

    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    username = os.environ.get("NEO4J_USERNAME") or os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")

    if not password:
        log.error("NEO4J_PASSWORD is not set. Add it to .env or the environment.")
        sys.exit(1)

    return {"uri": uri, "username": username, "password": password, "database": database}


def _connect(creds: dict) -> Driver:
    log.info("Connecting to Neo4j at %s (database: %s)", creds["uri"], creds["database"])
    driver = GraphDatabase.driver(
        creds["uri"],
        auth=(creds["username"], creds["password"]),
    )
    driver.verify_connectivity()
    log.info("Connected.")
    return driver


def _load_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        log.warning("File not found, skipping: %s", path)
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
        log.info("Loaded %s: %d rows", label, len(df))
        return df
    except Exception as exc:
        log.error("Failed to read %s (%s): %s", label, path, exc)
        return pd.DataFrame()


def _sanitize(val):
    """Convert NaN/None/empty strings to None for Neo4j compatibility."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, str) and val.strip().lower() in ("nan", "none", "null", ""):
        return None
    return val


def _detect_col(columns: list[str], candidates: list[str]) -> str | None:
    col_map = {c.strip().lower(): c for c in columns}
    for candidate in candidates:
        if candidate in col_map:
            return col_map[candidate]
    return None


def _detect_prop_cols(
    columns: list[str],
    candidates_map: dict[str, list[str]],
) -> dict[str, str]:
    """Return {prop_name: actual_column_name} for detected properties."""
    result = {}
    for prop, candidates in candidates_map.items():
        col = _detect_col(columns, candidates)
        if col:
            result[prop] = col
    return result


def _df_to_rows(
    df: pd.DataFrame,
    name_col: str,
    prop_col_map: dict[str, str],
    extra_cols: dict[str, str] | None = None,
) -> list[dict]:
    """
    Convert DataFrame rows to dicts suitable for Neo4j UNWIND.
    extra_cols: {output_key: dataframe_column} for additional scalar fields.
    """
    rows = []
    for _, row in df.iterrows():
        props = {}
        for prop, col in prop_col_map.items():
            props[prop] = _sanitize(row.get(col))

        record = {"name": _sanitize(row.get(name_col)), "props": props}

        if extra_cols:
            for key, col in extra_cols.items():
                record[key] = _sanitize(row.get(col))

        if record["name"] is None:
            continue
        rows.append(record)
    return rows


def _run_batched(session, query: str, rows: list[dict], counters: Counters) -> None:
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        result = session.run(query, rows=batch)
        counters.absorb(result.consume())


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CONSTRAINTS = [
    "CREATE CONSTRAINT server_name_unique IF NOT EXISTS "
    "FOR (s:Server) REQUIRE s.name IS UNIQUE",
    "CREATE CONSTRAINT application_name_unique IF NOT EXISTS "
    "FOR (a:Application) REQUIRE a.name IS UNIQUE",
]


def ensure_schema(driver: Driver, database: str) -> None:
    log.info("Ensuring schema constraints…")
    with driver.session(database=database) as session:
        for stmt in _CONSTRAINTS:
            try:
                session.run(stmt)
            except Exception as exc:
                log.warning("Could not create constraint (may already exist): %s", exc)
    log.info("Schema ready.")


# ---------------------------------------------------------------------------
# Server nodes
# ---------------------------------------------------------------------------

_MERGE_SERVERS = """
UNWIND $rows AS row
MERGE (s:Server {name: row.name})
SET s += row.props
"""


def load_servers(driver: Driver, database: str, servers: pd.DataFrame) -> Counters:
    if servers.empty:
        log.warning("No server data — skipping Server nodes.")
        return Counters()

    name_col = "server_name_normalized"
    if name_col not in servers.columns:
        log.error("'%s' missing from servers_clean — cannot load Server nodes.", name_col)
        return Counters()

    prop_col_map = _detect_prop_cols(servers.columns.tolist(), _SERVER_PROP_CANDIDATES)
    log.info("Server property columns detected: %s", prop_col_map)

    rows = _df_to_rows(servers, name_col, prop_col_map)
    log.info("Merging %d Server nodes…", len(rows))

    counters = Counters()
    with driver.session(database=database) as session:
        _run_batched(session, _MERGE_SERVERS, rows, counters)

    counters.log_section("Server nodes")
    return counters


# ---------------------------------------------------------------------------
# Application nodes
# ---------------------------------------------------------------------------

_MERGE_APPS = """
UNWIND $rows AS row
MERGE (a:Application {name: row.name})
SET a += row.props
"""


def load_applications(
    driver: Driver,
    database: str,
    mapping: pd.DataFrame,
    inventory: pd.DataFrame,
) -> Counters:
    # Distinct application names from the mapping (exclude nulls and unmatched)
    app_names = (
        mapping[mapping["application_name"].notna()]["application_name"]
        .drop_duplicates()
        .tolist()
    )

    if not app_names:
        log.warning("No application names found in mapping — skipping Application nodes.")
        return Counters()

    # Build enrichment lookup from inventory if available
    enrich: dict[str, dict] = {}
    if not inventory.empty:
        app_col = _detect_col(inventory.columns.tolist(), [
            "application_name", "application", "app_name", "app",
            "business_application", "service_name", "service",
        ])
        if app_col:
            prop_col_map = _detect_prop_cols(inventory.columns.tolist(), _APP_PROP_CANDIDATES)
            log.info("Application property columns detected: %s", prop_col_map)
            for _, row in inventory.iterrows():
                app_name = _sanitize(row.get(app_col))
                if app_name is None:
                    continue
                props = {p: _sanitize(row.get(c)) for p, c in prop_col_map.items()}
                enrich[app_name] = props

    rows = []
    for name in app_names:
        rows.append({"name": name, "props": enrich.get(name, {})})

    log.info("Merging %d Application nodes…", len(rows))
    counters = Counters()
    with driver.session(database=database) as session:
        _run_batched(session, _MERGE_APPS, rows, counters)

    counters.log_section("Application nodes")
    return counters


# ---------------------------------------------------------------------------
# RUNS_ON relationships
# ---------------------------------------------------------------------------

_MERGE_RUNS_ON = """
UNWIND $rows AS row
MATCH (a:Application {name: row.app_name})
MATCH (s:Server {name: row.server_name})
MERGE (a)-[r:RUNS_ON]->(s)
SET r.match_type = row.match_type,
    r.match_confidence = row.match_confidence
"""


def load_runs_on(driver: Driver, database: str, mapping: pd.DataFrame) -> Counters:
    valid = mapping[
        mapping["application_name"].notna() & mapping["server_name"].notna()
    ].copy()

    if valid.empty:
        log.warning("No valid app→server pairs — skipping RUNS_ON relationships.")
        return Counters()

    rows = [
        {
            "app_name": _sanitize(r["application_name"]),
            "server_name": _sanitize(r["server_name"]),
            "match_type": _sanitize(r.get("match_type")),
            "match_confidence": _sanitize(r.get("match_confidence")),
        }
        for _, r in valid.iterrows()
        if _sanitize(r["application_name"]) and _sanitize(r["server_name"])
    ]

    log.info("Merging %d RUNS_ON relationships…", len(rows))
    counters = Counters()
    with driver.session(database=database) as session:
        _run_batched(session, _MERGE_RUNS_ON, rows, counters)

    counters.log_section("RUNS_ON relationships")
    return counters


# ---------------------------------------------------------------------------
# COMMUNICATES_WITH relationships (Azure Migrate dependency data)
# ---------------------------------------------------------------------------

_MERGE_COMMS = """
UNWIND $rows AS row
MATCH (s1:Server {name: row.source})
MATCH (s2:Server {name: row.dest})
MERGE (s1)-[r:COMMUNICATES_WITH]->(s2)
SET r.port = row.port,
    r.protocol = row.protocol,
    r.process = row.process
"""


def _is_dependency_file(df: pd.DataFrame) -> bool:
    """Return True if this CSV looks like a network dependency export."""
    cols_lower = {c.strip().lower() for c in df.columns}
    has_source = any(c in cols_lower for c in _DEP_SOURCE_CANDIDATES)
    has_dest = any(c in cols_lower for c in _DEP_DEST_CANDIDATES)
    return has_source and has_dest


def _normalize_name(val) -> str | None:
    v = _sanitize(val)
    if v is None:
        return None
    return str(v).strip().lower()


def load_communicates_with(driver: Driver, database: str) -> Counters:
    if not AZURE_MIGRATE_DIR.exists():
        log.warning("Azure Migrate input dir not found — skipping COMMUNICATES_WITH.")
        return Counters()

    dep_files = sorted(AZURE_MIGRATE_DIR.glob("*.csv"))
    counters = Counters()
    files_processed = 0

    for path in dep_files:
        try:
            df = pd.read_csv(path, dtype=str)
        except Exception as exc:
            log.error("Could not read %s: %s", path.name, exc)
            continue

        if not _is_dependency_file(df):
            log.debug("Skipping (not a dependency file): %s", path.name)
            continue

        log.info("Processing dependency file: %s (%d rows)", path.name, len(df))
        cols = df.columns.tolist()

        source_col = _detect_col(cols, _DEP_SOURCE_CANDIDATES)
        dest_col = _detect_col(cols, _DEP_DEST_CANDIDATES)
        port_col = _detect_col(cols, _DEP_PORT_CANDIDATES)
        protocol_col = _detect_col(cols, _DEP_PROTOCOL_CANDIDATES)
        process_col = _detect_col(cols, _DEP_PROCESS_CANDIDATES)

        rows = []
        for _, row in df.iterrows():
            source = _normalize_name(row.get(source_col))
            dest = _normalize_name(row.get(dest_col))
            if not source or not dest or source == dest:
                counters.skipped_rows += 1
                continue
            rows.append({
                "source": source,
                "dest": dest,
                "port": _sanitize(row.get(port_col) if port_col else None),
                "protocol": _sanitize(row.get(protocol_col) if protocol_col else None),
                "process": _sanitize(row.get(process_col) if process_col else None),
            })

        if rows:
            with driver.session(database=database) as session:
                _run_batched(session, _MERGE_COMMS, rows, counters)

        files_processed += 1

    if files_processed == 0:
        log.warning(
            "No dependency files detected in %s "
            "(looked for CSVs with source+destination columns).",
            AZURE_MIGRATE_DIR,
        )
    else:
        log.info("Processed %d dependency file(s).", files_processed)

    counters.log_section("COMMUNICATES_WITH relationships")
    return counters


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _log_summary(totals: dict[str, Counters]) -> None:
    total_nodes = sum(c.nodes_created for c in totals.values())
    total_rels = sum(c.relationships_created for c in totals.values())
    total_props = sum(c.properties_set for c in totals.values())
    skipped = sum(c.skipped_rows for c in totals.values())

    log.info("=" * 60)
    log.info("LOAD COMPLETE")
    log.info("  Nodes created:         %d", total_nodes)
    log.info("  Relationships created: %d", total_rels)
    log.info("  Properties set:        %d", total_props)
    log.info("  Dependency rows skipped (self-loops / nulls): %d", skipped)
    log.info("=" * 60)

    for section, c in totals.items():
        log.info(
            "  %-30s nodes=%d  rels=%d",
            section,
            c.nodes_created,
            c.relationships_created,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    creds = _load_env()

    try:
        driver = _connect(creds)
    except Exception as exc:
        log.error("Failed to connect to Neo4j: %s", exc)
        sys.exit(1)

    try:
        database = creds["database"]

        servers = _load_parquet(SERVERS_FILE, "servers_clean")
        mapping = _load_parquet(MAPPING_FILE, "app_server_mapping")
        inventory = _load_parquet(INVENTORY_FILE, "inventory_clean")

        if servers.empty and mapping.empty:
            log.error("No usable input data. Run the ingest scripts first.")
            sys.exit(1)

        ensure_schema(driver, database)

        totals = {
            "Server nodes": load_servers(driver, database, servers),
            "Application nodes": load_applications(driver, database, mapping, inventory),
            "RUNS_ON": load_runs_on(driver, database, mapping),
            "COMMUNICATES_WITH": load_communicates_with(driver, database),
        }

        _log_summary(totals)

    finally:
        driver.close()
        log.info("Driver closed.")


if __name__ == "__main__":
    main()
