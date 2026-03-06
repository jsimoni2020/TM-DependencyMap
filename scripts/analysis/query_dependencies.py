"""
Query Neo4j for dependency information about an application or server.

Usage:
    python query_dependencies.py "my-app"
    python query_dependencies.py "web-server-01" --type server
    python query_dependencies.py "my-app" --type application
    python query_dependencies.py "web-server-01" --output-dir /tmp/reports

Without --type the script auto-detects whether the name matches an Application
or Server node. If both match, the Application view is shown.

Questions answered:
    Application mode:
        1. What servers does this application run on?
        2. What servers does it communicate with (outbound)?
        3. What servers communicate with it (inbound)?
        4. What other applications share those same servers?

    Server mode:
        1. What applications run on this server?
        2. What servers does it communicate with (outbound)?
        3. What servers communicate with it (inbound)?
        4. What other applications share servers that communicate with this one?

Outputs:
    - Formatted terminal text
    - JSON file → outputs/reports/<name>_<timestamp>.json
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase, Driver

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / ".env"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "reports"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("query_dependencies")


# ---------------------------------------------------------------------------
# Neo4j connection (mirrors load_neo4j.py pattern)
# ---------------------------------------------------------------------------

def _load_driver() -> tuple[Driver, str]:
    if ENV_FILE.exists():
        load_dotenv(ENV_FILE)
    else:
        log.warning(".env not found at %s — using environment variables", ENV_FILE)

    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    username = os.environ.get("NEO4J_USERNAME") or os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")

    if not password:
        log.error("NEO4J_PASSWORD not set. Add it to .env or the environment.")
        sys.exit(1)

    driver = GraphDatabase.driver(uri, auth=(username, password))
    try:
        driver.verify_connectivity()
    except Exception as exc:
        log.error("Cannot connect to Neo4j at %s: %s", uri, exc)
        sys.exit(1)

    log.info("Connected to Neo4j at %s (database: %s)", uri, database)
    return driver, database


# ---------------------------------------------------------------------------
# Auto-detect: Application or Server?
# ---------------------------------------------------------------------------

_DETECT_QUERY = """
MATCH (n)
WHERE (n:Application OR n:Server)
  AND toLower(n.name) = toLower($name)
RETURN labels(n) AS labels, n.name AS name
LIMIT 5
"""

_SUGGEST_QUERY = """
CALL {
    MATCH (a:Application)
    WHERE toLower(a.name) CONTAINS toLower($fragment)
    RETURN a.name AS name, 'Application' AS type
    LIMIT 5
    UNION ALL
    MATCH (s:Server)
    WHERE toLower(s.name) CONTAINS toLower($fragment)
    RETURN s.name AS name, 'Server' AS type
    LIMIT 5
}
RETURN name, type ORDER BY type, name LIMIT 10
"""


def detect_node_type(session, name: str) -> str | None:
    """
    Return 'application', 'server', or None if not found.
    If both labels match, 'application' takes priority.
    """
    records = session.run(_DETECT_QUERY, name=name).data()
    if not records:
        return None

    found_labels = set()
    for r in records:
        for label in r["labels"]:
            found_labels.add(label.lower())

    if "application" in found_labels:
        return "application"
    if "server" in found_labels:
        return "server"
    return None


def suggest_alternatives(session, name: str) -> list[dict]:
    fragment = name[:10]  # use prefix for substring search
    return session.run(_SUGGEST_QUERY, fragment=fragment).data()


# ---------------------------------------------------------------------------
# Application queries
# ---------------------------------------------------------------------------

_APP_SERVERS = """
MATCH (a:Application)-[r:RUNS_ON]->(s:Server)
WHERE toLower(a.name) = toLower($name)
RETURN
    s.name        AS server,
    s.ip          AS ip,
    s.os          AS os,
    s.environment AS environment,
    r.match_type        AS match_type,
    r.match_confidence  AS match_confidence
ORDER BY s.name
"""

_APP_OUTBOUND = """
MATCH (a:Application)-[:RUNS_ON]->(s:Server)-[r:COMMUNICATES_WITH]->(t:Server)
WHERE toLower(a.name) = toLower($name)
RETURN
    s.name    AS source_server,
    t.name    AS target_server,
    t.ip      AS target_ip,
    r.port    AS port,
    r.protocol AS protocol,
    r.process AS process
ORDER BY source_server, target_server
"""

_APP_INBOUND = """
MATCH (a:Application)-[:RUNS_ON]->(s:Server)<-[r:COMMUNICATES_WITH]-(src:Server)
WHERE toLower(a.name) = toLower($name)
RETURN
    src.name  AS source_server,
    src.ip    AS source_ip,
    s.name    AS target_server,
    r.port    AS port,
    r.protocol AS protocol,
    r.process AS process
ORDER BY source_server, target_server
"""

_APP_SHARED_APPS = """
MATCH (a:Application)-[:RUNS_ON]->(s:Server)<-[:RUNS_ON]-(other:Application)
WHERE toLower(a.name) = toLower($name)
  AND other.name <> a.name
RETURN
    other.name  AS application,
    other.owner AS owner,
    s.name      AS shared_server
ORDER BY application, shared_server
"""


def query_application(session, name: str) -> dict:
    return {
        "type": "application",
        "name": name,
        "servers":                 session.run(_APP_SERVERS,     name=name).data(),
        "outbound_communications": session.run(_APP_OUTBOUND,    name=name).data(),
        "inbound_communications":  session.run(_APP_INBOUND,     name=name).data(),
        "shared_applications":     session.run(_APP_SHARED_APPS, name=name).data(),
    }


# ---------------------------------------------------------------------------
# Server queries
# ---------------------------------------------------------------------------

_SRV_APPLICATIONS = """
MATCH (app:Application)-[r:RUNS_ON]->(s:Server)
WHERE toLower(s.name) = toLower($name)
RETURN
    app.name        AS application,
    app.owner       AS owner,
    r.match_type        AS match_type,
    r.match_confidence  AS match_confidence
ORDER BY app.name
"""

_SRV_OUTBOUND = """
MATCH (s:Server)-[r:COMMUNICATES_WITH]->(t:Server)
WHERE toLower(s.name) = toLower($name)
RETURN
    t.name    AS target_server,
    t.ip      AS target_ip,
    r.port    AS port,
    r.protocol AS protocol,
    r.process AS process
ORDER BY target_server
"""

_SRV_INBOUND = """
MATCH (s:Server)<-[r:COMMUNICATES_WITH]-(src:Server)
WHERE toLower(s.name) = toLower($name)
RETURN
    src.name  AS source_server,
    src.ip    AS source_ip,
    r.port    AS port,
    r.protocol AS protocol,
    r.process AS process
ORDER BY source_server
"""

_SRV_SHARED_APPS = """
MATCH (s:Server)-[:COMMUNICATES_WITH]->(t:Server)<-[:RUNS_ON]-(app:Application)
WHERE toLower(s.name) = toLower($name)
RETURN
    app.name  AS application,
    app.owner AS owner,
    t.name    AS via_server
ORDER BY application, via_server
"""


def query_server(session, name: str) -> dict:
    return {
        "type": "server",
        "name": name,
        "applications":            session.run(_SRV_APPLICATIONS, name=name).data(),
        "outbound_communications": session.run(_SRV_OUTBOUND,     name=name).data(),
        "inbound_communications":  session.run(_SRV_INBOUND,      name=name).data(),
        "shared_applications":     session.run(_SRV_SHARED_APPS,  name=name).data(),
    }


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------

_SEP = "=" * 72
_SUB = "-" * 72


def _fmt_kv(d: dict, skip: set[str] | None = None) -> str:
    """Render a record dict as 'key=value  key=value …' skipping None values."""
    skip = skip or set()
    parts = []
    for k, v in d.items():
        if k in skip or v is None:
            continue
        parts.append(f"{k}={v}")
    return "  ".join(parts) if parts else ""


def print_application_report(data: dict) -> None:
    name = data["name"]
    servers = data["servers"]
    outbound = data["outbound_communications"]
    inbound = data["inbound_communications"]
    shared = data["shared_applications"]

    print(_SEP)
    print(f"  APPLICATION DEPENDENCY REPORT: {name}")
    print(_SEP)

    # --- 1. Servers ---
    print(f"\n[1] SERVERS THIS APPLICATION RUNS ON  ({len(servers)} total)")
    print(_SUB)
    if servers:
        for r in servers:
            meta = _fmt_kv(r, skip={"server", "match_type", "match_confidence"})
            conf = r.get("match_confidence")
            conf_str = f"  [confidence={conf:.2f}]" if conf is not None else ""
            print(f"  • {r['server']}{conf_str}")
            if meta:
                print(f"    {meta}")
    else:
        print("  (none found)")

    # --- 2. Outbound ---
    print(f"\n[2] OUTBOUND COMMUNICATIONS  ({len(outbound)} total)")
    print(_SUB)
    if outbound:
        for r in outbound:
            meta = _fmt_kv(r, skip={"source_server", "target_server"})
            print(f"  • {r['source_server']}  →  {r['target_server']}")
            if meta:
                print(f"    {meta}")
    else:
        print("  (none found)")

    # --- 3. Inbound ---
    print(f"\n[3] INBOUND COMMUNICATIONS  ({len(inbound)} total)")
    print(_SUB)
    if inbound:
        for r in inbound:
            meta = _fmt_kv(r, skip={"source_server", "target_server"})
            print(f"  • {r['source_server']}  →  {r['target_server']}")
            if meta:
                print(f"    {meta}")
    else:
        print("  (none found)")

    # --- 4. Shared apps ---
    print(f"\n[4] OTHER APPLICATIONS SHARING THE SAME SERVERS  ({len(shared)} total)")
    print(_SUB)
    if shared:
        # Group by application
        by_app: dict[str, list[str]] = {}
        for r in shared:
            by_app.setdefault(r["application"], []).append(r["shared_server"])
        for app, srv_list in sorted(by_app.items()):
            print(f"  • {app}  (shared servers: {', '.join(sorted(srv_list))})")
    else:
        print("  (none found)")

    print()


def print_server_report(data: dict) -> None:
    name = data["name"]
    apps = data["applications"]
    outbound = data["outbound_communications"]
    inbound = data["inbound_communications"]
    shared = data["shared_applications"]

    print(_SEP)
    print(f"  SERVER DEPENDENCY REPORT: {name}")
    print(_SEP)

    # --- 1. Applications ---
    print(f"\n[1] APPLICATIONS RUNNING ON THIS SERVER  ({len(apps)} total)")
    print(_SUB)
    if apps:
        for r in apps:
            owner = f"  owner={r['owner']}" if r.get("owner") else ""
            conf = r.get("match_confidence")
            conf_str = f"  [confidence={conf:.2f}]" if conf is not None else ""
            print(f"  • {r['application']}{owner}{conf_str}")
    else:
        print("  (none found)")

    # --- 2. Outbound ---
    print(f"\n[2] OUTBOUND COMMUNICATIONS  ({len(outbound)} total)")
    print(_SUB)
    if outbound:
        for r in outbound:
            meta = _fmt_kv(r, skip={"target_server"})
            print(f"  • → {r['target_server']}")
            if meta:
                print(f"    {meta}")
    else:
        print("  (none found)")

    # --- 3. Inbound ---
    print(f"\n[3] INBOUND COMMUNICATIONS  ({len(inbound)} total)")
    print(_SUB)
    if inbound:
        for r in inbound:
            meta = _fmt_kv(r, skip={"source_server"})
            print(f"  • {r['source_server']} →")
            if meta:
                print(f"    {meta}")
    else:
        print("  (none found)")

    # --- 4. Shared apps ---
    print(f"\n[4] APPLICATIONS ON SERVERS THIS SERVER COMMUNICATES WITH  ({len(shared)} total)")
    print(_SUB)
    if shared:
        by_app: dict[str, list[str]] = {}
        for r in shared:
            by_app.setdefault(r["application"], []).append(r["via_server"])
        for app, srv_list in sorted(by_app.items()):
            print(f"  • {app}  (via: {', '.join(sorted(srv_list))})")
    else:
        print("  (none found)")

    print()


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

def _safe_json(obj):
    """Recursively convert neo4j types and None to JSON-safe values."""
    if obj is None:
        return None
    if isinstance(obj, float) and obj != obj:  # NaN check
        return None
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_json(i) for i in obj]
    return obj


def _sanitize_filename(name: str) -> str:
    return re.sub(r"[^\w\-]", "_", name).strip("_")


def write_json_report(data: dict, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    safe_name = _sanitize_filename(data["name"])
    filename = f"{safe_name}_{timestamp}.json"
    output_path = output_dir / filename

    payload = {
        "query": {
            "type": data["type"],
            "name": data["name"],
            "generated_at": now.isoformat(),
        },
        **{k: _safe_json(v) for k, v in data.items() if k not in ("type", "name")},
    }

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    log.info("JSON report written to %s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Query Neo4j for application/server dependency information.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python query_dependencies.py "my-app"
  python query_dependencies.py "web-server-01" --type server
  python query_dependencies.py "my-app" --type application --output-dir /tmp
        """,
    )
    parser.add_argument(
        "name",
        help="Application or server name to query (case-insensitive).",
    )
    parser.add_argument(
        "--type",
        choices=["application", "server", "auto"],
        default="auto",
        help="Node type to query. Default: auto-detect from the graph.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for JSON report output. Default: {DEFAULT_OUTPUT_DIR}",
    )
    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    driver, database = _load_driver()

    try:
        with driver.session(database=database) as session:

            # --- Resolve node type ---
            node_type = args.type
            if node_type == "auto":
                node_type = detect_node_type(session, args.name)
                if node_type is None:
                    print(f"\nNo Application or Server named '{args.name}' found in Neo4j.\n")
                    suggestions = suggest_alternatives(session, args.name)
                    if suggestions:
                        print("Did you mean one of these?")
                        for s in suggestions:
                            print(f"  [{s['type']}] {s['name']}")
                    else:
                        print("No similar names found. Check that the graph has been loaded.")
                    sys.exit(1)
                log.info("Auto-detected node type: %s", node_type)

            # --- Run queries ---
            if node_type == "application":
                data = query_application(session, args.name)
                print_application_report(data)
            else:
                data = query_server(session, args.name)
                print_server_report(data)

            # --- Warn if nothing came back at all ---
            result_counts = {
                k: len(v)
                for k, v in data.items()
                if isinstance(v, list)
            }
            if all(n == 0 for n in result_counts.values()):
                print(
                    f"Warning: '{args.name}' exists in the graph but has no "
                    "connected nodes. The graph may not be fully loaded.\n"
                )

            # --- Write JSON ---
            report_path = write_json_report(data, args.output_dir)
            print(f"Report saved to: {report_path}\n")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
