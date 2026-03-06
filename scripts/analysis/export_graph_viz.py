"""
Export the Neo4j dependency graph to multiple visualization formats.

Usage:
    # Full graph
    python export_graph_viz.py

    # Filtered to one application's subgraph
    python export_graph_viz.py "my-app" --type application

    # Filtered to one server's neighbourhood
    python export_graph_viz.py "web-server-01" --type server

    # Skip the PNG for large graphs
    python export_graph_viz.py --no-png

Outputs (all written to outputs/graphs/ by default):
    <prefix>_<timestamp>.graphml   — importable into yEd / Gephi
    <prefix>_<timestamp>.html      — interactive browser viz (self-contained)
    <prefix>_<timestamp>.png       — static overview via matplotlib/networkx

Node colours:
    Application → blue  (#4C72B0)   square
    Server      → green (#55A868)   circle

Edge styles:
    RUNS_ON           → blue, solid
    COMMUNICATES_WITH → orange, dashed
"""

import argparse
import logging
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend; must be set before pyplot import
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import networkx as nx
from dotenv import load_dotenv
from neo4j import GraphDatabase, Driver
from pyvis.network import Network

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / ".env"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "graphs"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("export_graph_viz")

# ---------------------------------------------------------------------------
# Visual constants
# ---------------------------------------------------------------------------
COLORS = {
    "Application": "#4C72B0",
    "Server":      "#55A868",
    "unknown":     "#999999",
}
PYVIS_SHAPES = {
    "Application": "box",
    "Server":      "ellipse",
}
EDGE_COLORS = {
    "RUNS_ON":           "#4C72B0",
    "COMMUNICATES_WITH": "#DD8452",
}
EDGE_STYLES = {
    "RUNS_ON":           "solid",
    "COMMUNICATES_WITH": "dashed",
}

PNG_MAX_NODES = 300   # skip PNG and warn above this threshold
PNG_WARN_NODES = 100  # warn (but still render) above this


# ---------------------------------------------------------------------------
# Neo4j connection
# ---------------------------------------------------------------------------

def _load_driver() -> tuple[Driver, str]:
    if ENV_FILE.exists():
        load_dotenv(ENV_FILE)
    else:
        log.warning(".env not found at %s — using environment variables", ENV_FILE)

    uri      = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
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
# Cypher queries
# ---------------------------------------------------------------------------

# --- Full graph ---

_NODES_FULL = """
MATCH (n)
WHERE n:Application OR n:Server
RETURN n.name AS name, labels(n)[0] AS node_type, properties(n) AS props
ORDER BY node_type, name
"""

_RUNS_ON_FULL = """
MATCH (a:Application)-[r:RUNS_ON]->(s:Server)
RETURN a.name AS source, s.name AS target,
       'RUNS_ON' AS rel_type,
       r.match_confidence AS match_confidence
"""

_COMMS_FULL = """
MATCH (s1:Server)-[r:COMMUNICATES_WITH]->(s2:Server)
RETURN s1.name AS source, s2.name AS target,
       'COMMUNICATES_WITH' AS rel_type,
       r.port AS port, r.protocol AS protocol
"""

# --- Filtered by application ---

_RUNS_ON_APP = """
MATCH (a:Application)-[r:RUNS_ON]->(s:Server)
WHERE toLower(a.name) = toLower($name)
RETURN a.name AS source, s.name AS target,
       'RUNS_ON' AS rel_type, r.match_confidence AS match_confidence
UNION
MATCH (a:Application)-[:RUNS_ON]->(s:Server)<-[r:RUNS_ON]-(other:Application)
WHERE toLower(a.name) = toLower($name) AND other.name <> a.name
RETURN other.name AS source, s.name AS target,
       'RUNS_ON' AS rel_type, r.match_confidence AS match_confidence
"""

_COMMS_APP = """
MATCH (a:Application)-[:RUNS_ON]->(s:Server)-[r:COMMUNICATES_WITH]->(t:Server)
WHERE toLower(a.name) = toLower($name)
RETURN s.name AS source, t.name AS target,
       'COMMUNICATES_WITH' AS rel_type, r.port AS port, r.protocol AS protocol
UNION
MATCH (a:Application)-[:RUNS_ON]->(s:Server)<-[r:COMMUNICATES_WITH]-(src:Server)
WHERE toLower(a.name) = toLower($name)
RETURN src.name AS source, s.name AS target,
       'COMMUNICATES_WITH' AS rel_type, r.port AS port, r.protocol AS protocol
"""

# --- Filtered by server ---

_RUNS_ON_SRV = """
MATCH (app:Application)-[r:RUNS_ON]->(s:Server)
WHERE toLower(s.name) = toLower($name)
RETURN app.name AS source, s.name AS target,
       'RUNS_ON' AS rel_type, r.match_confidence AS match_confidence
"""

_COMMS_SRV = """
MATCH (s:Server)-[r:COMMUNICATES_WITH]->(t:Server)
WHERE toLower(s.name) = toLower($name)
RETURN s.name AS source, t.name AS target,
       'COMMUNICATES_WITH' AS rel_type, r.port AS port, r.protocol AS protocol
UNION
MATCH (s:Server)<-[r:COMMUNICATES_WITH]-(src:Server)
WHERE toLower(s.name) = toLower($name)
RETURN src.name AS source, s.name AS target,
       'COMMUNICATES_WITH' AS rel_type, r.port AS port, r.protocol AS protocol
"""

# --- Node property lookup by name list ---

_NODE_PROPS = """
MATCH (n)
WHERE (n:Application OR n:Server) AND n.name IN $names
RETURN n.name AS name, labels(n)[0] AS node_type, properties(n) AS props
"""


# ---------------------------------------------------------------------------
# Graph fetching
# ---------------------------------------------------------------------------

def _safe_props(props: dict) -> dict:
    """Strip None and non-primitive values for GraphML / networkx compatibility."""
    out = {}
    for k, v in props.items():
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)):
            out[k] = v
        else:
            out[k] = str(v)  # coerce lists, dicts, etc.
    return out


def fetch_full_graph(session) -> tuple[dict, list[dict]]:
    log.info("Fetching full graph from Neo4j…")
    nodes_raw = session.run(_NODES_FULL).data()
    edges = (
        session.run(_RUNS_ON_FULL).data()
        + session.run(_COMMS_FULL).data()
    )

    nodes = {
        r["name"]: {"node_type": r["node_type"], **_safe_props(r["props"])}
        for r in nodes_raw
    }
    log.info("Full graph: %d nodes, %d edges", len(nodes), len(edges))
    return nodes, edges


def fetch_filtered_graph(session, name: str, node_type: str) -> tuple[dict, list[dict]]:
    log.info("Fetching subgraph for %s '%s'…", node_type, name)

    if node_type == "application":
        edges = (
            session.run(_RUNS_ON_APP, name=name).data()
            + session.run(_COMMS_APP,  name=name).data()
        )
    else:
        edges = (
            session.run(_RUNS_ON_SRV, name=name).data()
            + session.run(_COMMS_SRV,  name=name).data()
        )

    # Derive all node names from edge endpoints, then fetch their properties
    node_names = list({e["source"] for e in edges} | {e["target"] for e in edges})
    nodes_raw = session.run(_NODE_PROPS, names=node_names).data()
    nodes = {
        r["name"]: {"node_type": r["node_type"], **_safe_props(r["props"])}
        for r in nodes_raw
    }
    # Ensure every endpoint has an entry (fallback if not in graph)
    for n in node_names:
        if n not in nodes:
            nodes[n] = {"node_type": "Server"}

    log.info("Subgraph: %d nodes, %d edges", len(nodes), len(edges))
    return nodes, edges


# ---------------------------------------------------------------------------
# NetworkX graph builder
# ---------------------------------------------------------------------------

def build_nx_graph(nodes: dict, edges: list[dict]) -> nx.DiGraph:
    G = nx.DiGraph()

    for name, props in nodes.items():
        G.add_node(name, **props)

    for e in edges:
        src, dst = e["source"], e["target"]
        # Ensure orphaned endpoints have nodes
        if src not in G:
            G.add_node(src, node_type="Server")
        if dst not in G:
            G.add_node(dst, node_type="Server")

        edge_attrs = {k: v for k, v in e.items()
                      if k not in ("source", "target") and v is not None}
        # Deduplicate: if edge already exists keep existing (MERGE semantics)
        if not G.has_edge(src, dst):
            G.add_edge(src, dst, **edge_attrs)

    return G


# ---------------------------------------------------------------------------
# GraphML export
# ---------------------------------------------------------------------------

def export_graphml(G: nx.DiGraph, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    # networkx write_graphml fails on None; they were stripped in _safe_props
    # but edge attrs may still carry them — clean edges too
    H = G.copy()
    for u, v, data in H.edges(data=True):
        H[u][v].update({k: v for k, vv in data.items() if vv is None for k in [k]})
        clean = {k: vv for k, vv in data.items() if vv is not None}
        H[u][v].clear()
        H[u][v].update(clean)

    nx.write_graphml(H, str(path))
    log.info("GraphML written: %s", path)
    return path


# ---------------------------------------------------------------------------
# Interactive HTML export (pyvis)
# ---------------------------------------------------------------------------

def _node_tooltip(name: str, data: dict) -> str:
    lines = [f"<b>{name}</b>"]
    for k, v in data.items():
        if k != "node_type" and v is not None:
            lines.append(f"<b>{k}</b>: {v}")
    return "<br>".join(lines)


def _edge_label(data: dict) -> str:
    rel = data.get("rel_type", "")
    if rel == "COMMUNICATES_WITH":
        parts = [p for p in (data.get("port"), data.get("protocol")) if p]
        return "/".join(parts) if parts else "COMMS"
    return rel


def export_html(G: nx.DiGraph, path: Path, title: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        net = Network(
            height="920px",
            width="100%",
            directed=True,
            cdn_resources="in_line",   # self-contained; embeds vis.js inline
        )
    except TypeError:
        # Older pyvis doesn't support cdn_resources — falls back to remote CDN
        log.warning("pyvis cdn_resources='in_line' not supported; using remote CDN.")
        net = Network(height="920px", width="100%", directed=True)

    net.barnes_hut(
        gravity=-8000,
        central_gravity=0.3,
        spring_length=200,
        spring_strength=0.05,
        damping=0.09,
    )

    # Nodes
    for node_id, data in G.nodes(data=True):
        ntype = data.get("node_type", "Server")
        color = COLORS.get(ntype, COLORS["unknown"])
        shape = PYVIS_SHAPES.get(ntype, "ellipse")
        size  = 30 if ntype == "Application" else 20
        net.add_node(
            node_id,
            label=node_id,
            color=color,
            shape=shape,
            size=size,
            title=_node_tooltip(node_id, data),
            font={"size": 12, "color": "#333333"},
            borderWidth=2,
        )

    # Edges
    for src, dst, data in G.edges(data=True):
        rel = data.get("rel_type", "")
        color = EDGE_COLORS.get(rel, "#999999")
        dashes = rel == "COMMUNICATES_WITH"
        net.add_edge(
            src,
            dst,
            label=_edge_label(data),
            color={"color": color, "highlight": "#FF0000"},
            dashes=dashes,
            arrows="to",
            font={"size": 9, "color": "#666666"},
            title=f"{rel}" + (f"  port={data.get('port')}  protocol={data.get('protocol')}"
                              if rel == "COMMUNICATES_WITH" else ""),
        )

    # Inject a title banner via custom HTML header
    net.html = net.html if hasattr(net, "html") else ""
    net.set_options("""
    var options = {
      "interaction": {
        "hover": true,
        "navigationButtons": true,
        "keyboard": { "enabled": true }
      },
      "edges": { "smooth": { "type": "dynamic" } }
    }
    """)

    net.write_html(str(path))
    log.info("HTML written: %s", path)
    return path


# ---------------------------------------------------------------------------
# PNG export (matplotlib + networkx)
# ---------------------------------------------------------------------------

def export_png(G: nx.DiGraph, path: Path, title: str, force: bool = False) -> Path | None:
    n = G.number_of_nodes()

    if n > PNG_MAX_NODES and not force:
        log.warning(
            "Graph has %d nodes (limit %d). Skipping PNG. "
            "Use --force-png to override.",
            n, PNG_MAX_NODES,
        )
        return None

    if n > PNG_WARN_NODES:
        log.warning(
            "Graph has %d nodes — PNG may be dense and hard to read.", n
        )

    path.parent.mkdir(parents=True, exist_ok=True)

    # Layout: kamada_kawai for smaller graphs (more structured),
    # spring for larger (faster, handles disconnected components)
    log.info("Computing layout for %d nodes…", n)
    if n <= 80:
        try:
            pos = nx.kamada_kawai_layout(G)
        except Exception:
            pos = nx.spring_layout(G, seed=42, k=3 / math.sqrt(max(n, 1)))
    else:
        pos = nx.spring_layout(G, seed=42, k=3 / math.sqrt(max(n, 1)), iterations=60)

    # Separate nodes by type
    app_nodes = [nd for nd, d in G.nodes(data=True) if d.get("node_type") == "Application"]
    srv_nodes = [nd for nd, d in G.nodes(data=True) if d.get("node_type") != "Application"]

    # Separate edges by relationship type
    runs_on_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get("rel_type") == "RUNS_ON"]
    comms_edges   = [(u, v) for u, v, d in G.edges(data=True) if d.get("rel_type") == "COMMUNICATES_WITH"]

    # Figure sizing: scale with node count
    fig_w = max(16, min(36, 14 + n * 0.15))
    fig_h = max(12, min(28, 10 + n * 0.12))
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.set_title(title, fontsize=14, fontweight="bold", pad=16)
    ax.axis("off")

    node_size_app = max(600, min(2000, 4000 // max(n, 1) * 10))
    node_size_srv = max(400, min(1400, 3000 // max(n, 1) * 10))
    font_size     = max(5, min(9, 120 // max(n, 1)))

    # Draw nodes
    nx.draw_networkx_nodes(
        G, pos, nodelist=app_nodes,
        node_color=COLORS["Application"], node_shape="s",
        node_size=node_size_app, alpha=0.9, ax=ax,
    )
    nx.draw_networkx_nodes(
        G, pos, nodelist=srv_nodes,
        node_color=COLORS["Server"], node_shape="o",
        node_size=node_size_srv, alpha=0.9, ax=ax,
    )

    # Draw edges — use arc style to separate bidirectional arrows
    common_edge_kw = dict(ax=ax, arrows=True, arrowsize=14,
                          node_size=node_size_srv,
                          connectionstyle="arc3,rad=0.08")

    nx.draw_networkx_edges(
        G, pos, edgelist=runs_on_edges,
        edge_color=COLORS["Application"], style="solid",
        width=1.5, alpha=0.8, **common_edge_kw,
    )
    nx.draw_networkx_edges(
        G, pos, edgelist=comms_edges,
        edge_color=EDGE_COLORS["COMMUNICATES_WITH"], style="dashed",
        width=1.2, alpha=0.7, **common_edge_kw,
    )

    # Labels — truncate long names
    labels = {nd: (nd[:22] + "…" if len(nd) > 24 else nd) for nd in G.nodes()}
    nx.draw_networkx_labels(G, pos, labels=labels, font_size=font_size, ax=ax)

    # Legend
    legend_handles = [
        mpatches.Patch(color=COLORS["Application"], label="Application"),
        mpatches.Patch(color=COLORS["Server"],      label="Server"),
        mpatches.Patch(color=COLORS["Application"], label="RUNS_ON (solid)"),
        mpatches.Patch(color=EDGE_COLORS["COMMUNICATES_WITH"], label="COMMUNICATES_WITH (dashed)"),
    ]
    ax.legend(handles=legend_handles, loc="upper left", fontsize=9,
              framealpha=0.85, borderpad=0.8)

    # Stats annotation
    ax.text(
        0.99, 0.01,
        f"{G.number_of_nodes()} nodes · {G.number_of_edges()} edges",
        transform=ax.transAxes,
        ha="right", va="bottom", fontsize=8, color="#666666",
    )

    plt.tight_layout()
    plt.savefig(str(path), dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    log.info("PNG written: %s", path)
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export Neo4j dependency graph to GraphML, HTML, and PNG.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python export_graph_viz.py
  python export_graph_viz.py "my-app" --type application
  python export_graph_viz.py "web-server-01" --type server
  python export_graph_viz.py --no-png
  python export_graph_viz.py "my-app" --force-png
        """,
    )
    parser.add_argument(
        "name",
        nargs="?",
        default=None,
        help="Optional application or server name to filter the export. "
             "Omit for the full graph.",
    )
    parser.add_argument(
        "--type",
        choices=["application", "server"],
        default=None,
        help="Node type when --name is given. Required if name is ambiguous.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--no-png",
        action="store_true",
        help="Skip PNG export.",
    )
    parser.add_argument(
        "--force-png",
        action="store_true",
        help=f"Force PNG export even when node count exceeds {PNG_MAX_NODES}.",
    )
    return parser


def _resolve_node_type(session, name: str, node_type: str | None) -> str:
    """Auto-detect node type if not specified; exit if ambiguous or missing."""
    if node_type:
        return node_type

    result = session.run(
        "MATCH (n) WHERE (n:Application OR n:Server) "
        "AND toLower(n.name) = toLower($name) "
        "RETURN labels(n)[0] AS label LIMIT 1",
        name=name,
    ).data()

    if not result:
        log.error("'%s' not found in Neo4j as Application or Server.", name)
        sys.exit(1)

    detected = result[0]["label"].lower()
    log.info("Auto-detected node type: %s", detected)
    return detected


def _file_prefix(name: str | None) -> str:
    if not name:
        return "full_graph"
    import re
    return re.sub(r"[^\w\-]", "_", name).strip("_")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    driver, database = _load_driver()

    try:
        with driver.session(database=database) as session:

            # --- Fetch graph data ---
            if args.name:
                node_type = _resolve_node_type(session, args.name, args.type)
                nodes, edges = fetch_filtered_graph(session, args.name, node_type)
                title = f"Dependency Graph — {node_type.title()}: {args.name}"
            else:
                nodes, edges = fetch_full_graph(session)
                title = "Full Dependency Graph"

            if not nodes and not edges:
                log.error("No graph data found. Has the graph been loaded?")
                sys.exit(1)

            G = build_nx_graph(nodes, edges)
            log.info(
                "Graph built: %d nodes, %d edges",
                G.number_of_nodes(), G.number_of_edges(),
            )

            # --- Output file paths ---
            ts     = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            prefix = _file_prefix(args.name)
            stem   = f"{prefix}_{ts}"
            outdir = args.output_dir

            # --- Exports ---
            graphml_path = export_graphml(G, outdir / f"{stem}.graphml")
            html_path    = export_html(G, outdir / f"{stem}.html", title)

            png_path = None
            if not args.no_png:
                png_path = export_png(
                    G, outdir / f"{stem}.png", title,
                    force=args.force_png,
                )

            # --- Summary ---
            print()
            print(f"  Graph:   {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
            print(f"  GraphML: {graphml_path}")
            print(f"  HTML:    {html_path}")
            if png_path:
                print(f"  PNG:     {png_path}")
            elif args.no_png:
                print("  PNG:     skipped (--no-png)")
            else:
                print(f"  PNG:     skipped (>{PNG_MAX_NODES} nodes; use --force-png)")
            print()

    finally:
        driver.close()


if __name__ == "__main__":
    main()
