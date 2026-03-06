# Session Log — TM Dependency Mapping

_Last updated: 2026-03-06_

---

## What Was Built

This session produced a complete greenfield pipeline from raw exports to a
queryable, visualizable dependency graph. All code is in `scripts/` and no
input data exists yet — the pipeline is ready and waiting for real files.

### Pipeline overview

```
inputs/azure_migrate/   inputs/device42/
        │                       │
        ▼                       ▼
ingest_azure_migrate.py   ingest_inventory.py
        │                       │
        ▼                       ▼
outputs/servers_clean.parquet   outputs/inventory_clean.parquet
                │                        │
                └──────────┬─────────────┘
                           ▼
              build_app_server_mapping.py
                           │
              ┌────────────┴───────────────────┐
              ▼                                ▼
outputs/app_server_mapping.parquet   outputs/summaries/match_report.md
              │
              ▼
         load_neo4j.py  ──→  Neo4j (bolt://localhost:7687)
              │
    ┌─────────┴──────────────┐
    ▼                        ▼
query_dependencies.py   export_graph_viz.py
    │                        │
    ▼                        ▼
outputs/reports/         outputs/graphs/
  *.json                   *.graphml
                           *.html
                           *.png
```

---

### Files created

| File | Lines | Purpose |
| --- | --- | --- |
| `scripts/ingest/ingest_azure_migrate.py` | 193 | Read CSV/JSON from `inputs/azure_migrate/`, normalize server names, write `servers_clean.parquet` |
| `scripts/ingest/ingest_inventory.py` | 189 | Read CSV/Excel from `inputs/device42/`, normalize server names, write `inventory_clean.parquet` |
| `scripts/ingest/build_app_server_mapping.py` | 468 | Exact + fuzzy join on server name, write `app_server_mapping.parquet` + `match_report.md` |
| `scripts/ingest/load_neo4j.py` | 562 | Load parquets into Neo4j; create Server/Application nodes and RUNS_ON/COMMUNICATES_WITH relationships |
| `scripts/analysis/query_dependencies.py` | 554 | CLI: answer 4 dependency questions for a given app or server; output terminal + JSON |
| `scripts/analysis/export_graph_viz.py` | 661 | CLI: export full or filtered graph to GraphML, interactive HTML (pyvis), and PNG |
| `requirements.txt` | — | All 9 dependencies pinned with minimum versions |
| `.env.example` | — | Template for Neo4j credentials |
| `.gitignore` | — | Excludes `.env`, parquet outputs, `__pycache__`, `.DS_Store` |

**Total:** ~2,627 lines of Python across 6 scripts.

---

### Dependencies installed (all confirmed working)

| Package | Version | Role |
| --- | --- | --- |
| pandas | 2.2.3 | DataFrame I/O throughout |
| pyarrow | 23.0.1 | Parquet read/write |
| rapidfuzz | 3.14.3 | Fuzzy server name matching (WRatio scorer) |
| openpyxl | 3.1.5 | Excel inventory file reading |
| neo4j | 6.1.0 | Neo4j Python driver |
| python-dotenv | 1.2.2 | Credential loading from `.env` |
| networkx | 3.6.1 | Graph model, GraphML export, layout |
| pyvis | 0.3.2 | Interactive HTML / vis.js export |
| matplotlib | 3.10.8 | Static PNG export |

---

### Key design decisions

- **Normalization is identical everywhere**: `str.strip().str.lower()` applied in all
  three ingest/mapping scripts so joins are consistent.
- **Column detection is flexible**: every script probes a priority-ordered list of
  known column name variants (case-insensitive) rather than hardcoding. This avoids
  breakage when Azure Migrate or Device42 export formats differ between tenants.
- **Idempotency**: `load_neo4j.py` uses `MERGE` throughout — safe to re-run without
  creating duplicate nodes or relationships.
- **Credentials never hardcoded**: `.env` file with `python-dotenv`; `.env` is in
  `.gitignore`.
- **Graceful degradation**: every script handles missing files, empty DataFrames, and
  missing optional packages (e.g., rapidfuzz absent → exact-match only; PNG skipped
  above 300 nodes).
- **Batched Neo4j writes**: `UNWIND` with `BATCH_SIZE = 500` rows per transaction.

---

## What Is Incomplete / Not Yet Tested

| Gap | Detail |
| --- | --- |
| **No real input data** | `inputs/azure_migrate/` and `inputs/device42/` are empty. Every script has been written and reviewed but none has run against real data. |
| **Column names unverified** | The candidate column lists in all scripts are based on known Azure Migrate and Device42 export conventions. They will likely need tuning once real files arrive. |
| **`data_profile.md` missing** | Referenced in the original brief but never created. Should be generated after first real ingest run to document actual column names, row counts, and data quality. |
| **No Neo4j instance confirmed** | `load_neo4j.py` and both analysis scripts assume Neo4j is running locally. Whether a local instance is installed and configured is unknown. |
| **Network dependency files** | `load_neo4j.py` detects dependency CSVs by column shape. No sample file exists to confirm detection logic is correct. |
| **Tests** | No unit or integration tests written. |
| **`outputs/graphs/`** | Directory created but no exports generated yet (requires Neo4j data). |

---

## What Works Right Now (Without Data)

The following can be verified immediately:

```bash
# Syntax check all scripts
python -m py_compile scripts/ingest/ingest_azure_migrate.py
python -m py_compile scripts/ingest/ingest_inventory.py
python -m py_compile scripts/ingest/build_app_server_mapping.py
python -m py_compile scripts/ingest/load_neo4j.py
python -m py_compile scripts/analysis/query_dependencies.py
python -m py_compile scripts/analysis/export_graph_viz.py

# CLI help works without Neo4j
python scripts/analysis/query_dependencies.py --help
python scripts/analysis/export_graph_viz.py --help
```

---

## Recommended Next Steps

### Immediate (before anything else)

1. **Drop real files into the input directories:**
   - `inputs/azure_migrate/` — Azure Migrate server inventory CSV or JSON export
   - `inputs/device42/` — Device42 inventory CSV or Excel export
   - Optionally a network dependency CSV in `inputs/azure_migrate/` with
     source/destination server columns

2. **Run the ingest pipeline end-to-end:**
   ```bash
   python scripts/ingest/ingest_azure_migrate.py
   python scripts/ingest/ingest_inventory.py
   python scripts/ingest/build_app_server_mapping.py
   ```

3. **Review `outputs/summaries/match_report.md`** — it will show how many servers
   matched, fuzzy score distribution, and unmatched names. Use this to tune the
   column name candidate lists.

4. **Create `outputs/summaries/data_profile.md`** — document actual column names,
   row counts, null rates, and value samples from the real files. This should be
   generated once and kept as a reference for all future scripts.

### Short-term

5. **Install and configure Neo4j** (if not already running):
   ```bash
   brew install neo4j
   brew services start neo4j
   # then set your password at http://localhost:7474
   ```
   Copy `.env.example` → `.env` and fill in the password.

6. **Load the graph:**
   ```bash
   python scripts/ingest/load_neo4j.py
   ```

7. **Query and verify:**
   ```bash
   python scripts/analysis/query_dependencies.py "AppName"
   python scripts/analysis/export_graph_viz.py
   ```

### Future phases

8. **Add Transition Manager (TM) integration** — the core project goal. TM downloads
   contain the authoritative server→application mapping. A dedicated ingest script
   for TM format is needed, likely replacing or supplementing `ingest_inventory.py`.

9. **Add tests** — at minimum: normalization unit tests, column detection tests,
   and an integration test with synthetic parquet fixtures.

10. **Export to TM API** — once the dependency graph is validated, write a script
    to push applications and dependencies back into Transition Manager via its API.
