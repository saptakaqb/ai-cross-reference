# -*- coding: utf-8 -*-
"""
db_load.py  —  DuckDB local database loader and query interface.

Replaces the former CockroachDB/psycopg2 implementation.

Usage:
    python db_load.py              # build encoder_crossref.duckdb from CSVs
    python db_load.py --rebuild    # force rebuild even if .duckdb exists
    python db_load.py --verify     # print row counts only, no build
"""
import os
import sys
import argparse
import time

import duckdb
import pandas as pd

# ── Paths ──────────────────────────────────────────────────────────────────────
_HERE          = os.path.dirname(os.path.abspath(__file__))
DB_PATH        = os.path.join(_HERE, "data", "encoder_crossref.duckdb")
COMPETITOR_CSV = os.path.join(_HERE, "data", "competitor_unified.csv")


def _csv_path(p: str) -> str:
    """DuckDB requires forward slashes even on Windows."""
    return p.replace("\\", "/")


# ── Build / rebuild ────────────────────────────────────────────────────────────
def _build_db():
    """
    Ingest competitor_unified.csv into DuckDB.
    Called automatically when the .duckdb file is missing.
    """
    if not os.path.exists(COMPETITOR_CSV):
        print(f"ERROR: CSV not found at {COMPETITOR_CSV}")
        print("Run  python make_kubler_compatible_samples.py  first.")
        sys.exit(1)

    print(f"Building DuckDB at {DB_PATH} ...")
    t0 = time.time()
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    con = duckdb.connect(DB_PATH)

    # Load with all_varchar=True to avoid type-inference failures.
    # DuckDB samples only the first ~20K rows for type detection. Mixed-type
    # columns (e.g. oc_ppr has numeric PPR values AND string codes like "AD")
    # cause ConversionException deeper in the file. all_varchar sidesteps this.
    # The app/matcher work from pandas DataFrames where types are already
    # handled correctly by the ETL scripts.
    print(f"  Loading {COMPETITOR_CSV} ...")
    cp = _csv_path(COMPETITOR_CSV)
    con.execute(f"""
        CREATE OR REPLACE TABLE competitor AS
        SELECT * FROM read_csv(
            '{cp}',
            header       = True,
            null_padding = True,
            all_varchar  = True
        )
    """)

    # kubler_unified.csv — load separately if it exists
    kubler_csv = os.path.join(_HERE, "data", "kubler_unified.csv")
    if os.path.exists(kubler_csv):
        print(f"  Loading {kubler_csv} ...")
        kp = _csv_path(kubler_csv)
        con.execute(f"""
            CREATE OR REPLACE TABLE kubler AS
            SELECT * FROM read_csv(
                '{kp}',
                header       = True,
                null_padding = True,
                all_varchar  = True
            )
        """)
    else:
        # Kubler rows already inside competitor_unified — create a view
        con.execute("""
            CREATE OR REPLACE VIEW kubler AS
            SELECT * FROM competitor WHERE manufacturer = 'Kubler'
        """)

    # Indexes on T1/T2 filter columns
    print("  Creating indexes ...")
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_manufacturer  ON competitor(manufacturer)",
        "CREATE INDEX IF NOT EXISTS idx_shaft_type    ON competitor(shaft_type)",
        "CREATE INDEX IF NOT EXISTS idx_housing       ON competitor(housing_diameter_mm)",
        "CREATE INDEX IF NOT EXISTS idx_shaft_d       ON competitor(shaft_diameter_mm)",
        "CREATE INDEX IF NOT EXISTS idx_output_circ   ON competitor(output_circuit_canonical)",
        "CREATE INDEX IF NOT EXISTS idx_ppr           ON competitor(resolution_ppr)",
        "CREATE INDEX IF NOT EXISTS idx_ip            ON competitor(ip_rating)",
    ]:
        try:
            con.execute(idx_sql)
        except Exception:
            pass  # column may not exist in all schema versions

    con.close()
    elapsed = time.time() - t0
    print(f"  Done in {elapsed:.1f}s")
    _verify(verbose=True)


def _verify(verbose=False):
    """Print row counts per manufacturer."""
    if not os.path.exists(DB_PATH):
        print("No DuckDB file found.")
        return
    con = duckdb.connect(DB_PATH, read_only=True)
    total = con.execute("SELECT COUNT(*) FROM competitor").fetchone()[0]
    rows  = con.execute(
        "SELECT manufacturer, COUNT(*) AS cnt FROM competitor "
        "GROUP BY manufacturer ORDER BY cnt DESC"
    ).fetchall()
    con.close()
    if verbose:
        print(f"\ncompetitor table: {total:,} rows")
        for mfr, cnt in rows:
            print(f"  {str(mfr):<30s}: {cnt:>8,}")


# ── Data loading (called by streamlit_app.py) ──────────────────────────────────
def load_all_as_df() -> pd.DataFrame:
    """
    Load the full competitor table into a pandas DataFrame.
    Replaces the old  pd.read_csv('competitor_unified.csv').
    Called once at Streamlit startup, cached with @st.cache_resource.
    """
    if not os.path.exists(DB_PATH):
        _build_db()
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("SELECT * FROM competitor").fetchdf()
    con.close()
    return df


def test_connection() -> bool:
    """
    Lightweight check used by streamlit_app.py for the DB status indicator.
    Returns True if the DuckDB file exists and is readable.
    """
    try:
        if not os.path.exists(DB_PATH):
            return False
        con = duckdb.connect(DB_PATH, read_only=True)
        con.execute("SELECT 1").fetchone()
        con.close()
        return True
    except Exception:
        return False


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build or verify the encoder DuckDB database.")
    ap.add_argument("--rebuild", action="store_true", help="Force rebuild even if .duckdb exists")
    ap.add_argument("--verify",  action="store_true", help="Print row counts only, no build")
    args = ap.parse_args()

    if args.verify:
        _verify(verbose=True)
    elif args.rebuild or not os.path.exists(DB_PATH):
        if args.rebuild and os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            print(f"Removed existing {DB_PATH}")
        _build_db()
    else:
        print(f"DuckDB already exists at {DB_PATH}")
        print("Use --rebuild to force a fresh build, or --verify to check row counts.")
        _verify(verbose=True)
