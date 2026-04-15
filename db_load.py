# -*- coding: utf-8 -*-
"""
db_load.py - Load competitor_unified.csv into CockroachDB encoders table.

Usage:
    python db_load.py              # load all rows
    python db_load.py --truncate   # drop existing rows first
    python db_load.py --batch 500  # custom batch size (default 200)
"""
import os
import sys
import argparse
import math

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

COCKROACHDB_URL = (
    "postgresql://saptak:xjsWn0LLpiIRk0dBGAnn1g"
    "@itchy-gazelle-24547.j77.aws-us-east-1.cockroachlabs.cloud:26257"
    "/defaultdb?sslmode=verify-full"
)

_HERE     = os.path.dirname(os.path.abspath(__file__))
CSV_PATH  = os.path.join(_HERE, "data", "competitor_unified.csv")

# Columns that map directly to DB columns (no 'id')
DB_COLS = [
    "manufacturer", "part_number", "product_family", "encoder_type",
    "sensing_method", "source_pdf", "order_pattern",
    "resolution_ppr", "ppr_range_min", "ppr_range_max", "is_programmable",
    "output_circuit_canonical", "output_signals", "num_output_channels", "max_output_freq_hz",
    "supply_voltage_min_v", "supply_voltage_max_v", "output_current_ma", "power_consumption_typ_mw",
    "reverse_polarity_protection", "short_circuit_protection",
    "housing_diameter_mm", "shaft_diameter_mm", "shaft_type", "flange_type",
    "connection_type", "connector_pins",
    "ip_rating", "operating_temp_min_c", "operating_temp_max_c",
    "max_speed_rpm_peak", "shock_resistance", "vibration_resistance",
    "weight_g", "startup_torque_ncm", "shaft_load_radial_n", "shaft_load_axial_n",
    "moment_of_inertia",
    "oc_shaft_type", "oc_flange", "oc_ppr", "oc_interface", "oc_connector",
]

BOOL_COLS  = {"is_programmable", "reverse_polarity_protection", "short_circuit_protection"}
FLOAT_COLS = {
    "resolution_ppr", "ppr_range_min", "ppr_range_max",
    "num_output_channels", "max_output_freq_hz",
    "supply_voltage_min_v", "supply_voltage_max_v",
    "output_current_ma", "power_consumption_typ_mw",
    "housing_diameter_mm", "shaft_diameter_mm", "connector_pins",
    "ip_rating", "operating_temp_min_c", "operating_temp_max_c",
    "max_speed_rpm_peak", "weight_g", "startup_torque_ncm",
    "shaft_load_radial_n", "shaft_load_axial_n",
}


def _clean_row(row: dict) -> tuple:
    """Convert a DataFrame row dict to a tuple matching DB_COLS order."""
    vals = []
    for col in DB_COLS:
        v = row.get(col)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            vals.append(None)
        elif col in BOOL_COLS:
            vals.append(bool(v) if str(v).strip().lower() not in ("nan", "none", "") else None)
        elif col in FLOAT_COLS:
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                vals.append(None)
        else:
            s = str(v).strip()
            vals.append(s if s not in ("nan", "None", "") else None)
    return tuple(vals)


def _get_conn():
    """Return a fresh psycopg2 connection."""
    return psycopg2.connect(COCKROACHDB_URL)


def load(truncate: bool = False, batch_size: int = 300, reconnect_every: int = 100, start_row: int = 0):
    """
    Load competitor_unified.csv into CockroachDB.
    reconnects every `reconnect_every` batches to avoid server-side timeout drops.
    """
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        print("Run  python competitor_etl.py  first.")
        sys.exit(1)

    print(f"Loading {CSV_PATH} ...")
    df = pd.read_csv(CSV_PATH, low_memory=False)
    for col in DB_COLS:
        if col not in df.columns:
            df[col] = None
    df = df[DB_COLS]
    if start_row > 0:
        df = df.iloc[start_row:].reset_index(drop=True)
        print(f"  Skipping first {start_row:,} rows (resume mode)")
    total = len(df)
    print(f"  {total:,} rows to load")

    print("Connecting to CockroachDB ...")
    conn = _get_conn()
    conn.autocommit = False
    cur  = conn.cursor()

    if truncate:
        print("  Truncating existing data ...")
        cur.execute("DELETE FROM encoders WHERE 1=1;")
        conn.commit()
        print("  Done.")

    insert_sql = f"""
        INSERT INTO encoders ({', '.join(DB_COLS)})
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    batches = math.ceil(total / batch_size)
    loaded  = 0
    errors  = 0

    for i in range(batches):
        # Reconnect periodically to avoid server-side timeout
        if i > 0 and i % reconnect_every == 0:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
            conn = _get_conn()
            conn.autocommit = False
            cur  = conn.cursor()

        chunk = df.iloc[i * batch_size : (i + 1) * batch_size]
        rows  = [_clean_row(r) for r in chunk.to_dict("records")]
        try:
            execute_values(cur, insert_sql, rows)
            conn.commit()
            loaded += len(rows)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"\n  Batch {i+1} error: {e}")
            errors += 1
            # Reconnect after error
            try:
                cur.close(); conn.close()
            except Exception:
                pass
            conn = _get_conn()
            conn.autocommit = False
            cur  = conn.cursor()
            continue

        pct = loaded / total * 100
        print(f"  Batch {i+1}/{batches}  ({loaded:,}/{total:,}  {pct:.1f}%)", end="\r", flush=True)

    print(f"\nLoaded {loaded:,} rows into CockroachDB. Errors: {errors}")
    try:
        cur.close()
        conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--truncate",  action="store_true", help="Delete existing rows first")
    ap.add_argument("--batch",     type=int, default=300, help="Batch size (default 300)")
    ap.add_argument("--resume",    action="store_true",
                    help="Skip rows already in DB (checks row count and starts from that offset)")
    args = ap.parse_args()

    if args.resume:
        # Find how many rows are already loaded and start from that offset
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM encoders")
        already = cur.fetchone()[0]
        cur.close(); conn.close()
        print(f"Resume mode: {already:,} rows already in DB, starting from row {already:,}")
        load(truncate=False, batch_size=args.batch, start_row=already)
    else:
        load(truncate=args.truncate, batch_size=args.batch)


def test_connection() -> bool:
    """Lightweight check that CockroachDB is reachable. Returns False if unavailable."""
    try:
        conn = _get_conn()
        conn.close()
        return True
    except Exception:
        return False
