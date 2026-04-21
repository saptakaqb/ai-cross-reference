# -*- coding: utf-8 -*-
"""
db_setup.py - Create the 'encoders' table in CockroachDB.

Run once:  python db_setup.py
"""
import os
import psycopg2
from psycopg2 import sql

COCKROACHDB_URL = (
    "postgresql://saptak:xjsWn0LLpiIRk0dBGAnn1g"
    "@itchy-gazelle-24547.j77.aws-us-east-1.cockroachlabs.cloud:26257"
    "/defaultdb?sslmode=verify-full"
)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS encoders (
    id                          SERIAL PRIMARY KEY,

    -- Identity
    manufacturer                VARCHAR(64),
    part_number                 VARCHAR(128),
    product_family              VARCHAR(128),
    encoder_type                VARCHAR(64),
    sensing_method              VARCHAR(64),
    source_pdf                  TEXT,
    order_pattern               TEXT,

    -- Resolution
    resolution_ppr              FLOAT,
    ppr_range_min               FLOAT,
    ppr_range_max               FLOAT,
    is_programmable             BOOLEAN,

    -- Output
    output_circuit_canonical    VARCHAR(64),
    output_signals              TEXT,
    num_output_channels         FLOAT,
    max_output_freq_hz          FLOAT,

    -- Electrical
    supply_voltage_min_v        FLOAT,
    supply_voltage_max_v        FLOAT,
    output_current_ma           FLOAT,
    power_consumption_typ_mw    FLOAT,
    reverse_polarity_protection BOOLEAN,
    short_circuit_protection    BOOLEAN,

    -- Mechanical
    housing_diameter_mm         FLOAT,
    shaft_diameter_mm           FLOAT,
    shaft_type                  VARCHAR(64),
    flange_type                 VARCHAR(128),
    connection_type             VARCHAR(128),
    connector_pins              FLOAT,

    -- Environmental
    ip_rating                   FLOAT,
    operating_temp_min_c        FLOAT,
    operating_temp_max_c        FLOAT,
    max_speed_rpm_peak          FLOAT,
    shock_resistance            TEXT,
    vibration_resistance        TEXT,

    -- Physical
    weight_g                    FLOAT,
    startup_torque_ncm          FLOAT,
    shaft_load_radial_n         FLOAT,
    shaft_load_axial_n          FLOAT,
    moment_of_inertia           TEXT,

    -- Order code fields
    oc_shaft_type               TEXT,
    oc_flange                   TEXT,
    oc_ppr                      TEXT,
    oc_interface                TEXT,
    oc_connector                TEXT
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_enc_mfr       ON encoders (manufacturer);",
    "CREATE INDEX IF NOT EXISTS idx_enc_ppr       ON encoders (resolution_ppr);",
    "CREATE INDEX IF NOT EXISTS idx_enc_housing   ON encoders (housing_diameter_mm);",
    "CREATE INDEX IF NOT EXISTS idx_enc_shaft_d   ON encoders (shaft_diameter_mm);",
    "CREATE INDEX IF NOT EXISTS idx_enc_ip        ON encoders (ip_rating);",
    "CREATE INDEX IF NOT EXISTS idx_enc_oc        ON encoders (output_circuit_canonical);",
    "CREATE INDEX IF NOT EXISTS idx_enc_family    ON encoders (product_family);",
]


def setup():
    print(f"Connecting to CockroachDB ...")
    conn = psycopg2.connect(COCKROACHDB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    print("Creating table 'encoders' ...")
    cur.execute(CREATE_TABLE)
    print("  Table created (or already exists).")

    print("Creating indexes ...")
    for idx_sql in CREATE_INDEXES:
        cur.execute(idx_sql)
    print(f"  {len(CREATE_INDEXES)} indexes created.")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    setup()
