# -*- coding: utf-8 -*-
"""
posital_etl.py
==============
ETL script to convert Posital incremental encoder CSVs → posital_unified.csv
in the unified 44-column schema (CANONICAL_COLUMNS + product_url).

Usage:
    python posital_etl.py --input-dir <dir_with_csvs> --output posital_unified.csv

Handles the irregular CSV structure (rows have 99–144 columns depending on
product variant); uses the csv module directly since pandas read_csv fails
on mismatched column counts.
"""
import os
import re
import csv
import argparse
import sys

# ── Inline parsers (no dependency on normalizer to keep ETL standalone) ───────

def _safe_float(s):
    if s is None: return None
    try:
        f = float(str(s).strip())
        import math
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None

def _first_num(s):
    """Extract first decimal number from string."""
    if not s: return None
    m = re.search(r'[\d]+(?:[.,]\d+)?', str(s))
    if m:
        try: return float(m.group().replace(',','.'))
        except: return None
    return None

def _parse_voltage(s):
    """Return (min_v, max_v) from '4.75 – 30 VDC' or '10...30 VDC'."""
    if not s or str(s).strip() in ('', 'nan', 'None'): return None, None
    s = str(s).strip()
    m = re.search(r'([\d.]+)\s*[–\-\.]{1,3}\s*([\d.]+)', s)
    if m:
        return float(m.group(1)), float(m.group(2))
    n = _first_num(s)
    return (n, n) if n else (None, None)

def _parse_temp(s):
    """Return (min_c, max_c) from '-40 °C (-40 °F)' or '+85 °C (+185 °F)'."""
    if not s or str(s).strip() in ('', 'nan', 'None'): return None, None
    s = str(s)
    # Strip Fahrenheit in parentheses
    s = re.sub(r'\([^)]*°?F[^)]*\)', '', s, flags=re.I)
    nums = []
    for m in re.finditer(r'[+\-]?\d+(?:\.\d+)?', s):
        try:
            v = float(m.group())
            if -100 <= v <= 250: nums.append(v)
        except: pass
    if len(nums) >= 2: return min(nums), max(nums)
    if len(nums) == 1: return (nums[0], None) if nums[0] < 0 else (None, nums[0])
    return None, None

def _parse_ip(s):
    """Return highest numeric IP rating found, or None."""
    if not s or str(s).strip() in ('', 'nan', 'None'): return None
    matches = re.findall(r'IP\s?(\d{2,3})', str(s).upper())
    vals = []
    for m in matches:
        try: vals.append(int(m.rstrip('K')))
        except: pass
    return min(vals) if vals else None   # take the lower (more conservative) rating

def _parse_speed(s):
    """Extract max speed in RPM from '≤ 12000 1/min'."""
    if not s or str(s).strip() in ('', 'nan', 'None'): return None
    n = _first_num(str(s).replace('≤','').replace('<','').strip())
    return n if n and 0 < n < 200_000 else None

def _parse_shaft(s):
    """Return (shaft_type, shaft_diameter_mm) from Posital shaft description."""
    if not s or str(s).strip() in ('', 'nan'): return None, None
    sl = s.lower()
    if 'through' in sl:
        st = 'Through Hollow'
    elif 'hollow' in sl or 'blind' in sl:
        st = 'Hollow'
    elif 'solid' in sl:
        st = 'Solid'
    else:
        st = None
    # diameter: "ø 6 mm" or "6.35 mm"
    m = re.search(r'(?:ø\s*)?([\d.]+)\s*mm', s)
    dia = float(m.group(1)) if m else None
    return st, dia

def _parse_shaft_dia_col(s):
    """ø 6 mm (0.24") -> 6.0"""
    if not s: return None
    m = re.search(r'(?:ø\s*)?([\d.]+)\s*mm', str(s))
    return float(m.group(1)) if m else None

def _parse_housing_dia(s):
    """'Synchro, ø 58 mm (Y)' -> (flange_type, 58.0)"""
    if not s or str(s).strip() in ('', 'nan'): return None, None
    sl = s.lower()
    if 'synchro' in sl: ft = 'Synchro'
    elif 'clamp' in sl: ft = 'Clamping'
    elif 'square' in sl: ft = 'Square'
    elif 'face' in sl: ft = 'Face Mount'
    elif 'hollow' in sl: ft = 'Hollow Shaft'
    elif 'stator' in sl: ft = 'Stator Coupler'
    else: ft = s.split(',')[0].strip()
    m = re.search(r'(?:ø\s*)?([\d.]+)\s*mm', s)
    dia = float(m.group(1)) if m else None
    return ft, dia

def _parse_ppr_range(s):
    """'PPR (1-32768), Output, ...' -> (True, 1, 32768)"""
    if not s or str(s).strip() in ('', 'nan'): return False, None, None
    m = re.search(r'PPR\s*\((\d+)[\-–](\d+)\)', str(s), re.I)
    if m:
        return True, int(m.group(1)), int(m.group(2))
    return False, None, None

def _parse_ppr_fixed(s):
    """'2048' or '1000 PPR' -> 2048.0"""
    if not s or str(s).strip() in ('', 'nan'): return None
    n = _first_num(str(s))
    return n if n and n > 0 else None

def _parse_output_circuit(s):
    """Map Posital output driver string to canonical output circuit."""
    if not s or str(s).strip() in ('', 'nan'): return None
    sl = s.lower().strip()
    if 'push-pull (htl) or rs' in sl or 'htl or ttl' in sl or 'ttl or htl' in sl:
        return 'TTL/HTL Universal'
    if 'push-pull (htl)' in sl or 'htl' in sl:
        return 'Push-Pull'
    if 'rs 422' in sl or 'rs422' in sl or 'ttl' in sl:
        return 'TTL RS422'
    if 'push-pull' in sl:
        return 'Push-Pull'
    if 'open collector' in sl or 'nch' in sl:
        return 'Open Collector'
    return s.strip()

def _parse_weight(s):
    """'270 g (0.60 lb)' -> 270.0"""
    if not s or str(s).strip() in ('', 'nan'): return None
    # kg first
    m = re.search(r'([\d.]+)\s*kg', str(s), re.I)
    if m: return float(m.group(1)) * 1000
    m = re.search(r'([\d.]+)\s*g', str(s), re.I)
    if m: return float(m.group(1))
    return None

def _parse_connection(s_connector, s_conn_type=None):
    """Determine canonical connection type."""
    combined = f"{s_connector or ''} {s_conn_type or ''}".lower()
    if 'm23' in combined: return 'M23'
    if 'm12' in combined: return 'M12'
    if 'm8' in combined: return 'M8'
    if 'm17' in combined: return 'M17'
    if 'terminal' in combined: return 'Terminal'
    if 'cable' in combined: return 'Cable'
    if 'connector' in combined: return 'Connector'
    return None

def _parse_pins(s):
    if not s: return None
    m = re.search(r'(\d+)\s*pin', str(s), re.I)
    return int(m.group(1)) if m else None

# ── Column index helpers ───────────────────────────────────────────────────────

def _col_idx(header, name):
    """Return first index of column named `name` (case-insensitive), or -1."""
    nl = name.lower().strip()
    for i, h in enumerate(header):
        if h.lower().strip() == nl:
            return i
    return -1

def _get(row, idx):
    """Safe row access by index."""
    if idx < 0 or idx >= len(row): return None
    v = row[idx].strip()
    return None if v in ('', 'nan', 'None', '-', 'N/A', 'n/a') else v

# ── File-level product family name from filename ───────────────────────────────
FILENAME_TO_FAMILY = {
    'atex':            'ATEX',
    'compact_magnetic':'Compact Magnetic',
    'cube_and_square': 'Cube and Square',
    'heavy_duty':      'Heavy Duty',
    'industry_classic':'Industry Classic',
    'through_hollow':  'Through Hollow',
}

def _product_family(filename):
    fn = os.path.basename(filename).lower()
    for key, val in FILENAME_TO_FAMILY.items():
        if key in fn:
            return val
    return 'Posital'

# ── CANONICAL_COLUMNS (44 cols, schema v15) ────────────────────────────────────
CANONICAL_COLUMNS = [
    "manufacturer","part_number","product_family","encoder_type","sensing_method",
    "source_pdf","order_pattern","product_url",
    "resolution_ppr","ppr_range_min","ppr_range_max",
    "is_programmable","output_circuit_canonical","output_signals","num_output_channels",
    "max_output_freq_hz","supply_voltage_min_v","supply_voltage_max_v","output_current_ma",
    "power_consumption_typ_mw","reverse_polarity_protection","short_circuit_protection",
    "housing_diameter_mm","shaft_diameter_mm","shaft_type","flange_type","connection_type",
    "connector_pins","ip_rating","operating_temp_min_c","operating_temp_max_c",
    "max_speed_rpm_peak","shock_resistance","vibration_resistance","weight_g",
    "startup_torque_ncm","shaft_load_radial_n","shaft_load_axial_n","moment_of_inertia",
    "oc_shaft_type","oc_flange","oc_ppr","oc_interface","oc_connector",
]

# ── Per-row ETL ────────────────────────────────────────────────────────────────

def etl_row(row, header, product_family, product_url_col_idx):
    """Convert one raw CSV row to canonical dict."""
    def g(name): return _get(row, _col_idx(header, name))

    pn  = _get(row, 0)
    url = _get(row, product_url_col_idx) if product_url_col_idx >= 0 else None

    # PPR / programmable
    prog_str = g('Interface | Programming Functions')
    ppr_col  = g('Interface | Pulses per Revolution')  # fixed PPR (Heavy Duty / Through Hollow)
    is_prog, ppr_min, ppr_max = _parse_ppr_range(prog_str)
    if is_prog:
        resolution_ppr, ppr_range_min, ppr_range_max = None, ppr_min, ppr_max
    else:
        resolution_ppr = _parse_ppr_fixed(ppr_col)
        ppr_range_min  = None
        ppr_range_max  = None

    # Shaft type + diameter
    shaft_type_raw = g('Interface | Shaft Type')
    shaft_dia_raw  = g('Interface | Shaft Diameter')
    shaft_type, shaft_dia_from_type = _parse_shaft(shaft_type_raw)
    shaft_dia = _parse_shaft_dia_col(shaft_dia_raw) or shaft_dia_from_type

    # Housing diameter + flange type
    flange_raw = g('Interface | Flange Type')
    flange_type, housing_dia = _parse_housing_dia(flange_raw)

    # Output circuit
    oc_raw = g('Interface | Output Driver')
    output_circuit = _parse_output_circuit(oc_raw)

    # Supply voltage
    vmin, vmax = _parse_voltage(g('Interface | Supply Voltage'))

    # IP rating — take minimum of shaft and housing (conservative)
    ip_shaft   = _parse_ip(g('Interface | Protection Class (Shaft)'))
    ip_housing = _parse_ip(g('Interface | Protection Class (Housing)'))
    ip_vals = [v for v in [ip_shaft, ip_housing] if v is not None]
    ip_rating = min(ip_vals) if ip_vals else None

    # Temperature
    tmin, tmax = _parse_temp(g('Interface | Min Temperature'))
    _, tmax2   = _parse_temp(g('Interface | Max Temperature'))
    if tmax is None: tmax = tmax2

    # Speed
    max_speed = _parse_speed(g('Interface | Max. Permissible Mechanical Speed'))

    # Weight
    weight = _parse_weight(g('Interface | Weight'))

    # Shock / vibration
    shock     = g('Interface | Shock Resistance')
    vibration = g('Interface | Vibration Resistance')

    # Connection type
    connector = g('Interface | Connector')
    conn_type_raw = g('Interface | Connection Type')
    connection_type = _parse_connection(connector, conn_type_raw)
    connector_pins  = _parse_pins(connector)

    # Shaft load
    load_raw = g('Interface | Max. Shaft Load')
    # Try to extract radial / axial from combined string like "40N / 20N"
    shaft_load_radial = shaft_load_axial = None
    if load_raw:
        nums = re.findall(r'[\d.]+', load_raw)
        if len(nums) >= 2:
            shaft_load_radial, shaft_load_axial = float(nums[0]), float(nums[1])
        elif len(nums) == 1:
            shaft_load_radial = float(nums[0])

    # Moment of inertia
    inertia = g('Interface | Rotor Inertia')

    # Startup torque
    torque_raw = g('Interface | Friction Torque')
    startup_torque = None
    if torque_raw:
        m = re.search(r'([\d.]+)\s*Ncm', torque_raw, re.I)
        if m: startup_torque = float(m.group(1))
        else:
            m = re.search(r'([\d.]+)\s*Nm', torque_raw, re.I)
            if m: startup_torque = float(m.group(1)) * 100

    # Output frequency
    freq_raw = g('Interface | Maximum Frequency Response')
    max_freq = None
    if freq_raw:
        m = re.search(r'([\d.]+)\s*(kHz|MHz|Hz)', freq_raw, re.I)
        if m:
            val, unit = float(m.group(1)), m.group(2).upper()
            max_freq = val * 1_000_000 if unit == 'MHZ' else val * 1_000 if unit == 'KHZ' else val

    # Output current
    current_raw = g('Interface | Maximum Switching Current')
    current_ma = None
    if current_raw:
        m = re.search(r'([\d.]+)\s*mA', current_raw, re.I)
        if m: current_ma = float(m.group(1))

    # Technology / sensing
    technology = g('Interface | Technology')  # Magnetic / Optical

    # Encoder type: programmable incremental vs incremental
    enc_type_raw = g('Interface | Interface')
    encoder_type = enc_type_raw or 'Incremental'

    return {
        "manufacturer":              "Posital",
        "part_number":               pn,
        "product_family":            product_family,
        "encoder_type":              encoder_type,
        "sensing_method":            technology,
        "source_pdf":                None,
        "order_pattern":             None,
        "product_url":               url,
        "resolution_ppr":            resolution_ppr,
        "ppr_range_min":             ppr_range_min,
        "ppr_range_max":             ppr_range_max,
        "is_programmable":           is_prog,
        "output_circuit_canonical":  output_circuit,
        "output_signals":            g('Interface | Output Incremental'),
        "num_output_channels":       None,
        "max_output_freq_hz":        max_freq,
        "supply_voltage_min_v":      vmin,
        "supply_voltage_max_v":      vmax,
        "output_current_ma":         current_ma,
        "power_consumption_typ_mw":  None,
        "reverse_polarity_protection": g('Interface | Reverse Polarity Protection'),
        "short_circuit_protection":  g('Interface | Short Circuit Protection'),
        "housing_diameter_mm":       housing_dia,
        "shaft_diameter_mm":         shaft_dia,
        "shaft_type":                shaft_type,
        "flange_type":               flange_type,
        "connection_type":           connection_type,
        "connector_pins":            connector_pins,
        "ip_rating":                 ip_rating,
        "operating_temp_min_c":      tmin,
        "operating_temp_max_c":      tmax,
        "max_speed_rpm_peak":        max_speed,
        "shock_resistance":          shock,
        "vibration_resistance":      vibration,
        "weight_g":                  weight,
        "startup_torque_ncm":        startup_torque,
        "shaft_load_radial_n":       shaft_load_radial,
        "shaft_load_axial_n":        shaft_load_axial,
        "moment_of_inertia":         inertia,
        "oc_shaft_type":             None,
        "oc_flange":                 None,
        "oc_ppr":                    None,
        "oc_interface":              None,
        "oc_connector":              None,
    }


# ── Main ETL ───────────────────────────────────────────────────────────────────

def run_etl(input_dir, output_path):
    csv_files = [
        f for f in os.listdir(input_dir)
        if f.lower().startswith('posital_products') and f.lower().endswith('.csv')
    ]
    if not csv_files:
        print(f"ERROR: No posital_products*.csv files found in {input_dir}")
        sys.exit(1)

    print(f"Found {len(csv_files)} Posital CSV files:")
    for f in sorted(csv_files):
        print(f"  {f}")

    total_rows = 0
    skipped    = 0

    with open(output_path, 'w', newline='', encoding='utf-8') as out_f:
        writer = csv.DictWriter(out_f, fieldnames=CANONICAL_COLUMNS)
        writer.writeheader()

        for fname in sorted(csv_files):
            filepath = os.path.join(input_dir, fname)
            family   = _product_family(fname)

            with open(filepath, 'r', encoding='utf-8') as in_f:
                reader = csv.reader(in_f)
                rows   = list(reader)

            if len(rows) < 2:
                print(f"  Skipping {fname} (empty)")
                continue

            header = rows[0]
            url_idx = 1  # Product URL is always column index 1

            file_rows = 0
            for raw_row in rows[1:]:
                if not raw_row or not raw_row[0].strip():
                    continue
                try:
                    rec = etl_row(raw_row, header, family, url_idx)
                    if rec.get('part_number'):
                        writer.writerow(rec)
                        file_rows  += 1
                        total_rows += 1
                    else:
                        skipped += 1
                except Exception as e:
                    skipped += 1
                    # Uncomment for debugging: print(f"    SKIP row: {e}")

            print(f"  {fname}: {file_rows} rows written ({family})")

    print(f"\nDone: {total_rows} rows written to {output_path}  ({skipped} skipped)")
    return total_rows


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Posital incremental encoder ETL')
    ap.add_argument('--input-dir', default='.', help='Directory containing Posital CSV files')
    ap.add_argument('--output',    default='data/posital_unified.csv', help='Output CSV path')
    args = ap.parse_args()
    run_etl(args.input_dir, args.output)
