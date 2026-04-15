#!/usr/bin/env python3
"""
filter_lika.py
==============
Reads the raw Lika CSV (any size — processed in chunks) and outputs a
Kubler-compatible filtered sample in unified schema format.

Filtering logic:
  1. FAMILY FILTER  — Keep only 58mm-body families (match Kubler K58I / 5000 / 5020)
  2. CIRCUIT FILTER — Exclude NPN open-collector (N2) and PNP (P2); Kubler has none
  3. PPR FILTER     — Keep rows whose PPR is in Kubler's 52 discrete values,
                      OR whose Electrical-Resolution field lists Kubler PPR values,
                      OR which are programmable
  4. DEDUP          — One row per (Family, PPR, Circuit, ShaftDia, Protection)
                      eliminates cable-length explosion (C50 RC920…RC990 → 1 row)
  5. NON-NULL SCORE — When multiple rows have same dedup key, keep the richest one

Usage:
    python filter_lika.py \
        --input  /path/to/lika_full.csv \
        --output lika_filtered.csv \
        --target 100000            # default 100k; adjust to taste
        --chunksize 50000          # rows per chunk; lower if RAM is tight

Output columns: unified schema (43 cols, same as competitor_unified.csv)
"""

import argparse, re, sys
import pandas as pd
import numpy as np

# ── Kubler reference universe ─────────────────────────────────────────────────
KUBLER_PPR = frozenset({
    1,2,4,5,10,12,14,20,25,28,30,32,36,50,60,64,80,
    100,120,125,150,180,200,240,250,256,300,342,360,375,
    400,500,512,600,625,720,800,900,1000,1024,1200,1250,
    1500,1800,2000,2048,2500,3000,3600,4000,4096,5000,
})

# ── 58mm-body Lika families ───────────────────────────────────────────────────
# Families where the flange/body diameter is 58mm → matches Kubler K58I / 5000 / 5020
# Solid-shaft: C58, C58A, C58R, CK58, MC58, CX58, CKP58, CKQ58
# Hollow-shaft: I58, I58S, I58R, MI58, MI58S, CK58(hollow), IX58, IX58S, IP58, IP58S,
#               IQ58, IQ58S, I58SK
FAMILIES_58MM = {
    'C58', 'C58A', 'C58R', 'CK58', 'MC58', 'CX58', 'CKP58', 'CKQ58',
    'I58', 'I58S', 'I58R', 'MI58', 'MI58S', 'IX58', 'IX58S',
    'IP58', 'IP58S', 'IQ58', 'IQ58S', 'I58SK',
}

# ── Circuits to EXCLUDE (Kubler has no NPN or PNP OC products) ───────────────
EXCLUDE_CIRCUIT_CODES = {'N2', 'P2'}   # NPN o.c., PNP o.c.

# ── Unified output schema ─────────────────────────────────────────────────────
COLS = [
    'manufacturer','part_number','product_family','encoder_type','sensing_method',
    'source_pdf','order_pattern','resolution_ppr','ppr_range_min','ppr_range_max',
    'is_programmable','output_circuit_canonical','output_signals','num_output_channels',
    'max_output_freq_hz','supply_voltage_min_v','supply_voltage_max_v',
    'output_current_ma','power_consumption_typ_mw','reverse_polarity_protection',
    'short_circuit_protection','housing_diameter_mm','shaft_diameter_mm','shaft_type',
    'flange_type','connection_type','connector_pins','ip_rating',
    'operating_temp_min_c','operating_temp_max_c','max_speed_rpm_peak',
    'shock_resistance','vibration_resistance','weight_g','startup_torque_ncm',
    'shaft_load_radial_n','shaft_load_axial_n','moment_of_inertia',
    'oc_shaft_type','oc_flange','oc_ppr','oc_interface','oc_connector',
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def _parse_v(s):
    if pd.isna(s): return np.nan, np.nan
    nums = re.findall(r'\d+(?:\.\d+)?', str(s))
    f = [float(n) for n in nums if float(n) < 1000]
    if len(f) == 1: return f[0], f[0]
    if len(f) >= 2: return f[0], f[-1]
    return np.nan, np.nan

def _parse_ip(s):
    if pd.isna(s): return np.nan
    nums = re.findall(r'\d{2,3}', str(s))
    vals = [int(n) for n in nums if 40 <= int(n) <= 69]
    return float(max(vals)) if vals else np.nan

def _parse_temp(s):
    if pd.isna(s): return np.nan, np.nan
    nums = re.findall(r'[+-]?\d+', str(s))
    floats = [float(n) for n in nums if -100 <= float(n) <= 200]
    if len(floats) >= 2: return min(floats), max(floats)
    return np.nan, np.nan

def _parse_shaft_mm(s):
    """Extract numeric shaft diameter from strings like '06 = 6mm', 'P9 = 9.52 mm'."""
    if pd.isna(s): return np.nan
    m = re.search(r'(?:=|Ø)\s*(\d+(?:\.\d+)?)', str(s))
    if m: return float(m.group(1))
    m = re.search(r'^(\d+(?:\.\d+)?)', str(s).strip())
    if m: return float(m.group(1))
    return np.nan

def _parse_weight(s):
    if pd.isna(s): return np.nan
    nums = re.findall(r'\d+(?:\.\d+)?', str(s))
    if not nums: return np.nan
    v = float(nums[0])
    # Lika weight is in grams or kg — ~100g is typical
    if v > 10: return v          # already in grams
    return v * 1000              # kg → g

def _parse_torque(s):
    """Extract Ncm value from '≤ 0,25 Ncm' or '0.25 Ncm'."""
    if pd.isna(s): return np.nan
    s2 = str(s).replace(',', '.')
    m = re.search(r'(\d+(?:\.\d+)?)\s*[Nn][Cc][Mm]', s2)
    if m: return float(m.group(1))
    return np.nan

def _parse_speed(s):
    if pd.isna(s): return np.nan
    nums = re.findall(r'\d+', str(s))
    vals = [int(n) for n in nums if int(n) > 100]
    return float(max(vals)) if vals else np.nan

def _circuit_code(s):
    """Extract output circuit code like 'Y2', 'L1', 'N2', 'H4' from raw string."""
    if pd.isna(s): return None, np.nan
    raw = str(s).strip()
    m = re.match(r'^([A-Z]\d)', raw)
    code = m.group(1) if m else None
    return code, raw

def _canonical_circuit(s):
    """Map Lika output circuit field → unified canonical label."""
    if pd.isna(s): return np.nan
    sl = str(s).lower()
    if 'sin' in sl or '1 vpp' in sl: return 'Sin/Cos'
    if 'pp/ld' in sl or 'universal' in sl: return 'PP/LD Universal'
    if 'rs422' in sl or 'line driver' in sl:
        if '10v' in sl or '30v' in sl or '10 v' in sl: return 'TTL RS422'  # L2
        return 'TTL RS422'
    if 'push pull' in sl or 'push-pull' in sl: return 'Push-Pull'
    if 'npn' in sl or 'n.p.n' in sl or 'n.o.c' in sl: return 'NPN Open Collector'
    if 'pnp' in sl or 'p.n.p' in sl or 'p.o.c' in sl: return 'PNP Open Collector'
    if 'htl' in sl: return 'Push-Pull'
    return str(s).strip()

def _programmable(resolution_field):
    """Return (is_prog, ppr_min, ppr_max) from Electrical-Resolution string."""
    if pd.isna(resolution_field): return False, np.nan, np.nan
    s = str(resolution_field).lower()
    if 'programmable' in s or 'xxxxx' in s:
        nums = re.findall(r'\d+', s)
        big = [int(n) for n in nums if int(n) > 100]
        ppr_max = max(big) if big else np.nan
        return True, 1.0, float(ppr_max) if big else np.nan
    return False, np.nan, np.nan

def _shaft_type(family, hollow_col):
    """Determine solid vs hollow from family name and hollow shaft column.

    Hollow families (through-shaft, slide-on): I58, I58S, I58R, MI58, MI58S,
        IX58, IX58S, IP58, IP58S, IQ58, IQ58S, I58SK — start with I or MI
    Solid families: C58, C58A, C58R, CK58, CKP58, CKQ58, MC58, CX58
    """
    fam = str(family).strip().upper()
    # Families that start with 'I' followed by digits are hollow through-shaft
    if re.match(r'^I\d|^MI\d|^IX\d|^IQ\d|^IP\d', fam):
        return 'Hollow'
    # C-prefix families are solid shaft (C58, C58A, C58R, CK58, MC58, CX58, etc.)
    # Do NOT rely on hollow_col here — C58 datasheets list available hollow bore
    # reducing sleeves as an option but the encoder itself is a solid-shaft design
    return 'Solid'

def _parse_max_freq(s):
    if pd.isna(s): return np.nan
    nums = re.findall(r'\d+', str(s))
    vals = [int(n) for n in nums if int(n) > 0]
    if not vals: return np.nan
    v = max(vals)
    if 'mhz' in str(s).lower(): return v * 1_000_000
    if 'khz' in str(s).lower(): return v * 1_000
    return float(v)


# ── Row mapper ────────────────────────────────────────────────────────────────
def map_row(r):
    """Map one raw Lika row dict → unified schema dict."""
    rec = {c: np.nan for c in COLS}
    rec['manufacturer'] = 'Lika'
    rec['part_number']  = r.get('Part Number')
    rec['product_family'] = r.get('Family')
    rec['encoder_type'] = 'Incremental'
    rec['housing_diameter_mm'] = 58.0   # all rows passing family filter are 58mm

    # Circuit
    circ_raw = r.get('Output Circuits / Power Supply', '')
    circ_code, circ_full = _circuit_code(circ_raw)
    rec['output_circuit_canonical'] = _canonical_circuit(circ_raw)
    rec['oc_interface'] = circ_raw

    # PPR
    ppr_raw = r.get('Resolution (PPR)')
    ppr_num = pd.to_numeric(ppr_raw, errors='coerce')
    elec_res = r.get('Electrical - Resolution', '')
    is_prog, ppr_min, ppr_max = _programmable(elec_res)

    if not is_prog and pd.isna(ppr_num):
        is_prog, ppr_min, ppr_max = _programmable(ppr_raw)

    rec['resolution_ppr'] = ppr_num if pd.notna(ppr_num) else np.nan
    rec['is_programmable'] = is_prog
    rec['ppr_range_min']   = ppr_min
    rec['ppr_range_max']   = ppr_max
    rec['oc_ppr'] = ppr_raw

    # Output signals
    sig = r.get('Output Signals', '')
    rec['output_signals'] = sig
    if pd.notna(sig):
        sl = str(sig).lower()
        if 'abn' in sl or 'zero' in sl or 'ab0' in sl: rec['num_output_channels'] = 3
        elif 'ab' in sl: rec['num_output_channels'] = 2
        elif 'a' in sl:  rec['num_output_channels'] = 1

    # Shaft
    shaft_raw  = r.get('Shaft Diameter', '')
    hollow_raw = r.get('Mechanical - Hollow shaft diameter', '')
    rec['shaft_type']       = _shaft_type(r.get('Family', ''), hollow_raw)
    rec['shaft_diameter_mm'] = _parse_shaft_mm(shaft_raw)
    rec['oc_shaft_type']    = shaft_raw

    # IP
    prot1 = r.get('Protection', '')
    prot2 = r.get('Environmental - Protection', '')
    rec['ip_rating'] = _parse_ip(prot1) or _parse_ip(prot2)

    # Temperature
    temp1 = r.get('Operating Temperature', '')
    temp2 = r.get('Environmental - Operating temperature range', '')
    t = _parse_temp(temp1)
    if np.isnan(t[0]): t = _parse_temp(temp2)
    rec['operating_temp_min_c'] = t[0]
    rec['operating_temp_max_c'] = t[1]

    # Voltage — encoded in circuit code; parse from Supply Voltage or circuit string
    sv_raw = r.get('Supply Voltage') or r.get('Electrical - Power Supply', '')
    pwr    = r.get('Electrical - Power Supply', '')
    vmin, vmax = _parse_v(sv_raw)
    if np.isnan(vmin): vmin, vmax = _parse_v(pwr)
    if np.isnan(vmin): vmin, vmax = _parse_v(circ_raw)
    rec['supply_voltage_min_v'] = vmin
    rec['supply_voltage_max_v'] = vmax

    # Speed
    rec['max_speed_rpm_peak'] = _parse_speed(r.get('Mechanical - Shaft rotational speed', ''))

    # Weight
    rec['weight_g'] = _parse_weight(r.get('Mechanical - Weight', ''))

    # Torque
    rec['startup_torque_ncm'] = _parse_torque(r.get('Mechanical - Starting torque (at 20°C)', ''))

    # Shaft loading
    shaft_load = str(r.get('Mechanical - Shaft loading (axial, radial)', '') or '')
    nums_load  = re.findall(r'\d+(?:\.\d+)?', shaft_load)
    if len(nums_load) >= 2:
        rec['shaft_load_radial_n'] = float(nums_load[0])
        rec['shaft_load_axial_n']  = float(nums_load[1])

    # Shock / vibration
    rec['shock_resistance']     = r.get('Environmental - Shock', '')
    rec['vibration_resistance'] = r.get('Environmental - Vibrations', '')

    # Max output frequency
    rec['max_output_freq_hz'] = _parse_max_freq(r.get('Electrical - Counting Frequency', ''))

    # Output current
    cur_raw = str(r.get('Electrical - Output Current (each channel)', '') or '')
    cur_nums = re.findall(r'\d+(?:\.\d+)?', cur_raw)
    if cur_nums: rec['output_current_ma'] = float(cur_nums[0])

    # Connection
    conn_pos = str(r.get('Connection Position', '') or '')
    conn_len = str(r.get('Cable Length', '') or '')
    if 'M' in conn_pos.upper():
        rec['connection_type'] = conn_pos.strip()
    elif 'cable' in conn_len.lower() or 'L0' in conn_len:
        rec['connection_type'] = 'Cable'
    elif 'C' in conn_len[:2]:
        rec['connection_type'] = 'Connector'
    rec['oc_connector'] = conn_len

    # Protection flags
    eprot = str(r.get('Electrical - Protection', '') or '')
    rec['reverse_polarity_protection'] = 'Yes' if 'polarity' in eprot.lower() else np.nan
    rec['short_circuit_protection']    = 'Yes' if 'short' in eprot.lower() else np.nan

    return rec


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description='Filter raw Lika CSV → Kubler-compatible unified sample')
    ap.add_argument('--input',     required=True, help='Path to raw Lika CSV')
    ap.add_argument('--output',    default='lika_filtered.csv')
    ap.add_argument('--target',    type=int, default=100_000)
    ap.add_argument('--chunksize', type=int, default=50_000)
    ap.add_argument('--sep',       default=',', help='CSV separator (default: comma)')
    args = ap.parse_args()

    print(f"Reading {args.input} in chunks of {args.chunksize:,} ...")
    print(f"Target: {args.target:,} output rows")

    kept_chunks = []
    total_read  = 0
    total_kept  = 0

    for chunk in pd.read_csv(args.input, chunksize=args.chunksize,
                              sep=args.sep, low_memory=False, dtype=str):
        total_read += len(chunk)

        # ── 1. Family filter: 58mm body ───────────────────────────────────────
        mask_fam = chunk['Family'].isin(FAMILIES_58MM)
        chunk = chunk[mask_fam]
        if len(chunk) == 0:
            continue

        # ── 2. Circuit filter: exclude NPN (N2) and PNP (P2) ─────────────────
        def _get_code(s):
            m = re.match(r'^([A-Z]\d)', str(s).strip()) if pd.notna(s) else None
            return m.group(1) if m else ''

        circuit_codes = chunk['Output Circuits / Power Supply'].apply(_get_code)
        mask_circ = ~circuit_codes.isin(EXCLUDE_CIRCUIT_CODES)
        chunk = chunk[mask_circ]
        if len(chunk) == 0:
            continue

        # ── 3. PPR filter: in Kubler set OR programmable ──────────────────────
        ppr_series = pd.to_numeric(chunk['Resolution (PPR)'], errors='coerce')
        elec_res   = chunk['Electrical - Resolution'].astype(str).str.lower()
        is_prog    = elec_res.str.contains('programmable|xxxxx', na=False)
        mask_ppr   = ppr_series.isin(KUBLER_PPR) | is_prog | ppr_series.isna()
        chunk = chunk[mask_ppr]
        if len(chunk) == 0:
            continue

        # ── 4. Map to unified schema ──────────────────────────────────────────
        mapped = pd.DataFrame(
            [map_row(r) for r in chunk.to_dict('records')],
            columns=COLS
        )

        # Non-null score for dedup preference
        mapped['_nn'] = mapped[COLS].notna().sum(axis=1)

        # ── 5. Dedup: one row per (family, ppr, circuit, shaft_dia, ip) ──────
        dedup_key = ['product_family', 'resolution_ppr', 'output_circuit_canonical',
                     'shaft_diameter_mm', 'ip_rating']
        mapped = (mapped.sort_values('_nn', ascending=False)
                        .drop_duplicates(subset=dedup_key, keep='first'))

        kept_chunks.append(mapped.drop(columns=['_nn']))
        total_kept += len(mapped)

        print(f"  Read {total_read:>9,} | Kept so far {total_kept:>7,} | "
              f"Chunk yield {len(mapped):>4,}")

        if total_kept >= args.target * 1.5:
            break

    if not kept_chunks:
        print("ERROR: No rows passed the filters. Check family names and CSV format.")
        sys.exit(1)

    result = pd.concat(kept_chunks, ignore_index=True)

    # Final diversity sample — stratify by (family, circuit, PPR)
    result['_nn'] = result[COLS].notna().sum(axis=1)
    result = result.sort_values('_nn', ascending=False)

    if len(result) > args.target:
        per_fam = max(1, args.target // result['product_family'].nunique())
        sampled = (result.groupby('product_family', group_keys=False)
                         .apply(lambda g: g.head(per_fam)))
        if len(sampled) < args.target:
            rest = result[~result.index.isin(sampled.index)]
            sampled = pd.concat([sampled, rest]).head(args.target)
        result = sampled.head(args.target)

    result = result.drop(columns=['_nn'], errors='ignore')[COLS].reset_index(drop=True)
    result.to_csv(args.output, index=False)

    print()
    print('=' * 55)
    print(f'OUTPUT: {args.output}')
    print(f'Rows:   {len(result):,}')
    print(f'Cols:   {len(result.columns)}')
    print()
    print('Family breakdown:')
    print(result['product_family'].value_counts().to_string())
    print()
    print('Circuit breakdown:')
    print(result['output_circuit_canonical'].value_counts(dropna=False).to_string())
    print()
    print('PPR breakdown (top 15):')
    print(result['resolution_ppr'].value_counts(dropna=False).head(15).to_string())
    print()
    prog = result['is_programmable'].astype(str).str.lower().isin(['true','1'])
    print(f'Programmable rows: {prog.sum():,}')
    print('=' * 55)


if __name__ == '__main__':
    main()
