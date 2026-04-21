#!/usr/bin/env python3
"""
filter_wachendorff.py
=====================
Reads the raw Wachendorff CSV (any size — processed in chunks) and outputs a
Kubler-compatible filtered sample in unified schema format.

Key facts about Wachendorff data:
  - ALL products are programmable (PPR range 1-25000 or 1-16384) — no fixed PPR
  - Flange sizes: 36mm (majority), 50mm, 30mm, 58mm (only ~2%)
  - Output: HTL (push-pull) + TTL — both compatible with Kubler
  - No NPN or PNP open-collector products

Filtering logic:
  1. CIRCUIT FILTER — Exclude any pure NPN/PNP rows (none expected but safety check)
  2. FLANGE FILTER  — Prefer 58mm (direct Kubler match). Also keep 50mm and 36mm
                      with clear notes on housing penalty:
                        58mm → housing score 1.0 (exact Kubler K58I match)
                        50mm → housing score 0.5 (8mm delta)
                        36mm → housing score 0.0 (>5mm delta, T2 hard penalty)
                      USER: if you want only 58mm matches, set --flange_filter 58
  3. DEDUP          — One row per (Family, FlangeMM, ShaftDia, Circuit, Signals)
                      removes cable-length variants
  4. NON-NULL SCORE — Keep richest row per dedup key

Usage:
    python filter_wachendorff.py \
        --input  /path/to/wachendorff_full.csv \
        --output wachendorff_filtered.csv \
        --target 50000 \
        --flange_filter all      # 'all' | '58' | '58,50'

Output: unified schema CSV (43 cols, same as competitor_unified.csv)
"""

import argparse, re, sys
import pandas as pd
import numpy as np

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
    nums = re.findall(r'\d+(?:\.\d+)?', str(s).replace(',', '.'))
    f = [float(n) for n in nums if float(n) < 1000]
    if not f: return np.nan, np.nan
    if len(f) == 1: return f[0], f[0]
    return f[0], f[-1]

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
    if pd.isna(s): return np.nan
    s = str(s).replace(',', '.')
    # '10 = Konus 1/10 9.25 mm' → prefer the main shaft code
    # '06' → 6mm, '08' → 8mm, '4Z = 9.525' → 9.525mm
    m = re.search(r'(\d+(?:\.\d+)?)\s*\[?mm\]?', s, re.I)
    if m: return float(m.group(1))
    m = re.match(r'^(\d+(?:\.\d+)?)', s.strip())
    if m:
        v = float(m.group(1))
        return v  # e.g. '06' = 6mm
    return np.nan

def _parse_weight(s):
    if pd.isna(s): return np.nan
    s2 = str(s).replace(',', '.')
    nums = re.findall(r'\d+(?:\.\d+)?', s2)
    if not nums: return np.nan
    v = float(nums[0])
    if 'kg' in s2.lower(): return v * 1000
    return v  # assume grams

def _parse_torque(s):
    if pd.isna(s): return np.nan
    s2 = str(s).replace(',', '.')
    m = re.search(r'(\d+(?:\.\d+)?)\s*[Nn][Cc][Mm]', s2)
    if m: return float(m.group(1))
    m = re.search(r'(\d+(?:\.\d+)?)\s*[Nn][Mm]', s2)
    if m: return float(m.group(1)) * 100  # Nm → Ncm
    return np.nan

def _parse_speed(s):
    if pd.isna(s): return np.nan
    nums = re.findall(r'\d+', str(s))
    vals = [int(n) for n in nums if int(n) > 100]
    return float(max(vals)) if vals else np.nan

def _parse_freq(s):
    if pd.isna(s): return np.nan
    s = str(s)
    nums = re.findall(r'\d+(?:\.\d+)?', s)
    if not nums: return np.nan
    v = float(nums[0])
    if 'mhz' in s.lower(): return v * 1_000_000
    if 'khz' in s.lower(): return v * 1_000
    return v

def _canonical_circuit(s):
    """Map Wachendorff output circuit string → unified canonical label.
    Handles both the Electrical-Output Circuit column and Electronic(Output Type) column,
    including ADV/BAS variant codes that prefix the circuit description."""
    if pd.isna(s): return np.nan
    sl = str(s).lower()
    # Wachendorff uses combined strings like:
    #   'HTL HTL, inv. TTL TTL, RS422 compatible, inv'   → TTL/HTL Universal
    #   'ADV = 4.75 - 32 VDC HTL/ TTL inverted, 600 kHz' → TTL/HTL Universal
    #   'N30 = 5-30 VDC HTL (TTL at 5 VDC)'              → TTL/HTL Universal
    has_ttl = 'ttl' in sl or 'rs422' in sl or 'rs-422' in sl
    has_htl = 'htl' in sl
    if has_ttl and has_htl:
        return 'TTL/HTL Universal'
    if has_htl:
        return 'HTL'
    if has_ttl:
        return 'TTL RS422'
    if 'npn' in sl: return 'NPN Open Collector'
    if 'pnp' in sl: return 'PNP Open Collector'
    return str(s).strip()

def _ppr_range(ppr_col, resolution_col):
    """Return (is_prog, ppr_min, ppr_max) for Wachendorff.
    All Wachendorff are programmable — PPR col contains '1 - 25000' or 'X (Programmable)'."""
    s = str(ppr_col or '').strip()
    r = str(resolution_col or '').strip()
    nums = re.findall(r'\d+', s + ' ' + r)
    ints = sorted([int(n) for n in nums if 1 <= int(n) <= 100000])
    if len(ints) >= 2:
        return True, float(ints[0]), float(ints[-1])
    if ints:
        return True, 1.0, float(ints[-1])
    # 'X (Programmable)' with no range → use max known
    if 'programmable' in s.lower() or s.upper() == 'X':
        return True, 1.0, 25000.0
    return True, 1.0, 25000.0  # all Wachendorff are programmable

def _shaft_type_wacho(type_col):
    """Wachendorff 'Type' column describes flange type (S/V/B/A/J/C/T), not shaft type.
    Wachendorff encoders are mostly solid shaft. Hollow shaft families have
    Type E/G/H/I where 'Ø is programmable from 06 to 72 mm' in shaft col."""
    if pd.isna(type_col): return 'Solid'
    t = str(type_col).strip().upper()
    if t in ('E', 'G', 'H', 'I'):
        return 'Hollow'
    return 'Solid'

def _flange_type_wacho(type_col):
    mapping = {
        'S': 'Clamping', 'V': 'Clamping', 'B': 'Clamping',
        'A': 'Synchro',  'J': 'Synchro',
        'C': 'Face Mount', 'T': 'Face Mount',
    }
    if pd.isna(type_col): return np.nan
    t = str(type_col).strip().upper()[:1]
    return mapping.get(t, str(type_col).strip())

# ── Row mapper ────────────────────────────────────────────────────────────────
def map_row(r):
    rec = {c: np.nan for c in COLS}
    rec['manufacturer']   = 'Wachendorff'
    rec['part_number']    = r.get('Part Number')
    rec['product_family'] = r.get('Family')
    rec['encoder_type']   = 'Incremental'

    # Housing diameter
    flange_mm = r.get('Flange Size (mm)')
    rec['housing_diameter_mm'] = float(flange_mm) if pd.notna(flange_mm) else np.nan
    rec['oc_flange'] = r.get('Mechanical - Flange', r.get('Family'))

    # PPR — all Wachendorff programmable
    is_prog, ppr_min, ppr_max = _ppr_range(r.get('PPR'), r.get('Resolution'))
    rec['is_programmable']  = is_prog
    rec['ppr_range_min']    = ppr_min
    rec['ppr_range_max']    = ppr_max
    rec['resolution_ppr']   = np.nan   # no fixed PPR
    rec['oc_ppr']           = r.get('PPR')

    # Circuit — try Electrical - Output Circuit first, then Electronic (Output Type)
    elec_circ = r.get('Electrical - Output Circuit', '')
    elec_type  = r.get('Electronic (Output Type)', '')
    # Use whichever has more meaningful content
    circ_raw = elec_circ if (pd.notna(elec_circ) and str(elec_circ).strip()) else elec_type
    rec['output_circuit_canonical'] = _canonical_circuit(circ_raw)
    rec['oc_interface'] = circ_raw

    # Signals
    sig_raw = r.get('Signals', r.get('Electrical - Channels', ''))
    rec['output_signals'] = sig_raw
    if pd.notna(sig_raw):
        sl = str(sig_raw).lower()
        if 'abn' in sl or 'zero' in sl: rec['num_output_channels'] = 3
        elif 'ab' in sl: rec['num_output_channels'] = 2
        elif 'a' in sl:  rec['num_output_channels'] = 1

    # Shaft
    type_col = r.get('Type', '')
    shaft_col = r.get('Ø in mm', r.get('Mechanical - Shaft', ''))
    rec['shaft_type']       = _shaft_type_wacho(type_col)
    rec['shaft_diameter_mm'] = _parse_shaft_mm(shaft_col)
    rec['flange_type']      = _flange_type_wacho(type_col)
    rec['oc_shaft_type']    = shaft_col

    # IP
    rec['ip_rating'] = _parse_ip(r.get('General - Protection rating (EN 60529)', ''))

    # Temperature
    t = _parse_temp(r.get('General - Operating temperature', ''))
    rec['operating_temp_min_c'] = t[0]
    rec['operating_temp_max_c'] = t[1]

    # Voltage
    pwr_raw = r.get('Electrical - Power Supply', r.get('Electronic (Output Type)', ''))
    vmin, vmax = _parse_v(pwr_raw)
    rec['supply_voltage_min_v'] = vmin
    rec['supply_voltage_max_v'] = vmax

    # Speed
    rec['max_speed_rpm_peak'] = _parse_speed(r.get('Mechanical - Max. Operating Speed', ''))

    # Weight
    rec['weight_g'] = _parse_weight(r.get('General - Weight', ''))

    # Torque
    rec['startup_torque_ncm'] = _parse_torque(r.get('Mechanical - Starting Torque', ''))

    # Shaft loads
    rad_raw = r.get('Mechanical - Max Permissible Shaft Loading Radial', '')
    axl_raw = r.get('Mechanical - Max Permissible Shaft Loading Axial', '')
    rnums = re.findall(r'\d+(?:\.\d+)?', str(rad_raw))
    anums = re.findall(r'\d+(?:\.\d+)?', str(axl_raw))
    if rnums: rec['shaft_load_radial_n'] = float(rnums[0])
    if anums: rec['shaft_load_axial_n']  = float(anums[0])

    # Shock / vibration
    rec['shock_resistance']     = r.get('Environmental - Shock (DIN EN 60068-2-27)', '')
    rec['vibration_resistance'] = r.get('Environmental - Vibration (DIN EN 60068-2-6)', '')

    # Max freq
    rec['max_output_freq_hz'] = _parse_freq(r.get('Electrical - Pulse Frequency', ''))

    # Connection
    conn_raw = r.get('Electrical Connection', r.get('General - Connections', ''))
    conn_s = str(conn_raw).upper()
    if 'M12' in conn_s: rec['connection_type'] = 'M12'
    elif 'M23' in conn_s: rec['connection_type'] = 'M23'
    elif 'CABLE' in conn_s or 'K' in conn_s[:2]: rec['connection_type'] = 'Cable'
    elif 'CONN' in conn_s or 'PLUG' in conn_s: rec['connection_type'] = 'Connector'
    rec['oc_connector'] = conn_raw

    # Circuit protection
    eprot = str(r.get('Electrical - Circuit Protection', '') or '')
    rec['reverse_polarity_protection'] = 'Yes' if 'polarity' in eprot.lower() or 'reverse' in eprot.lower() else np.nan
    rec['short_circuit_protection']    = 'Yes' if 'short' in eprot.lower() else np.nan

    return rec


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description='Filter raw Wachendorff CSV → Kubler-compatible unified sample'
    )
    ap.add_argument('--input',         required=True)
    ap.add_argument('--output',        default='wachendorff_filtered.csv')
    ap.add_argument('--target',        type=int, default=50_000)
    ap.add_argument('--chunksize',     type=int, default=50_000)
    ap.add_argument('--sep',           default=',')
    ap.add_argument('--flange_filter', default='all',
                    help="Comma-separated flange mm values to keep, e.g. '58' or '58,50' or 'all'")
    args = ap.parse_args()

    # Parse flange filter
    if args.flange_filter.lower() == 'all':
        allowed_flanges = None   # keep all
    else:
        allowed_flanges = set(int(x.strip()) for x in args.flange_filter.split(','))

    print(f"Reading {args.input} in chunks of {args.chunksize:,} ...")
    print(f"Flange filter: {args.flange_filter}")
    print(f"Target: {args.target:,} rows")
    print()

    # Wachendorff housing note
    print("NOTE — housing diameter match vs Kubler:")
    print("  58mm flange → exact match (K58I / 5000 / 5020) — score 1.0")
    print("  50mm flange → 8mm delta — housing T2 score ~0.5")
    print("  36mm flange → 22mm delta — housing T2 score 0.0 (>5mm penalty)")
    print("  30mm flange → 28mm delta — housing T2 score 0.0")
    print()

    kept_chunks = []
    total_read = total_kept = 0

    for chunk in pd.read_csv(args.input, chunksize=args.chunksize,
                              sep=args.sep, low_memory=False, dtype=str):
        total_read += len(chunk)

        # ── 1. Flange filter ──────────────────────────────────────────────────
        if allowed_flanges is not None:
            chunk['_flange_num'] = pd.to_numeric(chunk['Flange Size (mm)'], errors='coerce')
            chunk = chunk[chunk['_flange_num'].isin(allowed_flanges)]
            chunk = chunk.drop(columns=['_flange_num'])

        if len(chunk) == 0:
            continue

        # ── 2. Circuit filter: no pure NPN/PNP ───────────────────────────────
        circ_col = chunk.get('Electrical - Output Circuit',
                             chunk.get('Electronic (Output Type)', pd.Series(dtype=str)))
        is_bad = circ_col.str.contains(r'(?i)npn|pnp', na=False)
        # Only exclude if exclusively NPN/PNP (Wachendorff doesn't actually have these)
        chunk = chunk[~is_bad]
        if len(chunk) == 0:
            continue

        # ── 3. Map to unified schema ──────────────────────────────────────────
        mapped = pd.DataFrame(
            [map_row(r) for r in chunk.to_dict('records')],
            columns=COLS
        )
        mapped['_nn'] = mapped[COLS].notna().sum(axis=1)

        # ── 4. Dedup: one row per (family, flange, shaft_dia, circuit, signals) ─
        dedup_key = ['product_family', 'housing_diameter_mm',
                     'shaft_diameter_mm', 'output_circuit_canonical', 'output_signals']
        mapped = (mapped.sort_values('_nn', ascending=False)
                        .drop_duplicates(subset=dedup_key, keep='first'))

        kept_chunks.append(mapped.drop(columns=['_nn']))
        total_kept += len(mapped)

        print(f"  Read {total_read:>9,} | Kept so far {total_kept:>7,} | "
              f"Chunk yield {len(mapped):>4,}")

        if total_kept >= args.target * 1.5:
            break

    if not kept_chunks:
        print("ERROR: No rows passed the filters.")
        sys.exit(1)

    result = pd.concat(kept_chunks, ignore_index=True)
    result['_nn'] = result[COLS].notna().sum(axis=1)
    result = result.sort_values('_nn', ascending=False)

    if len(result) > args.target:
        per_fam = max(1, args.target // result['product_family'].nunique())
        sampled = (result.groupby(['product_family', 'housing_diameter_mm'],
                                  group_keys=False)
                         .apply(lambda g: g.head(per_fam)))
        if len(sampled) < args.target:
            rest = result[~result.index.isin(sampled.index)]
            sampled = pd.concat([sampled, rest]).head(args.target)
        result = sampled.head(args.target)

    result = result.drop(columns=['_nn'], errors='ignore')[COLS].reset_index(drop=True)
    result.to_csv(args.output, index=False)

    print()
    print('=' * 60)
    print(f'OUTPUT: {args.output}')
    print(f'Rows:   {len(result):,}')
    print()
    print('Flange (housing) breakdown:')
    print(result['housing_diameter_mm'].value_counts(dropna=False).to_string())
    print()
    print('Circuit breakdown:')
    print(result['output_circuit_canonical'].value_counts(dropna=False).to_string())
    print()
    print('Shaft type breakdown:')
    print(result['shaft_type'].value_counts(dropna=False).to_string())
    print()
    print('PPR range (all programmable):')
    print(f'  ppr_range_min: {result["ppr_range_min"].value_counts().head(5).to_string()}')
    print(f'  ppr_range_max: {result["ppr_range_max"].value_counts().head(5).to_string()}')
    print('=' * 60)


if __name__ == '__main__':
    main()
