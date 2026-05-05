# -*- coding: utf-8 -*-
"""
baumer_etl.py  —  Baumer encoder ETL  v1  (V17)
================================================
Reads:  baumer_raw.csv   (EIL580/EN580 precise optical, 222 rows)
        baumer_hd_raw.csv (HOG/HMG/POG heavy duty, 176 rows)
Writes: data/baumer_unified.csv  (47-col canonical schema)

Run:  python baumer_etl.py [--dry-run]
"""
import csv, json, re, os, sys

# Add project dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from schema import CANONICAL_COLUMNS

_HERE = os.path.dirname(os.path.abspath(__file__))
EIL_CSV = "/mnt/user-data/uploads/baumer_raw.csv"
HD_CSV  = "/mnt/user-data/uploads/baumer_hd_raw.csv"
OUT_CSV = os.path.join(_HERE, "data", "baumer_unified.csv")

# Output circuit mapping
_OC_MAP = {
    "TTL RS422":"TTL RS422","TTL":"TTL RS422","RS422":"TTL RS422",
    "HTL":"Push-Pull","HTL/push-pull":"Push-Pull","Push Pull":"Push-Pull",
    "HTL/TTL Universal":"TTL/HTL Universal","HTL/TTL (Vin = Vout)":"TTL/HTL Universal",
    "HTL/TTL/RS422":"TTL/HTL Universal","HTL\nTTL/RS422":"TTL/HTL Universal",
    "HTL-P (power linedriver)\nTTL":"TTL/HTL Universal",
    "HTL-P (power linedriver)\nTTL/RS422":"TTL/HTL Universal",
    "HTL-P (power linedriver)":"Push-Pull",
    "HTL (power linedriver)":"Push-Pull",
    "SinCos 1 Vpp":"Sin/Cos","SinCos":"Sin/Cos",
    "TTL/push-pull":"TTL RS422",
    "TTL/RS422":"TTL RS422",
    "TTL/RS422\nHTL/push-pull":"TTL/HTL Universal",
    "Linedriver/RS422\nPush-pull short-circuit proof":"TTL/HTL Universal",
}

def _oc(raw):
    if not raw: return None
    r = str(raw).strip()
    return _OC_MAP.get(r) or _OC_MAP.get(r.split("\n")[0].strip())

def _fix(s):
    if not s: return s
    return str(s).encode("latin-1").decode("utf-8", errors="replace") if "Ã" in str(s) else str(s)

def _parse_voltage(raw):
    if not raw: return None, None
    raw = str(raw).replace("…","...").replace("–","...").replace(" ","")
    m = re.search(r"([\d.]+)\.\.\.([\d.]+)", raw)
    if m: return float(m.group(1)), float(m.group(2))
    m2 = re.search(r"([\d.]+)\s*VDC", raw, re.I)
    if m2:
        v = float(m2.group(1))
        tm = re.search(r"±\s*([\d.]+)\s*%", raw)
        t = float(tm.group(1))/100 if tm else 0.1
        return round(v*(1-t),2), round(v*(1+t),2)
    return None, None

def _parse_ip(raw):
    if not raw: return None
    nums = re.findall(r"IP\s*(\d{2})", str(raw), re.I)
    return max(int(n) for n in nums) if nums else None

def _parse_speed(raw):
    if not raw: return None
    m = re.search(r"([\d,]+)\s*rpm", str(raw).replace(",",""), re.I)
    return float(m.group(1)) if m else None

def _parse_temp(raw):
    if not raw: return None, None
    raw = str(raw).replace("…","...").replace("–","...")
    m = re.search(r"([+-]?\d+\.?\d*)\s*\.\.\.?\s*([+-]?\d+\.?\d*)", raw)
    if m: return float(m.group(1)), float(m.group(2))
    return None, None

def _parse_shaft(raw):
    raw = _fix(str(raw or "")).lower().split("\n")[0]  # first line only
    # Use \d+\.?\d* to stop at range separator (avoids "8...15" → float error)
    m = re.search(r"ø\s*(\d+\.?\d*)", raw) or re.search(r"(\d+\.?\d*)\s*mm", raw)
    dia = float(m.group(1)) if m else None
    if "through" in raw: t = "Through Hollow"
    elif "hollow" in raw or "bore" in raw: t = "Hollow"
    else: t = "Solid"
    return t, dia

def _parse_connection(raw):
    if not raw: return None, None
    lo = str(raw).lower()
    for ct in ["M23","M12","M8","M17"]:
        if ct.lower() in lo: conn = ct; break
    else:
        conn = "Terminal" if "terminal" in lo else ("Cable" if "cable" in lo else None)
    m = re.search(r"(\d+)-?pin", lo)
    return conn, (int(m.group(1)) if m else None)

def _parse_weight(raw):
    if not raw: return None
    mkg = re.search(r"([\d.]+)\s*kg", str(raw), re.I)
    if mkg: return round(float(mkg.group(1))*1000)
    mg = re.search(r"([\d.]+)\s*g", str(raw), re.I)
    return float(mg.group(1)) if mg else None

def _parse_torque(raw):
    if not raw: return None
    mnm = re.search(r"([\d.]+)\s*Nm", str(raw), re.I)
    if mnm: return round(float(mnm.group(1))*100, 2)
    mncm = re.search(r"([\d.]+)\s*Ncm", str(raw), re.I)
    return float(mncm.group(1)) if mncm else None

def _parse_load(raw):
    if not raw: return None
    m = re.search(r"([\d.]+)\s*N", str(raw), re.I)
    return float(m.group(1)) if m else None

def _parse_inertia(raw):
    if not raw: return None
    raw2 = str(raw).replace(" ","")
    m = re.search(r"([\d.]+)\s*gcm", raw2, re.I)
    if m: return float(m.group(1))
    m2 = re.search(r"([\d.e+-]+)\s*kgm", raw2, re.I)
    if m2: return float(m2.group(1))*1e7
    return None

def _parse_ppr(raw):
    if not raw: return None
    m = re.search(r"(\d+)", str(raw).replace(",",""))
    return float(m.group(1)) if m else None

def _parse_family(pn):
    pn = str(pn).strip()
    m = re.match(r"^([A-Za-z]+[\d]*)", pn.replace(" ",""))
    return m.group(1) if m else pn.split("-")[0].split(" ")[0]

def _parse_corrosion(raw):
    if not raw: return None
    raw = str(raw).replace("\n"," ")
    if "C5-M" in raw.upper() or "C5M" in raw.upper(): return "C5-M"
    m = re.search(r'\bC(X|[1-5])\b', raw, re.I)
    return ("C"+m.group(1).upper()) if m else None

def _parse_si(raw):
    if not raw: return None
    m = re.search(r"([\d.]+)\s*kV", str(raw), re.I)
    return f"{m.group(1)} kV" if m else str(raw).strip()

def _is_atex(raw):
    if not raw: return None
    lo = str(raw).lower()
    return "True" if any(kw in lo for kw in ["ex ","atex","ii 2","ii 3","ex ec","ex db","ex tc"]) else None

def _blank():
    return {c: None for c in CANONICAL_COLUMNS}

def _eil_to_canon(r):
    row = _blank()
    row["manufacturer"] = "Baumer"; row["encoder_type"] = "Incremental"
    row["source_pdf"] = "baumer_scraper_v1"
    pn = r.get("listing_part_number","").strip()
    row["part_number"] = pn; row["product_family"] = _parse_family(pn)
    row["product_url"] = r.get("listing_product_url","").strip() or None
    row["sensing_method"] = r.get("page_sensing_method","").strip() or "Optical"
    row["resolution_ppr"] = _parse_ppr(r.get("page_pulses_per_revolution","") or r.get("listing_ppr",""))
    row["is_programmable"] = "False"
    row["output_circuit_canonical"] = _oc(r.get("page_output_stages","") or r.get("listing_output_stages",""))
    row["output_signals"] = r.get("page_output_signals","").strip() or None
    vmin, vmax = _parse_voltage(r.get("page_voltage_supply",""))
    row["supply_voltage_min_v"] = vmin; row["supply_voltage_max_v"] = vmax
    m_hd = re.search(r"(\d+\.?\d*)", str(r.get("listing_housing_dia_mm","") or r.get("page_size_flange","")))
    row["housing_diameter_mm"] = float(m_hd.group(1)) if m_hd else None
    st, sd = _parse_shaft(_fix(r.get("page_shaft_type","") or r.get("listing_shaft_type","")))
    row["shaft_type"] = st; row["shaft_diameter_mm"] = sd
    row["ip_rating"] = _parse_ip(r.get("page_protection_en60529",""))
    tmin, tmax = _parse_temp(r.get("page_operating_temperature",""))
    row["operating_temp_min_c"] = tmin; row["operating_temp_max_c"] = tmax
    row["max_speed_rpm_peak"] = _parse_speed(r.get("page_operating_speed",""))
    conn, pins = _parse_connection(r.get("page_connection",""))
    row["connection_type"] = conn; row["connector_pins"] = pins
    row["weight_g"] = _parse_weight(r.get("page_weight_approx",""))
    row["startup_torque_ncm"] = _parse_torque(r.get("page_starting_torque",""))
    row["shaft_load_radial_n"] = _parse_load(r.get("page_permissible_shaft_load_radial",""))
    row["shaft_load_axial_n"]  = _parse_load(r.get("page_permissible_shaft_load_axial",""))
    row["moment_of_inertia"] = _parse_inertia(r.get("page_moment_of_inertia",""))
    row["vibration_resistance"] = r.get("page_resistance_to_vibration","").strip() or None
    row["shock_resistance"] = r.get("page_resistance_to_shocks","").strip() or None
    row["short_circuit_protection"] = r.get("page_short_circuit_proof","").strip() or None
    row["reverse_polarity_protection"] = r.get("page_reverse_polarity_prot","").strip() or None
    row["flange_type"] = "Synchro"
    row["is_atex_certified"] = None; row["shaft_insulation_v"] = None; row["corrosion_protection_class"] = None
    return row

def _hd_to_canon(r):
    row = _blank()
    row["manufacturer"] = "Baumer"; row["encoder_type"] = "Incremental"
    row["source_pdf"] = "baumer_hd_scraper_v1"
    pn = r.get("listing_part_number","").strip()
    row["part_number"] = pn; row["product_family"] = _parse_family(pn)
    row["product_url"] = r.get("listing_product_url","").strip() or None
    row["sensing_method"] = r.get("page_sensing_method","").strip() or None
    ppr_min = r.get("listing_ppr_min",""); ppr_max = r.get("listing_ppr_max","")
    if ppr_min and ppr_max:
        row["is_programmable"] = "True"
        row["ppr_range_min"] = _parse_ppr(ppr_min)
        row["ppr_range_max"] = _parse_ppr(ppr_max)
    else:
        row["is_programmable"] = "False"
        row["resolution_ppr"] = _parse_ppr(r.get("page_pulses_per_revolution","") or r.get("listing_ppr",""))
    row["output_circuit_canonical"] = _oc(r.get("page_output_stages","") or r.get("listing_output_stages",""))
    row["output_signals"] = r.get("page_output_signals","").strip().split("\n")[0] or None
    vmin, vmax = _parse_voltage(r.get("page_voltage_supply",""))
    row["supply_voltage_min_v"] = vmin; row["supply_voltage_max_v"] = vmax
    m_hd = re.search(r"(\d+\.?\d*)", str(r.get("listing_housing_dia_mm","") or r.get("page_size_flange","")))
    row["housing_diameter_mm"] = float(m_hd.group(1)) if m_hd else None
    shaft_first = _fix(r.get("page_shaft_type","") or r.get("listing_shaft_type","")).split("\n")[0]
    st, sd = _parse_shaft(shaft_first)
    row["shaft_type"] = st; row["shaft_diameter_mm"] = sd
    row["ip_rating"] = _parse_ip(r.get("page_protection_en60529",""))
    tmin, tmax = _parse_temp(r.get("page_operating_temperature",""))
    row["operating_temp_min_c"] = tmin; row["operating_temp_max_c"] = tmax
    row["max_speed_rpm_peak"] = _parse_speed(r.get("page_operating_speed",""))
    conn, pins = _parse_connection((r.get("page_connection","") or "").split("\n")[0])
    row["connection_type"] = conn; row["connector_pins"] = pins
    row["weight_g"] = _parse_weight(r.get("page_weight_approx",""))
    row["startup_torque_ncm"] = _parse_torque(r.get("page_operating_torque_typ",""))
    row["shaft_load_radial_n"] = _parse_load(r.get("page_admitted_shaft_load_radial","") or r.get("page_admitted_shaft_load",""))
    row["shaft_load_axial_n"]  = _parse_load(r.get("page_admitted_shaft_load_axial","") or r.get("page_admitted_shaft_load",""))
    row["moment_of_inertia"] = _parse_inertia(r.get("page_rotor_moment_of_inertia",""))
    row["vibration_resistance"] = r.get("page_resistance_to_vibration","").strip() or None
    row["shock_resistance"] = r.get("page_resistance_to_shocks","").strip() or None
    row["short_circuit_protection"] = r.get("page_short_circuit_proof","").strip() or None
    flange_lo = (r.get("page_flange","") or "").lower()
    row["flange_type"] = "Stator Coupler" if ("torque arm" in flange_lo or "stator" in flange_lo) else None
    try:
        raw_all = json.loads(r.get("page_raw_all_fields","{}") or "{}")
    except Exception:
        raw_all = {}
    corr_raw = r.get("page_corrosion_protection","") or raw_all.get("Corrosion protection","")
    row["corrosion_protection_class"] = _parse_corrosion(corr_raw)
    row["shaft_insulation_v"] = _parse_si(raw_all.get("Shaft insulation",""))
    row["is_atex_certified"] = _is_atex(raw_all.get("Explosion protection",""))
    return row

def run(dry_run=False):
    rows = []
    eil_data = list(csv.DictReader(open(EIL_CSV, encoding="utf-8")))
    for i, r in enumerate(eil_data):
        try: rows.append(_eil_to_canon(r))
        except Exception as e: print(f"EIL row {i} error: {e}")

    hd_data = list(csv.DictReader(open(HD_CSV, encoding="utf-8")))
    for i, r in enumerate(hd_data):
        try: rows.append(_hd_to_canon(r))
        except Exception as e: print(f"HD row {i} error: {e}")

    print(f"=== Baumer ETL ===")
    print(f"  EIL: {len(eil_data)} in → {sum(1 for r in rows[:len(eil_data)] if r['part_number'])} out")
    print(f"  HD:  {len(hd_data)} in → {sum(1 for r in rows[len(eil_data):] if r['part_number'])} out")
    print(f"  Total: {len(rows)} rows")
    print(f"  ATEX: {sum(1 for r in rows if r.get('is_atex_certified')=='True')}")
    print(f"  Corrosion rated: {sum(1 for r in rows if r.get('corrosion_protection_class'))}")
    print(f"  Shaft insulation: {sum(1 for r in rows if r.get('shaft_insulation_v'))}")
    print(f"  Programmable: {sum(1 for r in rows if r.get('is_programmable')=='True')}")
    print(f"  No OC: {sum(1 for r in rows if not r.get('output_circuit_canonical'))}")
    
    if not dry_run:
        os.makedirs(os.path.join(_HERE, "data"), exist_ok=True)
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CANONICAL_COLUMNS)
            w.writeheader()
            for row in rows:
                w.writerow({c: row.get(c) for c in CANONICAL_COLUMNS})
        print(f"  Written: {OUT_CSV}")
    return rows

if __name__ == "__main__":
    run(dry_run="--dry-run" in sys.argv)
