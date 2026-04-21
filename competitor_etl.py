# -*- coding: utf-8 -*-
"""
competitor_etl.py - Build unified encoder CSV for encoder_crossref_v9.

Data sources:
  Kubler     : 2 families (K58I_hollow, K80I) from kubler_final.csv
  EPC        : 2 families (858S, 802S)        from epc_product_codes.csv
  Lika       : All 4 CSV files concatenated   from Lika/*.csv
  Wachendorff: Both CSVs concatenated         from Wachendorff/*.csv
  Nidec      : nidec_av4_av5.csv              (capped at NIDEC_CAP rows)
  Sick       : sick_incremental_etl.csv
  Baumer     : baumer_incremental_v3.csv

Output: data/competitor_unified.csv  (46-column unified schema)
"""
import os
import re
import sys
import argparse
import pandas as pd

from schema import UNIFIED_SCHEMA, OUTPUT_CIRCUIT_CANONICAL, housing_from_family, normalise_mfr
from normalizer import (
    parse_voltage_range, parse_temp_range, parse_ip_rating,
    parse_speed_rpm, parse_freq_hz, parse_weight_g, parse_torque_ncm,
    parse_current_ma, parse_power_mw, first_float, to_bool, safe_float,
)

# ?? Paths ??????????????????????????????????????????????????????????????????????
_HERE = os.path.dirname(os.path.abspath(__file__))
_INC  = r"C:\Users\sadhy\ai_cross_reference\incremental"

KUBLER_CSV   = os.path.join(_INC, r"Kubler\kubler_etl_full\kubler_final.csv\kubler_final.csv")
EPC_CSV      = os.path.join(_INC, r"Encoder\epc_product_codes.csv")
LIKA_DIR     = os.path.join(_INC, "Lika")
WACH_DIR     = os.path.join(_INC, "Wachendorff")
NIDEC_CSV    = os.path.join(_INC, r"Nidec\nidec_av4_av5.csv")
SICK_CSV     = os.path.join(_INC, r"Sick\sick_incremental_etl.csv")
BAUMER_CSV   = os.path.join(_INC, r"Baumer\baumer_incremental_v3.csv")
OUT_PATH     = os.path.join(_HERE, "data", "competitor_unified.csv")

KUBLER_FAMILIES = ["K58I_hollow", "K80I"]
EPC_FAMILIES    = ["858S", "802S"]
NIDEC_CAP       = 50_000

UNIFIED_COLS = UNIFIED_SCHEMA  # 46 cols


# ?? Vectorised helpers ?????????????????????????????????????????????????????????

def _vsafe(series: pd.Series) -> pd.Series:
    return series.where(series.notna() & (series.astype(str).str.strip() != ""), other=None)

def _v_voltage(series: pd.Series):
    parsed = series.map(lambda x: parse_voltage_range(x))
    lo = parsed.map(lambda t: t[0] if isinstance(t, tuple) else None)
    hi = parsed.map(lambda t: t[1] if isinstance(t, tuple) else None)
    return lo, hi

def _v_temp(series: pd.Series):
    parsed = series.map(lambda x: parse_temp_range(x))
    lo = parsed.map(lambda t: t[0] if isinstance(t, tuple) else None)
    hi = parsed.map(lambda t: t[1] if isinstance(t, tuple) else None)
    return lo, hi

def _v_ip(series: pd.Series) -> pd.Series:
    return series.map(parse_ip_rating)

def _v_speed(series: pd.Series) -> pd.Series:
    return series.map(parse_speed_rpm)

def _v_freq(series: pd.Series) -> pd.Series:
    return series.map(parse_freq_hz)

def _v_weight_g(series: pd.Series, unit: str = "g") -> pd.Series:
    return series.map(lambda x: parse_weight_g(x, unit_hint=unit))

def _v_float(series: pd.Series) -> pd.Series:
    return series.map(first_float)

def _v_circuit(series: pd.Series) -> pd.Series:
    return series.map(lambda x: OUTPUT_CIRCUIT_CANONICAL.get(
        str(x).strip(), str(x).strip()) if pd.notna(x) and str(x).strip() not in ("", "nan") else None)

def _blank() -> pd.DataFrame:
    """Return an empty unified DataFrame."""
    return pd.DataFrame(columns=UNIFIED_COLS)

def _finalise(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all unified columns exist and in correct order."""
    for col in UNIFIED_COLS:
        if col not in df.columns:
            df[col] = None
    return df[UNIFIED_COLS].copy()


# ?? Kubler ETL ?????????????????????????????????????????????????????????????????

def _etl_kubler(path: str, families: list[str]) -> pd.DataFrame:
    print(f"  [Kubler] Loading families {families} from {path}")
    df = pd.read_csv(path, low_memory=False)
    df = df[df["family"].isin(families)].copy()
    print(f"  [Kubler] {len(df):,} rows after family filter")

    out = pd.DataFrame()
    out["manufacturer"]     = df.get("manufacturer", pd.Series()).map(normalise_mfr).fillna("Kubler")
    out["part_number"]      = df.get("product_code", df.get("part_number", pd.Series())).astype(str)
    out["product_family"]   = df.get("family", df.get("product_family", pd.Series()))
    out["encoder_type"]     = df.get("encoder_type", pd.Series())
    out["sensing_method"]   = None
    out["source_pdf"]       = df.get("source_pdf", pd.Series())
    out["order_pattern"]    = df.get("order_pattern", pd.Series())

    out["resolution_ppr"]   = df.get("resolution_ppr", pd.Series()).map(safe_float)
    out["ppr_range_min"]    = df.get("ppr_range_min", pd.Series()).map(safe_float)
    out["ppr_range_max"]    = df.get("ppr_range_max", pd.Series()).map(safe_float)
    out["is_programmable"]  = (
        df.get("ppr_range_min", pd.Series()).notna() & df.get("ppr_range_max", pd.Series()).notna()
    )

    out["output_circuit_canonical"] = _v_circuit(df.get("interface_canonical", df.get("output_circuits", pd.Series())))
    out["output_signals"]   = df.get("output_signals", pd.Series())
    out["num_output_channels"] = None
    out["max_output_freq_hz"]  = df.get("max_output_frequency_hz", pd.Series()).map(safe_float)

    vlo, vhi = _v_voltage(df.get("supply_voltage", pd.Series()))
    out["supply_voltage_min_v"] = vlo
    out["supply_voltage_max_v"] = vhi
    out["output_current_ma"]    = df.get("permissible_load_per_channel", pd.Series()).map(parse_current_ma)
    out["power_consumption_typ_mw"] = df.get("power_consumption", pd.Series()).map(parse_power_mw)
    out["reverse_polarity_protection"] = df.get("reverse_polarity_protection", pd.Series()).map(to_bool)
    out["short_circuit_protection"]    = df.get("short_circuit_protection", pd.Series()).map(to_bool)

    out["housing_diameter_mm"] = df.get("family", pd.Series()).map(housing_from_family)
    out["shaft_diameter_mm"]   = df.get("shaft_diameter_mm", pd.Series()).map(safe_float)
    out["shaft_type"]          = None  # derive from family name
    fam = df.get("family", pd.Series()).astype(str)
    out["shaft_type"] = fam.map(
        lambda f: "Hollow" if "hollow" in f.lower() else ("Solid" if "shaft" in f.lower() else None)
    )
    out["flange_type"]     = None
    out["connection_type"] = df.get("connection_type", pd.Series())
    out["connector_pins"]  = df.get("connection_type", pd.Series()).map(parse_current_ma)

    out["ip_rating"]             = _v_ip(df.get("protection_rating", pd.Series()))
    tlo, thi = _v_temp(df.get("operating_temp_range", pd.Series()))
    out["operating_temp_min_c"]  = tlo
    out["operating_temp_max_c"]  = thi
    out["max_speed_rpm_peak"]    = df.get("max_speed_rpm", pd.Series()).map(parse_speed_rpm)
    out["shock_resistance"]      = df.get("shock_resistance", pd.Series())
    out["vibration_resistance"]  = df.get("vibration_resistance", pd.Series())

    out["weight_g"]          = df.get("weight_kg", pd.Series()).map(lambda x: parse_weight_g(x, "kg"))
    out["startup_torque_ncm"] = df.get("startup_torque", pd.Series()).map(parse_torque_ncm)
    out["shaft_load_radial_n"] = df.get("shaft_load_radial_n", pd.Series()).map(safe_float)
    out["shaft_load_axial_n"]  = df.get("shaft_load_axial_n", pd.Series()).map(safe_float)
    out["moment_of_inertia"]   = df.get("moment_of_inertia", pd.Series())

    # Order code fields from param columns
    out["oc_shaft_type"] = df.get("param_e_value", pd.Series())  # shaft param
    out["oc_flange"]     = df.get("param_d_value", pd.Series())
    out["oc_ppr"]        = df.get("param_b_value", pd.Series())
    out["oc_interface"]  = df.get("param_a_value", pd.Series())
    out["oc_connector"]  = df.get("param_c_value", pd.Series())

    out.index = range(len(out))
    return _finalise(out)


# ?? EPC ETL ????????????????????????????????????????????????????????????????????

def _etl_epc(path: str, families: list[str]) -> pd.DataFrame:
    print(f"  [EPC] Loading families {families} from {path}")
    df = pd.read_csv(path, low_memory=False)
    df = df[df["family"].isin(families)].copy()
    print(f"  [EPC] {len(df):,} rows after family filter")

    out = pd.DataFrame()
    out["manufacturer"]   = df.get("manufacturer", pd.Series()).map(normalise_mfr).fillna("EPC")
    out["part_number"]    = df.get("product_code", df.get("part_number", pd.Series())).astype(str)
    out["product_family"] = df.get("family", df.get("product_family", pd.Series()))
    out["encoder_type"]   = df.get("encoder_type", pd.Series())
    out["sensing_method"] = None
    out["source_pdf"]     = df.get("source_pdf", pd.Series())
    out["order_pattern"]  = df.get("order_pattern", pd.Series())

    out["resolution_ppr"]  = df.get("resolution_ppr", pd.Series()).map(safe_float)
    out["ppr_range_min"]   = None
    out["ppr_range_max"]   = None
    out["is_programmable"] = False

    out["output_circuit_canonical"] = _v_circuit(df.get("interface_canonical", df.get("output_circuits", pd.Series())))
    out["output_signals"]      = df.get("output_signals", pd.Series())
    out["num_output_channels"] = None
    out["max_output_freq_hz"]  = df.get("max_output_frequency_hz", pd.Series()).map(safe_float)

    vlo, vhi = _v_voltage(df.get("supply_voltage", pd.Series()))
    out["supply_voltage_min_v"] = vlo
    out["supply_voltage_max_v"] = vhi
    out["output_current_ma"]    = df.get("permissible_load_per_channel", pd.Series()).map(parse_current_ma)
    out["power_consumption_typ_mw"] = df.get("power_consumption", pd.Series()).map(parse_power_mw)
    out["reverse_polarity_protection"] = df.get("reverse_polarity_protection", pd.Series()).map(to_bool)
    out["short_circuit_protection"]    = df.get("short_circuit_protection", pd.Series()).map(to_bool)

    out["housing_diameter_mm"] = df.get("family", pd.Series()).map(housing_from_family)
    out["shaft_diameter_mm"]   = df.get("shaft_diameter_mm", pd.Series()).map(safe_float)
    out["shaft_type"]          = None
    out["flange_type"]         = None
    out["connection_type"]     = df.get("connection_type", pd.Series())
    out["connector_pins"]      = None

    out["ip_rating"]            = _v_ip(df.get("protection_rating", pd.Series()))
    tlo, thi = _v_temp(df.get("operating_temp_range", pd.Series()))
    out["operating_temp_min_c"] = tlo
    out["operating_temp_max_c"] = thi
    out["max_speed_rpm_peak"]   = df.get("max_speed_rpm", pd.Series()).map(parse_speed_rpm)
    out["shock_resistance"]     = df.get("shock_resistance", pd.Series())
    out["vibration_resistance"] = df.get("vibration_resistance", pd.Series())

    out["weight_g"]           = df.get("weight_kg", pd.Series()).map(lambda x: parse_weight_g(x, "kg"))
    out["startup_torque_ncm"] = df.get("startup_torque", pd.Series()).map(parse_torque_ncm)
    out["shaft_load_radial_n"] = df.get("shaft_load_radial_n", pd.Series()).map(safe_float)
    out["shaft_load_axial_n"]  = df.get("shaft_load_axial_n", pd.Series()).map(safe_float)
    out["moment_of_inertia"]   = df.get("moment_of_inertia", pd.Series())

    out["oc_shaft_type"] = df.get("param_e_value", pd.Series())
    out["oc_flange"]     = df.get("param_d_value", pd.Series())
    out["oc_ppr"]        = df.get("param_b_value", pd.Series())
    out["oc_interface"]  = df.get("param_a_value", pd.Series())
    out["oc_connector"]  = df.get("param_c_value", pd.Series())

    out.index = range(len(out))
    return _finalise(out)


# ?? Lika ETL ???????????????????????????????????????????????????????????????????

def _etl_lika(lika_dir: str) -> pd.DataFrame:
    import glob
    csvs = sorted(glob.glob(os.path.join(lika_dir, "*.csv")))
    print(f"  [Lika] Concatenating {len(csvs)} CSVs")
    dfs = []
    for p in csvs:
        dfs.append(pd.read_csv(p, low_memory=False))
    df = pd.concat(dfs, ignore_index=True)
    print(f"  [Lika] {len(df):,} rows total")

    out = pd.DataFrame(index=df.index)
    out["manufacturer"]   = "Lika"
    out["part_number"]    = df.get("product_code", pd.Series()).astype(str)
    out["product_family"] = df.get("family", pd.Series())
    out["encoder_type"]   = df.get("encoder_type", pd.Series())
    out["sensing_method"] = df.get("operating_principle", pd.Series())
    out["source_pdf"]     = df.get("source_pdf", pd.Series())
    out["order_pattern"]  = df.get("order_pattern", pd.Series())

    out["resolution_ppr"]  = df.get("resolution_ppr", pd.Series()).map(safe_float)
    out["ppr_range_min"]   = df.get("ppr_range_min", pd.Series()).map(safe_float)
    out["ppr_range_max"]   = df.get("ppr_range_max", pd.Series()).map(safe_float)
    out["is_programmable"] = (
        df.get("ppr_range_min", pd.Series()).notna() & df.get("ppr_range_max", pd.Series()).notna()
    )

    # output_circuits column in Lika
    raw_oc = df.get("output_circuits", df.get("interface_canonical", pd.Series()))
    out["output_circuit_canonical"] = _v_circuit(raw_oc)
    out["output_signals"]      = None
    out["num_output_channels"] = None
    out["max_output_freq_hz"]  = df.get("counting_frequency_hz", pd.Series()).map(safe_float)

    vlo, vhi = _v_voltage(df.get("supply_voltage", pd.Series()))
    out["supply_voltage_min_v"] = vlo
    out["supply_voltage_max_v"] = vhi
    out["output_current_ma"]    = df.get("output_current_ma", pd.Series()).map(safe_float)
    out["power_consumption_typ_mw"] = df.get("power_consumption_ma", pd.Series()).map(
        lambda x: first_float(str(x)) * 24 if pd.notna(x) else None)
    out["reverse_polarity_protection"] = df.get("reverse_polarity_protection", pd.Series()).map(to_bool)
    out["short_circuit_protection"]    = df.get("short_circuit_protection", pd.Series()).map(to_bool)

    out["housing_diameter_mm"] = df.get("family", pd.Series()).map(housing_from_family)
    out["shaft_diameter_mm"]   = df.get("shaft_diameter_mm", pd.Series()).map(safe_float)
    out["shaft_type"]          = df.get("param_shaft_value", pd.Series())
    out["flange_type"]         = None
    out["connection_type"]     = df.get("connection_type", pd.Series())
    out["connector_pins"]      = df.get("connection_type", pd.Series()).map(
        lambda x: re.search(r"(\d+)\s*pin", str(x), re.I) and
                  int(re.search(r"(\d+)\s*pin", str(x), re.I).group(1)))

    out["ip_rating"]            = _v_ip(df.get("protection_rating", pd.Series()))
    tlo, thi = _v_temp(df.get("operating_temp_range", pd.Series()))
    out["operating_temp_min_c"] = tlo
    out["operating_temp_max_c"] = thi
    out["max_speed_rpm_peak"]   = df.get("max_speed_rpm", pd.Series()).map(parse_speed_rpm)
    out["shock_resistance"]     = df.get("shock_resistance", pd.Series())
    out["vibration_resistance"] = df.get("vibration_resistance", pd.Series())

    out["weight_g"]           = df.get("weight_g", pd.Series()).map(safe_float)
    out["startup_torque_ncm"] = df.get("starting_torque_ncm", pd.Series()).map(safe_float)
    out["shaft_load_radial_n"] = df.get("shaft_load_n", pd.Series()).map(safe_float)
    out["shaft_load_axial_n"]  = None
    out["moment_of_inertia"]   = None

    out["oc_shaft_type"] = df.get("param_shaft_value", pd.Series())
    out["oc_flange"]     = None
    out["oc_ppr"]        = df.get("param_ppr_value", pd.Series())
    out["oc_interface"]  = df.get("param_circuit_value", pd.Series())
    out["oc_connector"]  = df.get("param_connection_value", pd.Series())

    out.index = range(len(out))
    return _finalise(out)


# ?? Wachendorff ETL ????????????????????????????????????????????????????????????

def _etl_wachendorff(wach_dir: str) -> pd.DataFrame:
    import glob
    csvs = sorted(glob.glob(os.path.join(wach_dir, "*.csv")))
    print(f"  [Wachendorff] Concatenating {len(csvs)} CSVs")
    dfs = []
    for p in csvs:
        dfs.append(pd.read_csv(p, low_memory=False))
    df = pd.concat(dfs, ignore_index=True)
    print(f"  [Wachendorff] {len(df):,} rows total")

    out = pd.DataFrame(index=df.index)
    out["manufacturer"]   = "Wachendorff"
    out["part_number"]    = df.get("product_code", pd.Series()).astype(str)
    out["product_family"] = df.get("family", pd.Series())
    out["encoder_type"]   = df.get("encoder_type", pd.Series())
    out["sensing_method"] = df.get("operating_principle", pd.Series())
    out["source_pdf"]     = df.get("source_pdf", pd.Series())
    out["order_pattern"]  = df.get("order_pattern", pd.Series())

    out["resolution_ppr"]  = df.get("resolution_ppr", pd.Series()).map(safe_float)
    out["ppr_range_min"]   = df.get("ppr_range_min", pd.Series()).map(safe_float)
    out["ppr_range_max"]   = df.get("ppr_range_max", pd.Series()).map(safe_float)
    out["is_programmable"] = (
        df.get("ppr_range_min", pd.Series()).notna() & df.get("ppr_range_max", pd.Series()).notna()
    )

    raw_oc = df.get("output_circuits", df.get("interface_canonical", pd.Series()))
    out["output_circuit_canonical"] = _v_circuit(raw_oc)
    out["output_signals"]      = None
    out["num_output_channels"] = df.get("channels", pd.Series()).map(safe_float)
    out["max_output_freq_hz"]  = df.get("max_pulse_frequency_hz", pd.Series()).map(safe_float)

    vlo, vhi = _v_voltage(df.get("supply_voltage", pd.Series()))
    out["supply_voltage_min_v"] = vlo
    out["supply_voltage_max_v"] = vhi
    out["output_current_ma"]    = df.get("output_current_ma", pd.Series()).map(safe_float)
    out["power_consumption_typ_mw"] = None
    out["reverse_polarity_protection"] = df.get("reverse_polarity_protection", pd.Series()).map(to_bool)
    out["short_circuit_protection"]    = df.get("short_circuit_protection", pd.Series()).map(to_bool)

    # Wachendorff has flange_diameter_mm as housing proxy
    out["housing_diameter_mm"] = df.get("flange_diameter_mm",
                                         df.get("family", pd.Series()).map(housing_from_family))
    out["shaft_diameter_mm"]   = df.get("shaft_diameter_mm", pd.Series()).map(safe_float)
    out["shaft_type"]          = df.get("param_shaft_value", pd.Series())
    out["flange_type"]         = df.get("flange_type", pd.Series())
    out["connection_type"]     = df.get("connection_type", pd.Series())
    out["connector_pins"]      = None

    out["ip_rating"]            = _v_ip(df.get("protection_rating", pd.Series()))
    tlo, thi = _v_temp(df.get("operating_temp_range", pd.Series()))
    out["operating_temp_min_c"] = tlo
    out["operating_temp_max_c"] = thi
    out["max_speed_rpm_peak"]   = df.get("max_speed_rpm", pd.Series()).map(parse_speed_rpm)
    out["shock_resistance"]     = df.get("shock_resistance", pd.Series())
    out["vibration_resistance"] = df.get("vibration_resistance", pd.Series())

    out["weight_g"]           = df.get("weight_g", pd.Series()).map(safe_float)
    out["startup_torque_ncm"] = df.get("starting_torque_ncm", pd.Series()).map(safe_float)
    out["shaft_load_radial_n"] = df.get("shaft_load_radial_n", pd.Series()).map(safe_float)
    out["shaft_load_axial_n"]  = df.get("shaft_load_axial_n", pd.Series()).map(safe_float)
    out["moment_of_inertia"]   = None

    out["oc_shaft_type"] = df.get("param_shaft_value", pd.Series())
    out["oc_flange"]     = None
    out["oc_ppr"]        = df.get("param_ppr_value", pd.Series())
    out["oc_interface"]  = df.get("param_circuit_value", pd.Series())
    out["oc_connector"]  = df.get("param_connection_value", pd.Series())

    out.index = range(len(out))
    return _finalise(out)


# ?? Nidec ETL ??????????????????????????????????????????????????????????????????

def _etl_nidec(path: str, max_rows: int = NIDEC_CAP) -> pd.DataFrame:
    print(f"  [Nidec] Loading {path} (cap={max_rows:,})")
    df = pd.read_csv(path, low_memory=False, nrows=max_rows)
    print(f"  [Nidec] {len(df):,} rows")

    out = pd.DataFrame(index=df.index)
    out["manufacturer"]   = "Nidec"
    out["part_number"]    = df.get("product_code", pd.Series()).astype(str)
    out["product_family"] = df.get("family", pd.Series())
    out["encoder_type"]   = "incremental"
    out["sensing_method"] = df.get("encoder_technology", pd.Series())
    out["source_pdf"]     = None
    out["order_pattern"]  = None

    out["resolution_ppr"]  = df.get("resolution_ppr", pd.Series()).map(safe_float)
    out["ppr_range_min"]   = None
    out["ppr_range_max"]   = None
    out["is_programmable"] = df.get("is_dual_output", pd.Series()).map(to_bool)

    # output_signals -> map to canonical
    raw_sig = df.get("output_signals", pd.Series())
    def _nidec_circuit(s):
        if not s or str(s).strip() in ("", "nan"):
            return None
        s = str(s)
        if "/A" in s or "diff" in s.lower():
            return "TTL"
        if "open" in s.lower():
            return "Open Collector"
        return "Push-Pull"
    out["output_circuit_canonical"] = raw_sig.map(_nidec_circuit)
    out["output_signals"]      = raw_sig
    out["num_output_channels"] = df.get("param_channels", pd.Series()).map(safe_float)
    # max_output_frequency_khz -> Hz
    out["max_output_freq_hz"]  = df.get("max_output_frequency_khz", pd.Series()).map(
        lambda x: (safe_float(x) or 0) * 1000 if safe_float(x) else None)

    vlo, vhi = _v_voltage(df.get("supply_voltage_vdc", pd.Series()))
    out["supply_voltage_min_v"] = vlo
    out["supply_voltage_max_v"] = vhi
    out["output_current_ma"]    = df.get("current_ma", pd.Series()).map(safe_float)
    out["power_consumption_typ_mw"] = None
    out["reverse_polarity_protection"] = None
    out["short_circuit_protection"]    = None

    out["housing_diameter_mm"] = df.get("family", pd.Series()).map(housing_from_family)
    out["shaft_diameter_mm"]   = df.get("param_rotor_bore", pd.Series()).map(safe_float)
    out["shaft_type"]          = df.get("param_shaft", pd.Series()).map(
        lambda x: "Hollow" if str(x).strip().upper() in ("H", "B") else (
                  "Solid"  if str(x).strip().upper() in ("S", "A") else None)
        if pd.notna(x) else None)
    out["flange_type"]     = df.get("param_flange", pd.Series())
    out["connection_type"] = df.get("param_connector", pd.Series())
    out["connector_pins"]  = None

    out["ip_rating"]            = _v_ip(df.get("protection_rating", pd.Series()))
    out["operating_temp_min_c"] = df.get("operating_temp_min_c", pd.Series()).map(safe_float)
    out["operating_temp_max_c"] = df.get("operating_temp_max_c", pd.Series()).map(safe_float)
    out["max_speed_rpm_peak"]   = df.get("max_speed_rpm", pd.Series()).map(parse_speed_rpm)
    out["shock_resistance"]     = df.get("shock_g", pd.Series()).map(
        lambda x: f"{x}g" if pd.notna(x) else None)
    out["vibration_resistance"] = df.get("vibration_g", pd.Series()).map(
        lambda x: f"{x}g" if pd.notna(x) else None)

    out["weight_g"]           = df.get("weight_kg", pd.Series()).map(lambda x: parse_weight_g(x, "kg"))
    out["startup_torque_ncm"] = None
    out["shaft_load_radial_n"] = df.get("shaft_load_radial_n", pd.Series()).map(safe_float)
    out["shaft_load_axial_n"]  = df.get("shaft_load_axial_n", pd.Series()).map(safe_float)
    out["moment_of_inertia"]   = None

    out["oc_shaft_type"] = df.get("param_shaft", pd.Series())
    out["oc_flange"]     = df.get("param_flange", pd.Series())
    out["oc_ppr"]        = df.get("param_ppr", pd.Series())
    out["oc_interface"]  = df.get("param_line_driver", pd.Series())
    out["oc_connector"]  = df.get("param_connector", pd.Series())

    out.index = range(len(out))
    return _finalise(out)


# ?? Sick ETL ???????????????????????????????????????????????????????????????????

def _etl_sick(path: str) -> pd.DataFrame:
    print(f"  [Sick] Loading {path}")
    df = pd.read_csv(path, low_memory=False)
    print(f"  [Sick] {len(df):,} rows")

    out = pd.DataFrame(index=df.index)
    out["manufacturer"]   = "Sick"
    out["part_number"]    = df.get("product_name", pd.Series()).astype(str)
    out["product_family"] = df.get("family", pd.Series())
    out["encoder_type"]   = "incremental"
    out["sensing_method"] = None
    out["source_pdf"]     = None
    out["order_pattern"]  = None

    # PPR: prefer oc_ppr field, fall back to "Pulses per revolution"
    ppr_raw = df.get("oc_ppr", pd.Series())
    ppr_fb  = df.get("Pulses per revolution", pd.Series())
    out["resolution_ppr"]  = ppr_raw.map(safe_float).fillna(ppr_fb.map(safe_float))
    out["ppr_range_min"]   = None
    out["ppr_range_max"]   = None
    out["is_programmable"] = False

    out["output_circuit_canonical"] = _v_circuit(df.get("oc_interface",
                                                  df.get("Communication interface", pd.Series())))
    out["output_signals"]      = None
    out["num_output_channels"] = df.get("Number of signal channels", pd.Series()).map(safe_float)
    out["max_output_freq_hz"]  = df.get("Output frequency", pd.Series()).map(parse_freq_hz)

    vlo, vhi = _v_voltage(df.get("Supply voltage", pd.Series()))
    out["supply_voltage_min_v"] = vlo
    out["supply_voltage_max_v"] = vhi
    out["output_current_ma"]    = df.get("Load current", pd.Series()).map(parse_current_ma)
    out["power_consumption_typ_mw"] = df.get("Power consumption", pd.Series()).map(parse_power_mw)
    out["reverse_polarity_protection"] = df.get("Reverse polarity protection", pd.Series()).map(to_bool)
    out["short_circuit_protection"]    = df.get("Short-circuit protection", pd.Series()).map(to_bool)

    out["housing_diameter_mm"] = df.get("family", pd.Series()).map(housing_from_family)
    out["shaft_diameter_mm"]   = df.get("oc_bore_diameter",
                                         df.get("Shaft diameter", pd.Series())).map(safe_float)
    out["shaft_type"]          = df.get("oc_shaft_type",
                                         df.get("Mechanical design", pd.Series()))
    out["flange_type"]         = df.get("oc_flange_type",
                                         df.get("Flange type / stator coupling", pd.Series()))
    out["connection_type"]     = df.get("oc_connector",
                                         df.get("Connection type", pd.Series()))
    out["connector_pins"]      = None

    out["ip_rating"]            = _v_ip(df.get("Enclosure rating", pd.Series()))
    tlo, thi = _v_temp(df.get("Operating temperature range", pd.Series()))
    out["operating_temp_min_c"] = tlo
    out["operating_temp_max_c"] = thi
    out["max_speed_rpm_peak"]   = df.get("Operating speed", pd.Series()).map(parse_speed_rpm)
    out["shock_resistance"]     = df.get("Resistance to shocks", pd.Series())
    out["vibration_resistance"] = df.get("Resistance to vibration", pd.Series())

    wt_col = next((df[c] for c in ["Net unit weight", "Weight", "Unit weight"] if c in df.columns),
                  pd.Series(dtype=str))
    out["weight_g"]           = _v_weight_g(wt_col, "g")
    out["startup_torque_ncm"] = df.get("Start up torque", pd.Series()).map(parse_torque_ncm)
    out["shaft_load_radial_n"] = df.get("Permissible shaft loading", pd.Series()).map(safe_float)
    out["shaft_load_axial_n"]  = None
    out["moment_of_inertia"]   = df.get("Moment of inertia of the rotor", pd.Series())

    out["oc_shaft_type"] = df.get("oc_shaft_type", pd.Series())
    out["oc_flange"]     = df.get("oc_flange_type", pd.Series())
    out["oc_ppr"]        = df.get("oc_ppr", pd.Series())
    out["oc_interface"]  = df.get("oc_interface", pd.Series())
    out["oc_connector"]  = df.get("oc_connector", pd.Series())

    out.index = range(len(out))
    return _finalise(out)


# ?? Baumer ETL ?????????????????????????????????????????????????????????????????

def _etl_baumer(path: str) -> pd.DataFrame:
    print(f"  [Baumer] Loading {path}")
    df = pd.read_csv(path, low_memory=False)
    print(f"  [Baumer] {len(df):,} rows")

    out = pd.DataFrame(index=df.index)
    out["manufacturer"]   = "Baumer"
    out["part_number"]    = df.get("product_code", pd.Series()).astype(str)
    out["product_family"] = df.get("family", pd.Series())
    out["encoder_type"]   = df.get("encoder_type", pd.Series())
    out["sensing_method"] = df.get("sensing_method", pd.Series())
    out["source_pdf"]     = df.get("source_file", pd.Series())
    out["order_pattern"]  = None

    out["resolution_ppr"]  = df.get("resolution_ppr", pd.Series()).map(safe_float)
    out["ppr_range_min"]   = df.get("ppr_range_min", pd.Series()).map(safe_float)
    out["ppr_range_max"]   = df.get("ppr_range_max", pd.Series()).map(safe_float)
    prog = df.get("oc_is_programmable", pd.Series(dtype=object))
    out["is_programmable"] = prog.map(
        lambda x: str(x).strip().lower() in ("true", "1", "yes")
        if pd.notna(x) else False).astype(bool)

    out["output_circuit_canonical"] = _v_circuit(df.get("interface_canonical", pd.Series()))
    out["output_signals"]      = df.get("output_stages", pd.Series())
    out["num_output_channels"] = None
    out["max_output_freq_hz"]  = df.get("max_output_frequency", pd.Series()).map(parse_freq_hz)

    vlo, vhi = _v_voltage(df.get("supply_voltage", pd.Series()))
    out["supply_voltage_min_v"] = vlo
    out["supply_voltage_max_v"] = vhi
    out["output_current_ma"]    = None
    out["power_consumption_typ_mw"] = df.get("power_consumption", pd.Series()).map(parse_power_mw)
    out["reverse_polarity_protection"] = df.get("reverse_polarity_protection", pd.Series()).map(to_bool)
    out["short_circuit_protection"]    = df.get("short_circuit_protection", pd.Series()).map(to_bool)

    out["housing_diameter_mm"] = df.get("size_flange",
                                         df.get("family", pd.Series()).map(housing_from_family))
    out["shaft_diameter_mm"]   = df.get("oc_shaft_diam_mm", pd.Series()).map(safe_float)
    out["shaft_type"]          = df.get("shaft_type", df.get("oc_shaft_type", pd.Series()))
    out["flange_type"]         = df.get("flange", df.get("oc_flange", pd.Series()))
    out["connection_type"]     = df.get("connection_type", df.get("oc_connection", pd.Series()))
    out["connector_pins"]      = None

    out["ip_rating"]            = _v_ip(df.get("oc_protection_ip",
                                                df.get("protection_rating", pd.Series())))
    tlo, thi = _v_temp(df.get("operating_temp_range", pd.Series()))
    out["operating_temp_min_c"] = tlo
    out["operating_temp_max_c"] = thi
    out["max_speed_rpm_peak"]   = df.get("max_speed_rpm", pd.Series()).map(parse_speed_rpm)
    out["shock_resistance"]     = df.get("shock_vibration_resistance", pd.Series())
    out["vibration_resistance"] = df.get("shock_vibration_resistance", pd.Series())

    out["weight_g"]           = df.get("weight_approx", pd.Series()).map(
        lambda x: parse_weight_g(str(x)) if pd.notna(x) else None)
    out["startup_torque_ncm"] = df.get("starting_torque", pd.Series()).map(parse_torque_ncm)
    out["shaft_load_radial_n"] = df.get("admitted_shaft_load", pd.Series()).map(safe_float)
    out["shaft_load_axial_n"]  = None
    out["moment_of_inertia"]   = df.get("rotor_moment_of_inertia", pd.Series())

    out["oc_shaft_type"] = df.get("oc_shaft_type", pd.Series())
    out["oc_flange"]     = df.get("oc_flange", pd.Series())
    out["oc_ppr"]        = df.get("oc_ppr_from_code", pd.Series())
    out["oc_interface"]  = df.get("oc_output_stage", pd.Series())
    out["oc_connector"]  = df.get("oc_connection", pd.Series())

    out.index = range(len(out))
    return _finalise(out)


# ?? Main build function ????????????????????????????????????????????????????????

def build_unified(
    output_path: str = OUT_PATH,
    nidec_cap:   int = NIDEC_CAP,
    only_mfr:    str = None,
) -> pd.DataFrame:
    """Build the full unified CSV. Returns the resulting DataFrame."""
    parts = []

    mfrs = only_mfr.lower().split(",") if only_mfr else None

    def _want(name):
        return mfrs is None or name.lower() in mfrs

    if _want("kubler"):
        parts.append(_etl_kubler(KUBLER_CSV, KUBLER_FAMILIES))
    if _want("epc"):
        parts.append(_etl_epc(EPC_CSV, EPC_FAMILIES))
    if _want("lika"):
        parts.append(_etl_lika(LIKA_DIR))
    if _want("wachendorff"):
        parts.append(_etl_wachendorff(WACH_DIR))
    if _want("nidec"):
        parts.append(_etl_nidec(NIDEC_CSV, nidec_cap))
    if _want("sick"):
        parts.append(_etl_sick(SICK_CSV))
    if _want("baumer"):
        parts.append(_etl_baumer(BAUMER_CSV))

    unified = pd.concat(parts, ignore_index=True)

    # Final type coercions
    for col in ["resolution_ppr", "ppr_range_min", "ppr_range_max",
                "housing_diameter_mm", "shaft_diameter_mm",
                "supply_voltage_min_v", "supply_voltage_max_v",
                "operating_temp_min_c", "operating_temp_max_c",
                "max_speed_rpm_peak", "max_output_freq_hz",
                "output_current_ma", "power_consumption_typ_mw",
                "weight_g", "startup_torque_ncm",
                "shaft_load_radial_n", "shaft_load_axial_n"]:
        if col in unified.columns:
            unified[col] = pd.to_numeric(unified[col], errors="coerce")

    unified = unified[UNIFIED_COLS]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    unified.to_csv(output_path, index=False)

    print(f"\nSaved {len(unified):,} rows -> {output_path}")
    return unified


def coverage_report(df: pd.DataFrame):
    """Print field coverage report."""
    scored = ["resolution_ppr", "output_circuit_canonical", "housing_diameter_mm",
              "shaft_diameter_mm", "ip_rating", "operating_temp_min_c", "operating_temp_max_c",
              "max_speed_rpm_peak", "supply_voltage_min_v", "connection_type"]
    print("\n=== Coverage Report ===")
    print(f"Total rows: {len(df):,}")
    print()
    by_mfr = df.groupby("manufacturer").size()
    for mfr, cnt in by_mfr.items():
        print(f"  {mfr:<20s}: {cnt:>8,}")
    print()
    print(f"{'Field':<35s}  {'%Filled':>8s}")
    print("-" * 46)
    for col in scored:
        if col in df.columns:
            pct = df[col].notna().mean() * 100
            print(f"  {col:<33s}  {pct:>7.1f}%")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build unified encoder CSV")
    ap.add_argument("--mfr",       default=None, help="Comma-separated manufacturer names to process only")
    ap.add_argument("--nidec-cap", type=int, default=NIDEC_CAP, help="Max Nidec rows (default 50000)")
    ap.add_argument("--coverage",  action="store_true", help="Print coverage report after build")
    ap.add_argument("--output",    default=OUT_PATH, help="Output CSV path")
    args = ap.parse_args()

    df = build_unified(output_path=args.output, nidec_cap=args.nidec_cap, only_mfr=args.mfr)
    if args.coverage:
        coverage_report(df)
