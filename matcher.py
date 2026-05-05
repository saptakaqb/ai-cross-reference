# -*- coding: utf-8 -*-
"""
matcher.py  v11  (V17)
=======================
V17 changes:
  • ATEX T1 hard stop  — ATEX source cannot match non-ATEX candidate
  • sensing_method scored T3 (weight 0.04) — optical vs magnetic now penalised
  • corrosion_protection_class scored T3 (weight 0.02)
  • Tighter PPR penalty curve (≥95% now 0.78 not 0.95)
  • Completeness penalty — prevents inflated scores when many fields missing
  • Score returned as raw float (no rounding) → display precision handled by UI
  • Warning flags added to generate_explanation output
  • All mismatched fields returned sorted by priority (worst first)
  • Weight rebalance to accommodate new T3 fields (total still sums to 1.0)

Tier 1 Hard Stops (score=0, candidate excluded):
    1. shaft_type mismatch (solid vs hollow)
    2. hollow bore diameter mismatch (|delta| > 1 mm)
    3. output voltage class cross (TTL/OC ↔ HTL)
    4. ATEX cross — source is ATEX-certified but candidate is not

Tier 2 Near-Hard:
    resolution_ppr (0.25), output_circuit_canonical (0.20),
    housing_diameter_mm (0.15), shaft_diameter_mm (0.12)

Tier 3 Soft:
    ip_rating (0.07), sensing_method (0.04), max_speed (0.04+0.01),
    operating_temp (0.03+0.02), supply_voltage (0.03+0.03),
    connection_type (0.03), corrosion_protection_class (0.02)

Completeness penalty (V17):
    When < 70% of total weight is computable (both values present),
    apply a mild penalty to prevent perfect scores from few matched fields.
"""

import math
import re
import pandas as pd
from typing import Optional

from schema import (
    OUTPUT_CIRCUIT_CANONICAL,
    SHAFT_TYPE_CANONICAL,
    OUTPUT_VOLTAGE_CLASS,
    IP_HIERARCHY,
    CORROSION_RANK,
    ppr_score as _ppr_score_fn,
    parse_corrosion_rank,
)
from normalizer import safe_float

# ---------------------------------------------------------------------------
# Voltage class map (built once at import)
# ---------------------------------------------------------------------------
_VC_MAP: dict = {}
def _build_vc_map():
    m = {}
    for raw, canon in OUTPUT_CIRCUIT_CANONICAL.items():
        cls = OUTPUT_VOLTAGE_CLASS.get(canon) or OUTPUT_VOLTAGE_CLASS.get(raw)
        if cls:
            m[raw] = cls; m[canon] = cls
    for k, v in OUTPUT_VOLTAGE_CLASS.items():
        m[k] = v
    return m
_VC_MAP = _build_vc_map()

# ---------------------------------------------------------------------------
# DEFAULT WEIGHTS  (sum = 1.0)
# V17: sensing_method + corrosion_protection_class added; others rebalanced
# ---------------------------------------------------------------------------
DEFAULT_WEIGHTS = {
    "resolution_ppr":              0.25,
    "output_circuit_canonical":    0.14,
    "housing_diameter_mm":         0.14,
    "shaft_diameter_mm":           0.12,
    "ip_rating":                   0.07,
    "sensing_method":              0.04,   # NEW — optical vs magnetic
    "supply_voltage_min_v":        0.03,
    "supply_voltage_max_v":        0.03,
    "operating_temp_min_c":        0.02,
    "operating_temp_max_c":        0.03,
    "max_speed_rpm_peak":          0.04,
    "max_speed_rpm_cont":          0.01,
    "connection_type":             0.03,
    "corrosion_protection_class":  0.02,   # NEW — ISO 12944-2 class
    "num_output_channels":         0.01,
    "flange_type":                 0.01,
    "connector_pins":              0.01,
}
assert abs(sum(DEFAULT_WEIGHTS.values()) - 1.0) < 1e-6, \
    f"DEFAULT_WEIGHTS sum = {sum(DEFAULT_WEIGHTS.values()):.6f}, must be 1.0"

# ---------------------------------------------------------------------------
# OC partial-credit table
# ---------------------------------------------------------------------------
_OC_SIMILARITY = {
    ("TTL RS422",        "Open Collector"):      0.5,
    ("Open Collector",   "TTL RS422"):           0.5,
    ("Push-Pull",        "TTL RS422"):           0.3,
    ("TTL RS422",        "Push-Pull"):           0.3,
    ("Push-Pull",        "Open Collector"):      0.4,
    ("Open Collector",   "Push-Pull"):           0.4,
    ("TTL/HTL Universal","TTL RS422"):           0.8,
    ("TTL RS422",        "TTL/HTL Universal"):   0.8,
    ("TTL/HTL Universal","Push-Pull"):           0.8,
    ("Push-Pull",        "TTL/HTL Universal"):   0.8,
    ("TTL/HTL Universal","Open Collector"):      0.6,
    ("Open Collector",   "TTL/HTL Universal"):   0.6,
    ("PP/LD Universal",  "TTL RS422"):           0.8,
    ("TTL RS422",        "PP/LD Universal"):     0.8,
    ("PP/LD Universal",  "Push-Pull"):           0.8,
    ("Push-Pull",        "PP/LD Universal"):     0.8,
    ("PP/LD Universal",  "TTL/HTL Universal"):   0.9,
    ("TTL/HTL Universal","PP/LD Universal"):     0.9,
    ("PP/LD Universal",  "Open Collector"):      0.4,
    ("Open Collector",   "PP/LD Universal"):     0.4,
    ("Sin/Cos",          "TTL RS422"):           0.1,
    ("TTL RS422",        "Sin/Cos"):             0.1,
    ("Sin/Cos",          "Push-Pull"):           0.1,
    ("Push-Pull",        "Sin/Cos"):             0.1,
}

_FLANGE_SIMILARITY = {
    ("servo","clamping"):0.7,("clamping","servo"):0.7,
    ("servo","face_mount"):0.8,("face_mount","servo"):0.8,
    ("servo","synchro"):0.7,("synchro","servo"):0.7,
    ("clamping","synchro"):0.6,("synchro","clamping"):0.6,
}

_CONN_SIMILARITY = {
    ("M12","M23"):0.7,("M23","M12"):0.7,
    ("M12","M8"):0.6,("M8","M12"):0.6,
    ("M12","cable"):0.4,("cable","M12"):0.4,
    ("M23","cable"):0.4,("cable","M23"):0.4,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _sf(v) -> Optional[float]: return safe_float(v)
def _ss(v) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    return None if s.lower() in ("nan","none","") else s
def _is_prog(v) -> bool:
    if v is None: return False
    if isinstance(v, bool): return v
    return str(v).strip().lower() in ("true","1","yes")
def _circuit(v) -> Optional[str]:
    raw = _ss(v)
    if not raw: return None
    return OUTPUT_CIRCUIT_CANONICAL.get(raw, raw)
def _voltage_class(circuit: str) -> Optional[str]:
    if circuit is None: return None
    return OUTPUT_VOLTAGE_CLASS.get(str(circuit).strip())
def _is_atex(v) -> bool:
    if v is None: return False
    s = str(v).strip().lower()
    return s not in ("", "nan", "none", "false", "0")

# ---------------------------------------------------------------------------
# Tier 1: Hard stops
# ---------------------------------------------------------------------------
def _check_hard_stops(source: dict, candidate: dict) -> Optional[str]:
    # 1. Shaft type mismatch
    s_shaft = _ss(source.get("shaft_type"))
    c_shaft = _ss(candidate.get("shaft_type"))
    if s_shaft and c_shaft:
        s_solid = "hollow" not in s_shaft.lower()
        c_solid = "hollow" not in c_shaft.lower()
        if s_solid != c_solid:
            return f"Shaft type mismatch: source={s_shaft}, candidate={c_shaft}"

    # 2. Hollow bore diameter mismatch
    if s_shaft and c_shaft and "hollow" in s_shaft.lower() and "hollow" in c_shaft.lower():
        s_bore = _sf(source.get("shaft_diameter_mm"))
        c_bore = _sf(candidate.get("shaft_diameter_mm"))
        if s_bore and c_bore and abs(s_bore - c_bore) > 1.0:
            return (f"Hollow bore mismatch: {s_bore}mm vs {c_bore}mm "
                    f"(|delta|={abs(s_bore-c_bore):.1f}mm > 1mm)")

    # 3. Output voltage class cross
    s_oc = _circuit(source.get("output_circuit_canonical"))
    c_oc = _circuit(candidate.get("output_circuit_canonical"))
    if s_oc and c_oc:
        s_cls = _voltage_class(s_oc); c_cls = _voltage_class(c_oc)
        if (s_cls and c_cls
                and s_cls != "universal" and c_cls != "universal"
                and s_cls != "analog"    and c_cls != "analog"
                and s_cls != c_cls):
            return (f"Voltage class mismatch: source={s_oc} ({s_cls}), "
                    f"candidate={c_oc} ({c_cls})")

    # 4. ATEX certification (V17 NEW)
    if _is_atex(source.get("is_atex_certified")) and not _is_atex(candidate.get("is_atex_certified")):
        return ("ATEX-certified encoder — candidate is not ATEX certified. "
                "Cannot substitute in hazardous zone (Ex) applications.")

    return None

# ---------------------------------------------------------------------------
# Field similarity functions
# ---------------------------------------------------------------------------
def _sim_ppr(source: dict, candidate: dict) -> tuple:
    return _ppr_score_fn(
        source_ppr  = _sf(source.get("resolution_ppr")),
        source_prog = _is_prog(source.get("is_programmable")),
        source_min  = _sf(source.get("ppr_range_min")),
        source_max  = _sf(source.get("ppr_range_max")),
        cand_ppr    = _sf(candidate.get("resolution_ppr")),
        cand_prog   = _is_prog(candidate.get("is_programmable")),
        cand_min    = _sf(candidate.get("ppr_range_min")),
        cand_max    = _sf(candidate.get("ppr_range_max")),
    )

def _sim_output_circuit(s_oc, c_oc) -> Optional[float]:
    if s_oc is None or c_oc is None: return None
    s_oc, c_oc = str(s_oc).strip(), str(c_oc).strip()
    if s_oc.lower() == c_oc.lower(): return 1.0
    return _OC_SIMILARITY.get((s_oc, c_oc), 0.0)

def _sim_housing(s_hd, c_hd) -> tuple:
    s_hd, c_hd = _sf(s_hd), _sf(c_hd)
    if s_hd is None or c_hd is None: return None, False
    denom = max(s_hd, c_hd, 1e-9)
    rel_diff = abs(s_hd - c_hd) / denom
    cap_fired = rel_diff > 0.30
    score = max(0.0, 1.0 - rel_diff * 2.0)
    return score, cap_fired

def _sim_shaft_dia(s_dia, c_dia) -> Optional[float]:
    s_dia, c_dia = _sf(s_dia), _sf(c_dia)
    if s_dia is None or c_dia is None: return None
    diff = abs(s_dia - c_dia)
    if diff <= 0.5: return 1.0
    return max(0.0, 1.0 - (diff - 0.5) / max(s_dia, 1.0))

def _sim_voltage(s_min, s_max, c_min, c_max) -> Optional[float]:
    s_min, s_max = _sf(s_min), _sf(s_max)
    c_min, c_max = _sf(c_min), _sf(c_max)
    if None in (s_min, s_max, c_min, c_max): return None
    if s_max <= s_min or s_max < 3.0: return None
    span = max(s_max - s_min, 1.0)
    sim_lo = 1.0 if c_min <= s_min else max(0.0, 1.0 - (c_min - s_min) / span)
    sim_hi = 1.0 if c_max >= s_max else max(0.0, 1.0 - (s_max - c_max) / span)
    return (sim_lo + sim_hi) / 2.0

def _sim_ip(s_ip, c_ip) -> Optional[float]:
    s_ip_f = _sf(s_ip); c_ip_f = _sf(c_ip)
    if s_ip_f is None or c_ip_f is None: return None
    s_rank = IP_HIERARCHY.get(int(float(s_ip_f)), 0)
    c_rank = IP_HIERARCHY.get(int(float(c_ip_f)), 0)
    delta = c_rank - s_rank
    if delta >= 0: return 1.0
    if delta == -1: return 0.5
    return 0.0

def _sim_temp(s_min, s_max, c_min, c_max) -> Optional[float]:
    s_min, s_max = _sf(s_min), _sf(s_max)
    c_min, c_max = _sf(c_min), _sf(c_max)
    if None in (s_min, s_max, c_min, c_max): return None
    if c_max < s_max - 10.0: return 0.0
    span = max(s_max - s_min, 1.0)
    sim_lo = 1.0 if c_min <= s_min else max(0.0, 1.0 - (c_min - s_min) / span * 2)
    sim_hi = 1.0 if c_max >= s_max else max(0.0, 1.0 - (s_max - c_max) / span * 2)
    return (sim_lo + sim_hi) / 2.0

def _sim_speed(s_peak, s_cont, c_peak) -> tuple:
    c_peak = _sf(c_peak); s_cont = _sf(s_cont) or _sf(s_peak); s_peak = _sf(s_peak)
    if c_peak is None or s_peak is None: return None, False
    cap_fired = bool(s_cont and c_peak < s_cont * 0.9)
    if c_peak >= s_peak: return 1.0, cap_fired
    return max(0.0, c_peak / s_peak), cap_fired

def _sim_channels(s_ch, c_ch) -> Optional[float]:
    if s_ch is None or c_ch is None: return None
    s_ch, c_ch = str(s_ch), str(c_ch)
    if s_ch == c_ch: return 1.0
    if s_ch in ("AB","A") and "ABN" in c_ch: return 0.8
    if "ABN" in s_ch and c_ch in ("AB","A"): return 0.5
    return 0.4

def _sim_flange(s_ft, c_ft) -> Optional[float]:
    if s_ft is None or c_ft is None: return None
    s_ft, c_ft = str(s_ft).strip(), str(c_ft).strip()
    if s_ft == c_ft: return 1.0
    return _FLANGE_SIMILARITY.get((s_ft, c_ft), 0.2)

def _sim_connection(s_ct, c_ct) -> Optional[float]:
    if s_ct is None or c_ct is None: return None
    s_ct, c_ct = str(s_ct).strip(), str(c_ct).strip()
    if s_ct == c_ct: return 1.0
    return _CONN_SIMILARITY.get((s_ct, c_ct), 0.2)

def _sim_pins(s_pins, c_pins) -> Optional[float]:
    s_pins, c_pins = _sf(s_pins), _sf(c_pins)
    if s_pins is None or c_pins is None: return None
    if c_pins >= s_pins: return 1.0 if c_pins == s_pins else 0.9
    return max(0.3, 1.0 - (s_pins - c_pins) * 0.15)

def _sim_sensing(s_sm, c_sm) -> Optional[float]:
    """V17: Score optical vs magnetic mismatch."""
    if s_sm is None: return None
    s = str(s_sm).strip().lower()
    if not s or s in ("nan","none",""): return None
    c = str(c_sm).strip().lower() if c_sm else ""
    if not c or c in ("nan","none",""): return None
    if s == c: return 1.0
    # Optical ↔ Magnetic is a significant technology difference
    if ("optical" in s and "magnetic" in c) or ("magnetic" in s and "optical" in c):
        return 0.3
    return 0.5   # other sensing method differences

def _sim_corrosion(s_cc, c_cc) -> Optional[float]:
    """V17: ISO 12944-2 corrosion class comparison."""
    s_rank = parse_corrosion_rank(s_cc) if s_cc else None
    if s_rank is None: return None  # source has no rating → not scored
    c_rank = parse_corrosion_rank(c_cc) if c_cc else None
    if c_rank is None: return 0.3   # candidate unrated but source has a requirement
    if c_rank >= s_rank: return 1.0
    diff = s_rank - c_rank
    return max(0.0, 1.0 - diff * 0.3)  # 1 class below=0.7, 2=0.4, 3+=0.1

def _completeness_factor(active_weight: float) -> float:
    """
    V17: Mild penalty when few fields are comparable.
    Prevents a perfect score on PPR alone from yielding 100%.
    • active_weight ≥ 0.70 → no penalty (factor = 1.0)
    • 0.40 ≤ active_weight < 0.70 → linear ramp 0.92 → 1.0
    • active_weight < 0.40 → factor = 0.92
    """
    if active_weight >= 0.70: return 1.0
    if active_weight >= 0.40: return 0.92 + 0.08 * (active_weight - 0.40) / 0.30
    return 0.92

# ---------------------------------------------------------------------------
# V18: Tiebreaker (ported from matcher_poc v1_test)
# ---------------------------------------------------------------------------
_IP_RANK_TB = {40:0,50:1,54:2,64:3,65:4,66:5,67:6,68:7,69:8}

def _tiebreak_score(source: dict, candidate: dict) -> float:
    """
    Returns a micro-adjustment (0.000–0.004) to differentiate candidates
    with otherwise identical weighted scores at 2 decimal place display.
    Applied AFTER main scoring + caps. Never changes the tier boundary.
    """
    tb = 0.0
    def _f(v):
        try: return float(v)
        except: return None

    # 1. IP rating — prefer candidate ≥ source
    s_ip = _f(source.get("ip_rating")); c_ip = _f(candidate.get("ip_rating"))
    if s_ip is not None and c_ip is not None:
        s_r = _IP_RANK_TB.get(int(s_ip), 0); c_r = _IP_RANK_TB.get(int(c_ip), 0)
        if c_r > s_r:    tb += 0.0010   # bonus: higher IP
        elif c_r == s_r: tb += 0.0005   # same IP

    # 2. Max speed — prefer candidate speed ≥ source
    sv_sp = _f(source.get("max_speed_rpm_peak"))
    cv_sp = _f(candidate.get("max_speed_rpm_peak"))
    if sv_sp and cv_sp and sv_sp > 0:
        ratio = cv_sp / sv_sp
        if ratio >= 1.0:   tb += 0.0010
        elif ratio >= 0.9: tb += 0.0005

    # 3. Operating temp range — prefer wider candidate coverage
    sv_lo = _f(source.get("operating_temp_min_c")); sv_hi = _f(source.get("operating_temp_max_c"))
    cv_lo = _f(candidate.get("operating_temp_min_c")); cv_hi = _f(candidate.get("operating_temp_max_c"))
    if all(v is not None for v in (sv_lo, sv_hi, cv_lo, cv_hi)):
        if cv_lo < sv_lo: tb += 0.0005   # wider low end
        if cv_hi > sv_hi: tb += 0.0005   # wider high end

    # 4. Connection type — prefer exact match
    s_ct = _ss(source.get("connection_type")); c_ct = _ss(candidate.get("connection_type"))
    if s_ct and c_ct and s_ct.strip().lower() == c_ct.strip().lower():
        tb += 0.0008

    return min(tb, 0.004)

# ---------------------------------------------------------------------------
# Core scorer
# ---------------------------------------------------------------------------
def score_pair(source: dict, candidate: dict, weights: dict = None,
               verbose: bool = False) -> tuple:
    """
    Score source vs candidate encoder.
    Returns (score 0-1, details dict).
    Score is a raw float — no rounding. UI formats to 2 decimal places.
    """
    if weights is None: weights = DEFAULT_WEIGHTS
    _wsum = sum(weights.values()) or 1.0
    w = {k: v / _wsum for k, v in weights.items()}
    details = {}

    # Tier 1 hard stops
    stop_reason = _check_hard_stops(source, candidate)
    if stop_reason:
        details["_hard_stop"] = {"reason": stop_reason, "score": 0.0}
        return (0.0, details)

    weighted_sum  = 0.0
    active_weight = 0.0
    cap_housing   = False
    cap_speed     = False

    def _accumulate(key, sim, label=""):
        nonlocal weighted_sum, active_weight
        wt = w.get(key, 0.0)
        if wt > 0 and sim is not None:
            weighted_sum  += wt * sim
            active_weight += wt
            if verbose:
                details[key] = {
                    "sim": sim, "weight": wt, "label": label,
                    "source": source.get(key, "–"), "candidate": candidate.get(key, "–"),
                }

    # PPR
    sim_ppr, label_ppr = _sim_ppr(source, candidate)
    _accumulate("resolution_ppr", sim_ppr, label_ppr)

    # Output circuit
    s_oc = _circuit(source.get("output_circuit_canonical"))
    c_oc = _circuit(candidate.get("output_circuit_canonical"))
    sim_oc = _sim_output_circuit(s_oc, c_oc)
    label_oc = ("Exact" if sim_oc == 1.0 else
                f"~{int((sim_oc or 0)*100)}% compatible" if sim_oc else "Incompatible")
    _accumulate("output_circuit_canonical", sim_oc, label_oc)
    if verbose and s_oc:
        details.setdefault("output_circuit_canonical", {})
        details["output_circuit_canonical"].update({"source": s_oc, "candidate": c_oc})

    # Housing diameter
    sim_hd, cap_housing = _sim_housing(
        source.get("housing_diameter_mm"), candidate.get("housing_diameter_mm"))
    label_hd = ("Adapter may be needed" if cap_housing
                 else "Compatible" if sim_hd and sim_hd >= 0.9
                 else f"~{int((sim_hd or 0)*100)}% compatible")
    _accumulate("housing_diameter_mm", sim_hd, label_hd)
    if verbose:
        details.setdefault("housing_diameter_mm", {})
        details["housing_diameter_mm"].update({
            "source": source.get("housing_diameter_mm"),
            "candidate": candidate.get("housing_diameter_mm"),
        })

    # Shaft diameter
    sim_sd = _sim_shaft_dia(
        source.get("shaft_diameter_mm"), candidate.get("shaft_diameter_mm"))
    label_sd = ("Within coupling tolerance" if sim_sd and sim_sd >= 0.9
                else f"~{int((sim_sd or 0)*100)}% compatible")
    _accumulate("shaft_diameter_mm", sim_sd, label_sd)
    if verbose:
        details.setdefault("shaft_diameter_mm", {})
        details["shaft_diameter_mm"].update({
            "source": source.get("shaft_diameter_mm"),
            "candidate": candidate.get("shaft_diameter_mm"),
        })

    # IP rating
    sim_ip = _sim_ip(source.get("ip_rating"), candidate.get("ip_rating"))
    label_ip = ("Meets or exceeds" if sim_ip == 1.0
                else "Slightly below" if sim_ip == 0.5 else "Below minimum")
    _accumulate("ip_rating", sim_ip, label_ip)
    if verbose:
        details.setdefault("ip_rating", {})
        details["ip_rating"].update({
            "source": source.get("ip_rating"),
            "candidate": candidate.get("ip_rating"),
        })

    # Sensing method (V17 — now scored)
    sim_sm = _sim_sensing(source.get("sensing_method"), candidate.get("sensing_method"))
    label_sm = ("Same sensing technology" if sim_sm == 1.0
                else "⚠ Optical vs Magnetic — technology difference" if sim_sm and sim_sm <= 0.35
                else "Sensing method difference")
    _accumulate("sensing_method", sim_sm, label_sm)
    if verbose:
        details.setdefault("sensing_method", {})
        details["sensing_method"].update({
            "source": source.get("sensing_method"),
            "candidate": candidate.get("sensing_method"),
            "sim": sim_sm, "weight": w.get("sensing_method", 0),
            "label": label_sm,
        })

    # Supply voltage
    sim_sv = _sim_voltage(
        source.get("supply_voltage_min_v"), source.get("supply_voltage_max_v"),
        candidate.get("supply_voltage_min_v"), candidate.get("supply_voltage_max_v"))
    label_sv = ("Compatible" if sim_sv and sim_sv >= 0.9
                else f"~{int((sim_sv or 0)*100)}% overlap")
    w_sv = w.get("supply_voltage_min_v", 0) + w.get("supply_voltage_max_v", 0)
    if w_sv > 0 and sim_sv is not None:
        weighted_sum  += w_sv * sim_sv
        active_weight += w_sv
    if verbose:
        details["_voltage_range"] = {
            "sim": sim_sv, "weight": w_sv, "label": label_sv,
            "source": f"{source.get('supply_voltage_min_v')}–{source.get('supply_voltage_max_v')} V",
            "candidate": f"{candidate.get('supply_voltage_min_v')}–{candidate.get('supply_voltage_max_v')} V",
        }

    # Operating temperature
    sim_temp = _sim_temp(
        source.get("operating_temp_min_c"), source.get("operating_temp_max_c"),
        candidate.get("operating_temp_min_c"), candidate.get("operating_temp_max_c"))
    label_temp = ("Covers required range" if sim_temp and sim_temp >= 0.9
                  else f"~{int((sim_temp or 0)*100)}% coverage" if sim_temp else "Below minimum")
    w_temp = w.get("operating_temp_min_c", 0) + w.get("operating_temp_max_c", 0)
    if w_temp > 0 and sim_temp is not None:
        weighted_sum  += w_temp * sim_temp
        active_weight += w_temp
    if verbose:
        details["_temp_range"] = {
            "sim": sim_temp, "weight": w_temp, "label": label_temp,
            "source": f"{source.get('operating_temp_min_c')}–{source.get('operating_temp_max_c')} °C",
            "candidate": f"{candidate.get('operating_temp_min_c')}–{candidate.get('operating_temp_max_c')} °C",
        }

    # Speed
    sim_sp, cap_speed = _sim_speed(
        source.get("max_speed_rpm_peak"), source.get("max_speed_rpm_cont"),
        candidate.get("max_speed_rpm_peak"))
    label_sp = ("Adequate" if sim_sp == 1.0
                else "Below required" if cap_speed
                else f"~{int((sim_sp or 0)*100)}% of required")
    w_sp = w.get("max_speed_rpm_peak", 0) + w.get("max_speed_rpm_cont", 0)
    if w_sp > 0 and sim_sp is not None:
        weighted_sum  += w_sp * sim_sp
        active_weight += w_sp
    if verbose:
        details["_speed"] = {
            "sim": sim_sp, "weight": w_sp, "label": label_sp,
            "source_peak": source.get("max_speed_rpm_peak"),
            "candidate_peak": candidate.get("max_speed_rpm_peak"),
        }

    # Connection type
    sim_ct = _sim_connection(source.get("connection_type"), candidate.get("connection_type"))
    _accumulate("connection_type", sim_ct,
                "Match" if sim_ct == 1.0 else "Adapter available" if sim_ct and sim_ct > 0.3 else "Incompatible")
    if verbose:
        details.setdefault("connection_type", {})
        details["connection_type"].update({
            "source": source.get("connection_type"),
            "candidate": candidate.get("connection_type"),
        })

    # Corrosion protection class (V17 NEW)
    sim_cp = _sim_corrosion(
        source.get("corrosion_protection_class"),
        candidate.get("corrosion_protection_class"))
    label_cp = ("Same/higher corrosion class" if sim_cp == 1.0
                else "No corrosion rating — check if required" if sim_cp and sim_cp <= 0.35
                else f"Lower corrosion class — verify suitability" if sim_cp else "Not applicable")
    _accumulate("corrosion_protection_class", sim_cp, label_cp)
    if verbose:
        details.setdefault("corrosion_protection_class", {})
        details["corrosion_protection_class"].update({
            "source": source.get("corrosion_protection_class"),
            "candidate": candidate.get("corrosion_protection_class"),
            "sim": sim_cp, "weight": w.get("corrosion_protection_class", 0),
            "label": label_cp,
        })

    # Output channels
    sim_ch = _sim_channels(
        source.get("num_output_channels"), candidate.get("num_output_channels"))
    _accumulate("num_output_channels", sim_ch,
                "Match" if sim_ch == 1.0 else "Z pulse may be missing")
    if verbose:
        details.setdefault("num_output_channels", {})
        details["num_output_channels"].update({
            "source": source.get("num_output_channels"),
            "candidate": candidate.get("num_output_channels"),
        })

    # Flange type
    sim_ft = _sim_flange(source.get("flange_type"), candidate.get("flange_type"))
    _accumulate("flange_type", sim_ft,
                "Match" if sim_ft == 1.0 else "Adapter may be needed")
    if verbose:
        details.setdefault("flange_type", {})
        details["flange_type"].update({
            "source": source.get("flange_type"),
            "candidate": candidate.get("flange_type"),
        })

    # Connector pins
    sim_cp2 = _sim_pins(source.get("connector_pins"), candidate.get("connector_pins"))
    _accumulate("connector_pins", sim_cp2,
                "Match" if sim_cp2 and sim_cp2 >= 1.0
                else f"{int((sim_cp2 or 0)*100)}% pin compatible")
    if verbose:
        details.setdefault("connector_pins", {})
        details["connector_pins"].update({
            "source": source.get("connector_pins"),
            "candidate": candidate.get("connector_pins"),
        })

    # Active weight normalisation
    if active_weight == 0: return (0.0, details)
    total = weighted_sum / active_weight

    # V17: Completeness penalty
    factor = _completeness_factor(active_weight)
    total  = total * factor

    # Caps
    if cap_housing:
        total = min(total, 0.75)
        details["_cap_housing"] = {
            "reason": "Housing diameter differs >30% — score capped at 0.75", "cap": 0.75}
    if cap_speed:
        total = min(total, 0.70)
        details["_cap_speed"] = {
            "reason": "Candidate max speed < 90% of source — score capped at 0.70", "cap": 0.70}

    # Warning flags (V17 — not scoring, just advisory)
    warnings = []
    s_sm = _ss(source.get("sensing_method")); c_sm = _ss(candidate.get("sensing_method"))
    if s_sm and c_sm and s_sm.lower() != c_sm.lower():
        warnings.append({
            "code": "SENSING_MISMATCH",
            "msg": f"⚠ Sensing technology differs: source={s_sm}, candidate={c_sm}. "
                   f"Optical and magnetic encoders differ in accuracy, contamination tolerance, and cost.",
        })
    s_si = _ss(source.get("shaft_insulation_v")); c_si = _ss(candidate.get("shaft_insulation_v"))
    if s_si and not c_si:
        warnings.append({
            "code": "SHAFT_INSULATION_MISSING",
            "msg": f"⚠ Source has shaft insulation ({s_si}) but candidate has none. "
                   f"Shaft insulation is required to protect motor bearings from stray VFD currents.",
        })
    if verbose and warnings:
        details["_warnings"] = warnings

    # V18: Tiebreaker micro-adjustment (0.000–0.004)
    # Differentiates near-identical candidates at 2dp precision without changing tier
    tb = _tiebreak_score(source, candidate)
    total = min(1.0, total + tb)
    if verbose:
        details["_tiebreaker"] = tb

    # Return raw float — no rounding. UI displays to 2 decimal places.
    return (total, details)

# ---------------------------------------------------------------------------
# Explanation card
# ---------------------------------------------------------------------------
_FIELD_META = {
    "resolution_ppr":              ("Resolution (PPR)",       2),
    "output_circuit_canonical":    ("Output Circuit",         2),
    "housing_diameter_mm":         ("Housing Diameter",       2),
    "shaft_diameter_mm":           ("Shaft Diameter",         2),
    "ip_rating":                   ("IP Rating",              3),
    "sensing_method":              ("Sensing Method",         3),  # V17
    "_voltage_range":              ("Supply Voltage",         3),
    "_temp_range":                 ("Operating Temperature",  3),
    "_speed":                      ("Max Speed",              3),
    "connection_type":             ("Connection Type",        3),
    "corrosion_protection_class":  ("Corrosion Protection",   3),  # V17
    "num_output_channels":         ("Output Channels",        3),
    "flange_type":                 ("Flange Type",            3),
    "connector_pins":              ("Connector Pins",         3),
}

# Symbol for sim score
def _sim_symbol(sim: float, tier: int) -> str:
    if sim is None: return "ℹ"
    if sim >= 0.99: return "✅"
    if sim >= 0.85: return "🟢"
    if sim >= 0.65: return "🟡"
    if sim >= 0.30: return "🟠"
    return "🔴"


def generate_explanation(source: dict, candidate: dict, weights: dict = None) -> dict:
    score, details = score_pair(source, candidate, weights, verbose=True)

    tier = ("strong"   if score >= 0.90 else
            "good"     if score >= 0.80 else
            "moderate" if score >= 0.50 else
            "weak"     if score > 0.0  else
            "no_match")

    hard_stop = details.get("_hard_stop", {}).get("reason")
    warnings  = details.get("_warnings", [])

    fields = []
    for key, (label, _tier) in _FIELD_META.items():
        if key not in details: continue
        d   = details[key]
        sim = d.get("sim")
        wt  = d.get("weight", 0)
        pts = (wt * (sim or 0) * 100) if sim is not None else None
        src_val = d.get("source") or d.get("source_peak") or "–"
        cnd_val = d.get("candidate") or d.get("candidate_peak") or "–"
        symbol  = _sim_symbol(sim, _tier)
        fields.append({
            "label":      label,
            "source_val": str(src_val),
            "cand_val":   str(cnd_val),
            "sim":        sim,
            "pts":        pts,
            "note":       d.get("label", ""),
            "tier":       _tier,
            "weight":     wt,
            "symbol":     symbol,
        })

    # Sort fields: worst first within each tier group, then by tier
    def _sort_key(f):
        sim = f.get("sim"); tier_f = f.get("tier", 0)
        sim_sort = (sim if sim is not None else 1.1)  # missing fields go last
        return (tier_f if tier_f > 0 else 99, sim_sort)
    fields.sort(key=_sort_key)

    # Informational fields (not scored)
    for field, label, unit in [
        ("weight_g",           "Weight",       "g"),
        ("startup_torque_ncm", "Startup Torque","Ncm"),
        ("shaft_insulation_v", "Shaft Insulation", ""),
        ("mttfd_years",        "MTTFd",        "years"),
        ("bearing_life_rev",   "Bearing Life", ""),
    ]:
        sv = source.get(field); cv = candidate.get(field)
        if sv or cv:
            fields.append({
                "label":      label,
                "source_val": f"{sv} {unit}".strip() if sv else "–",
                "cand_val":   f"{cv} {unit}".strip() if cv else "–",
                "sim":        None, "pts": None,
                "note":       "Informational",
                "tier": 0, "weight": 0, "symbol": "ℹ",
            })

    caps = []
    if "_cap_housing" in details: caps.append(details["_cap_housing"]["reason"])
    if "_cap_speed"   in details: caps.append(details["_cap_speed"]["reason"])

    ppr_d     = details.get("resolution_ppr", {})
    ppr_badge = ppr_d.get("label","") if "Programmable" in ppr_d.get("label","") else None
    src_pn    = source.get("part_number", "Source")
    cnd_mfr   = candidate.get("manufacturer", "")

    if hard_stop:
        summary = f"HARD STOP: {hard_stop}"
    elif tier == "strong":
        summary = (f"Strong match ({score*100:.2f}%). "
                   f"{cnd_mfr} {candidate.get('part_number','')} is a direct replacement candidate.")
    elif tier == "good":
        summary = f"Good match ({score*100:.2f}%). Minor differences — review before ordering."
    elif tier == "moderate":
        summary = f"Moderate match ({score*100:.2f}%). Engineering review recommended."
    else:
        summary = f"Weak match ({score*100:.2f}%). No close equivalent found."

    return {
        "score":     score,
        "score_pct": f"{score*100:.2f}%",   # V17: full precision
        "tier":      tier,
        "hard_stop": hard_stop,
        "fields":    fields,
        "caps":      caps,
        "warnings":  warnings,
        "summary":   summary,
        "ppr_badge": ppr_badge,
    }

# ---------------------------------------------------------------------------
# Prefilter
# ---------------------------------------------------------------------------
def _prefilter(pool_df: pd.DataFrame, source: dict) -> pd.DataFrame:
    import numpy as np
    df = pool_df

    s_shaft = _ss(source.get("shaft_type"))
    if s_shaft:
        s_hollow = "hollow" in s_shaft.lower()
        if "_is_hollow" in df.columns:
            c_hollow = df["_is_hollow"].values.astype(bool)
            _st = df["shaft_type"]
            _st_arr = _st.astype(object).values if hasattr(_st, 'cat') else _st.values
            c_has    = np.array([str(v) if v is not None else "" for v in _st_arr]) != ""
            mask     = ~c_has | (c_hollow == s_hollow)
        else:
            sc = df["shaft_type"].fillna("").str.lower()
            c_hollow = sc.str.contains("hollow", na=False)
            mask = (c_hollow == s_hollow) | (sc == "")
        filtered = df[mask]
        if len(filtered) > 0: df = filtered

    s_hd = _sf(source.get("housing_diameter_mm"))
    if s_hd and len(df) > 500:
        hd    = pd.to_numeric(df["housing_diameter_mm"], errors="coerce").values
        denom = np.where(hd > s_hd, hd, s_hd)
        rel   = np.abs(hd - s_hd) / np.maximum(denom, 1e-9)
        mask  = np.isnan(hd) | (rel <= 0.50)
        filtered = df[mask]
        if len(filtered) > 0: df = filtered

    s_oc = source.get("output_circuit_canonical")
    if s_oc and len(df) > 500:
        s_cls = _VC_MAP.get(str(s_oc).strip(), _VC_MAP.get(
            OUTPUT_CIRCUIT_CANONICAL.get(str(s_oc).strip(), str(s_oc).strip()), ""))
        if s_cls and s_cls not in ("universal","analog",""):
            if "_oc_class" in df.columns:
                _oc_col = df["_oc_class"]
                c_cls = _oc_col.astype(object).values if hasattr(_oc_col, 'cat') else _oc_col.values
                c_cls = np.array([str(v) if v is not None else "" for v in c_cls])
                mask  = (c_cls == "") | (c_cls == s_cls) | (c_cls == "universal") | (c_cls == "analog")
            else:
                oc_vals = df["output_circuit_canonical"].fillna("").values
                c_cls   = np.array([_VC_MAP.get(v, _VC_MAP.get(
                               OUTPUT_CIRCUIT_CANONICAL.get(v, ""), "")) for v in oc_vals])
                mask    = (c_cls == "") | (c_cls == s_cls) | (c_cls == "universal") | (c_cls == "analog")
            filtered = df[mask]
            if len(filtered) > 0: df = filtered

    s_ppr  = _sf(source.get("resolution_ppr"))
    s_prog = _is_prog(source.get("is_programmable"))
    if s_ppr and not s_prog and len(df) > 500:
        c_ppr  = pd.to_numeric(df["resolution_ppr"], errors="coerce")
        _pr    = df["is_programmable"].fillna("").astype(str).str.lower()
        c_prog = (_pr == "true") | (_pr == "1")
        lo     = c_ppr.where(c_ppr <= s_ppr, s_ppr)
        hi     = c_ppr.where(c_ppr >= s_ppr, s_ppr)
        ratio  = lo / hi.replace(0, float("nan"))
        ppr_ok = (ratio >= 0.50) | c_prog | c_ppr.isna()
        filtered = df[ppr_ok]
        if len(filtered) > 0: df = filtered

    return df

# ---------------------------------------------------------------------------
# Vectorized fast scorer
# ---------------------------------------------------------------------------
def _vectorized_scores(source: dict, pool: pd.DataFrame, weights: dict) -> pd.Series:
    import numpy as np
    _wsum = sum(weights.values()) or 1.0
    w = {k: v / _wsum for k, v in weights.items()}
    N = len(pool)
    if N == 0: return pd.Series(dtype=float)

    idx = pool.index
    weighted_sum  = np.zeros(N)
    active_weight = np.zeros(N)
    cap_housing   = np.zeros(N, dtype=bool)
    cap_speed     = np.zeros(N, dtype=bool)

    def _col(c):
        if c not in pool.columns: return np.full(N, np.nan)
        s = pool[c]
        if hasattr(s,'cat'): s = s.astype(object)
        return pd.to_numeric(s, errors="coerce").values

    def _scol(c):
        if c not in pool.columns: return np.full(N,"",dtype=object)
        s = pool[c]
        if hasattr(s,'cat'): s = s.astype(object)
        return s.fillna("").astype(str).values

    def _accum(key, sim_arr):
        wt = w.get(key,0.0)
        if wt <= 0: return
        ok = ~np.isnan(sim_arr)
        weighted_sum[ok]  += wt * sim_arr[ok]
        active_weight[ok] += wt

    # Hard stops
    s_shaft  = _ss(source.get("shaft_type")) or ""
    s_hollow = "hollow" in s_shaft.lower() if s_shaft else None
    c_shaft_arr = _scol("shaft_type")
    if s_hollow is not None:
        c_hollow_arr = np.array(["hollow" in v.lower() for v in c_shaft_arr])
        c_has        = np.array([v != "" for v in c_shaft_arr])
        shaft_stop   = c_has & (c_hollow_arr != s_hollow)
    else:
        shaft_stop = np.zeros(N, dtype=bool)

    bore_stop = np.zeros(N, dtype=bool)
    if s_hollow:
        s_bore = _sf(source.get("shaft_diameter_mm")); c_bore = _col("shaft_diameter_mm")
        if s_bore:
            both_hol = np.array(["hollow" in v.lower() for v in c_shaft_arr])
            bore_stop = both_hol & (~np.isnan(c_bore)) & (np.abs(c_bore - s_bore) > 1.0)

    s_oc  = _circuit(source.get("output_circuit_canonical"))
    s_cls = _voltage_class(s_oc) if s_oc else None
    c_oc_arr  = _scol("output_circuit_canonical")
    volt_stop = np.zeros(N, dtype=bool)
    if s_cls and s_cls not in ("universal","analog"):
        _OC_TO_CLS = {k:v for k,v in OUTPUT_VOLTAGE_CLASS.items()}
        _CANON_MAP  = OUTPUT_CIRCUIT_CANONICAL
        def _cls_of(v):
            raw=str(v).strip(); canon=_CANON_MAP.get(raw,raw)
            return _OC_TO_CLS.get(canon) or _OC_TO_CLS.get(raw)
        c_cls_arr = np.array([_cls_of(v) for v in c_oc_arr], dtype=object)
        volt_stop = np.array([
            (c is not None and c not in ("universal","analog") and c != s_cls)
            for c in c_cls_arr
        ])

    # ATEX hard stop (V17)
    atex_stop = np.zeros(N, dtype=bool)
    if _is_atex(source.get("is_atex_certified")):
        if "is_atex_certified" in pool.columns:
            c_atex_arr = _scol("is_atex_certified")
            atex_stop  = np.array([not _is_atex(v) for v in c_atex_arr])

    hard_stop = shaft_stop | bore_stop | volt_stop | atex_stop

    # PPR
    s_ppr  = _sf(source.get("resolution_ppr")); s_prog = _is_prog(source.get("is_programmable"))
    s_pmin = _sf(source.get("ppr_range_min"));  s_pmax = _sf(source.get("ppr_range_max"))
    c_ppr  = _col("resolution_ppr")
    _prog_raw = pool["is_programmable"].fillna("").astype(str).str.lower().values
    c_prog = (_prog_raw == "true") | (_prog_raw == "1")
    c_pmin = _col("ppr_range_min"); c_pmax = _col("ppr_range_max")

    ppr_sim = np.full(N, np.nan)
    if s_ppr and not s_prog:
        has = ~np.isnan(c_ppr)
        ratio = np.where(has, np.minimum(s_ppr,c_ppr)/np.maximum(np.maximum(s_ppr,c_ppr),1e-9), np.nan)
        # V17: tighter PPR curve
        exact_match = has & ((np.abs(c_ppr-s_ppr)<=1) | (ratio==1.0))
        ppr_sim = np.where(has,
            np.where(exact_match, 1.0,
            np.where(ratio>=0.99, 0.92,
            np.where(ratio>=0.95, 0.78,
            np.where(ratio>=0.80, ratio*0.65,
            np.where(ratio>=0.50, ratio*0.45, 0.0))))),
            ppr_sim)
        in_range = c_prog & ~np.isnan(c_pmin) & ~np.isnan(c_pmax) & (c_pmin<=s_ppr) & (s_ppr<=c_pmax)
        ppr_sim  = np.where(in_range, 1.0, ppr_sim)
        ppr_sim  = np.where(c_prog & np.isnan(c_pmin), 0.5, ppr_sim)
    elif s_prog and s_pmin is not None and s_pmax is not None:
        has = ~np.isnan(c_ppr)
        ppr_sim = np.where(has & (s_pmin<=c_ppr) & (c_ppr<=s_pmax), 1.0,
                  np.where(has, 0.0, np.nan))
        overlap = c_prog & ~np.isnan(c_pmin) & ~np.isnan(c_pmax) & (np.minimum(s_pmax,c_pmax)>np.maximum(s_pmin,c_pmin))
        ppr_sim = np.where(overlap, 1.0, ppr_sim)
    else:
        ppr_sim = np.full(N, 0.5)
    _accum("resolution_ppr", ppr_sim)

    # Output circuit
    wt_oc = w.get("output_circuit_canonical",0)
    if wt_oc > 0 and s_oc:
        s_oc_lo = s_oc.lower()
        oc_sim  = np.array([
            (1.0 if str(v).strip().lower()==s_oc_lo
             else _OC_SIMILARITY.get((s_oc,str(v).strip()),0.0))
            for v in c_oc_arr
        ])
        has_oc = np.array([v.strip()!="" for v in c_oc_arr])
        _accum("output_circuit_canonical", np.where(has_oc, oc_sim, np.nan))

    # Housing
    s_hd = _sf(source.get("housing_diameter_mm")); c_hd = _col("housing_diameter_mm")
    if w.get("housing_diameter_mm",0) > 0 and s_hd:
        has_hd = ~np.isnan(c_hd)
        denom  = np.maximum(np.maximum(s_hd,c_hd),1e-9)
        rel    = np.abs(c_hd-s_hd)/denom
        hd_sim = np.maximum(0.0, 1.0-rel*2.0)
        cap_housing = has_hd & (rel>0.30)
        _accum("housing_diameter_mm", np.where(has_hd, hd_sim, np.nan))

    # Shaft diameter
    s_sd = _sf(source.get("shaft_diameter_mm")); c_sd = _col("shaft_diameter_mm")
    if w.get("shaft_diameter_mm",0) > 0 and s_sd:
        has_sd = ~np.isnan(c_sd)
        diff   = np.abs(c_sd-s_sd)
        sd_sim = np.where(diff<=0.5, 1.0, np.maximum(0.0, 1.0-(diff-0.5)/max(s_sd,1.0)))
        _accum("shaft_diameter_mm", np.where(has_sd, sd_sim, np.nan))

    # IP rating
    s_ip = _sf(source.get("ip_rating")); wt_ip = w.get("ip_rating",0)
    if wt_ip > 0 and s_ip:
        c_ip_raw = _col("ip_rating"); has_ip = ~np.isnan(c_ip_raw)
        s_rank   = IP_HIERARCHY.get(int(float(s_ip)), 0)
        c_ranks  = np.array([IP_HIERARCHY.get(int(float(v)),0) if not np.isnan(v) else 0 for v in c_ip_raw])
        delta    = c_ranks - s_rank
        ip_sim   = np.where(delta>=0, 1.0, np.where(delta==-1, 0.5, 0.0))
        _accum("ip_rating", np.where(has_ip, ip_sim, np.nan))

    # Sensing method (V17)
    s_sm = _ss(source.get("sensing_method")); wt_sm = w.get("sensing_method",0)
    if wt_sm > 0 and s_sm:
        c_sm_arr = _scol("sensing_method")
        s_sm_lo  = s_sm.lower()
        sm_sim   = np.array([
            np.nan if not v.strip() or v.strip().lower() in ("nan","none") else
            1.0 if v.strip().lower()==s_sm_lo else
            0.3 if (("optical" in s_sm_lo and "magnetic" in v.lower()) or
                    ("magnetic" in s_sm_lo and "optical" in v.lower())) else
            0.5
            for v in c_sm_arr
        ])
        _accum("sensing_method", sm_sim)

    # Supply voltage
    s_vmin = _sf(source.get("supply_voltage_min_v")); s_vmax = _sf(source.get("supply_voltage_max_v"))
    w_sv   = w.get("supply_voltage_min_v",0)+w.get("supply_voltage_max_v",0)
    if w_sv>0 and s_vmin and s_vmax and s_vmax>s_vmin and s_vmax>=3.0:
        c_vmin,c_vmax = _col("supply_voltage_min_v"),_col("supply_voltage_max_v")
        has_v  = ~np.isnan(c_vmin)&~np.isnan(c_vmax)
        span   = max(s_vmax-s_vmin,1.0)
        slo    = np.where(c_vmin<=s_vmin,1.0,np.maximum(0.0,1.0-(c_vmin-s_vmin)/span))
        shi    = np.where(c_vmax>=s_vmax,1.0,np.maximum(0.0,1.0-(s_vmax-c_vmax)/span))
        sv_sim = (slo+shi)/2.0
        active_weight[has_v] += w_sv; weighted_sum[has_v] += w_sv*sv_sim[has_v]

    # Temperature
    s_tmin=_sf(source.get("operating_temp_min_c")); s_tmax=_sf(source.get("operating_temp_max_c"))
    w_tmp =w.get("operating_temp_min_c",0)+w.get("operating_temp_max_c",0)
    if w_tmp>0 and s_tmin is not None and s_tmax is not None:
        c_tmin,c_tmax=_col("operating_temp_min_c"),_col("operating_temp_max_c")
        has_t  = ~np.isnan(c_tmin)&~np.isnan(c_tmax)
        span   = max(s_tmax-s_tmin,1.0)
        hard_t = c_tmax<(s_tmax-10.0)
        slo    = np.where(c_tmin<=s_tmin,1.0,np.maximum(0.0,1.0-(c_tmin-s_tmin)/span*2))
        shi    = np.where(c_tmax>=s_tmax,1.0,np.maximum(0.0,1.0-(s_tmax-c_tmax)/span*2))
        t_sim  = np.where(hard_t, 0.0, (slo+shi)/2.0)
        active_weight[has_t]+=w_tmp; weighted_sum[has_t]+=w_tmp*t_sim[has_t]

    # Speed
    s_peak=_sf(source.get("max_speed_rpm_peak")); s_cont=_sf(source.get("max_speed_rpm_cont")) or s_peak
    w_sp  =w.get("max_speed_rpm_peak",0)+w.get("max_speed_rpm_cont",0)
    if w_sp>0 and s_peak:
        c_peak=_col("max_speed_rpm_peak"); has_sp=~np.isnan(c_peak)
        sp_sim=np.where(c_peak>=s_peak,1.0,np.maximum(0.0,c_peak/s_peak))
        if s_cont: cap_speed=cap_speed|(has_sp&(c_peak<s_cont*0.9))
        active_weight[has_sp]+=w_sp; weighted_sum[has_sp]+=w_sp*sp_sim[has_sp]

    # Connection type
    wt_ct=w.get("connection_type",0); s_ct=_ss(source.get("connection_type"))
    if wt_ct>0 and s_ct:
        c_ct_arr=_scol("connection_type")
        ct_sim  =np.array([1.0 if v.strip()==s_ct else _CONN_SIMILARITY.get((s_ct,v.strip()),0.2) for v in c_ct_arr])
        has_ct  =np.array([v.strip()!="" for v in c_ct_arr])
        _accum("connection_type", np.where(has_ct, ct_sim, np.nan))

    # Corrosion protection (V17)
    wt_cp=w.get("corrosion_protection_class",0)
    s_cc =_ss(source.get("corrosion_protection_class"))
    if wt_cp>0 and s_cc:
        s_rank_cp=parse_corrosion_rank(s_cc)
        if s_rank_cp is not None:
            c_cc_arr=_scol("corrosion_protection_class")
            def _cp_sim(v):
                if not v.strip(): return np.nan
                c_r=parse_corrosion_rank(v)
                if c_r is None: return 0.3
                diff=s_rank_cp-c_r
                return 1.0 if diff<=0 else max(0.0,1.0-diff*0.3)
            cp_sim=np.array([_cp_sim(v) for v in c_cc_arr])
            _accum("corrosion_protection_class", cp_sim)

    # Normalize
    has_any = active_weight > 0
    scores  = np.where(has_any, weighted_sum/np.where(has_any,active_weight,1.0), 0.0)

    # V17: Completeness penalty (vectorized)
    cf = np.where(active_weight>=0.70, 1.0,
         np.where(active_weight>=0.40, 0.92+0.08*(active_weight-0.40)/0.30, 0.92))
    scores = scores * cf

    # Caps
    scores = np.where(cap_housing, np.minimum(scores,0.75), scores)
    scores = np.where(cap_speed,   np.minimum(scores,0.70), scores)
    scores = np.where(hard_stop,   0.0, scores)

    return pd.Series(scores, index=idx)

# ---------------------------------------------------------------------------
# find_matches
# ---------------------------------------------------------------------------
_SCORE_KEY_COLS = [
    "resolution_ppr","ppr_range_min","ppr_range_max","is_programmable",
    "output_circuit_canonical","housing_diameter_mm","shaft_diameter_mm",
    "shaft_type","ip_rating","supply_voltage_min_v","supply_voltage_max_v",
    "operating_temp_min_c","operating_temp_max_c","max_speed_rpm_peak",
    "sensing_method","corrosion_protection_class","is_atex_certified",
]

def find_matches(source_row: dict, pool_df: pd.DataFrame,
                 target_mfr: str = None, top_n: int = 5,
                 min_score: float = 0.0,
                 allow_shaft_relaxation: bool = True,
                 weights: dict = None) -> pd.DataFrame:
    if weights is None: weights = DEFAULT_WEIGHTS
    pool = pool_df
    if target_mfr:
        pool = pool[pool["manufacturer"].str.lower() == target_mfr.strip().lower()]
    pool = _prefilter(pool, source_row)
    if len(pool) == 0: return pd.DataFrame()

    key_cols = [c for c in _SCORE_KEY_COLS if c in pool.columns]
    pool_reset = pool.reset_index(drop=True)
    dedup = pool_reset.drop_duplicates(subset=key_cols, keep="first")
    dedup_scores = _vectorized_scores(source_row, dedup, weights)
    dedup = dedup.copy(); dedup["_s"] = dedup_scores.values
    score_map = dedup[key_cols+["_s"]].copy()
    pool_scored = pool_reset.merge(score_map, on=key_cols, how="left")
    pool_scored["_s"] = pool_scored["_s"].fillna(0.0)

    if min_score > 0:
        pool_scored = pool_scored[pool_scored["_s"] > min_score]
    pool_scored = (pool_scored.sort_values("_s", ascending=False)
                               .drop_duplicates(subset=["part_number","manufacturer"]))
    top = pool_scored.head(top_n)[["part_number","manufacturer","product_family","_s"]].copy()
    top = top.rename(columns={"_s":"match_score"})
    return top.reset_index(drop=True)

# ---------------------------------------------------------------------------
# find_matches_with_status
# ---------------------------------------------------------------------------
def find_matches_with_status(source_pn: str, df: pd.DataFrame,
                              target_manufacturer: str = None,
                              source_manufacturer: str = None,
                              top_n: int = 5,
                              weights: dict = None,
                              blocklist: list = None,
                              booklist: list = None) -> tuple:
    if weights is None: weights = DEFAULT_WEIGHTS
    if blocklist is None: blocklist = []
    if booklist  is None: booklist  = []

    pn_upper = source_pn.strip().upper()
    if "_pn_upper" not in df.columns:
        _pn_col = df["part_number"].astype(str).str.upper()
    else:
        _pn_col = df["_pn_upper"]
    mask = _pn_col == pn_upper
    if source_manufacturer:
        m2 = mask & (df["manufacturer"].str.lower() == source_manufacturer.strip().lower())
        if m2.any(): mask = m2
    if not mask.any():
        return (pd.DataFrame(),
                {"tier":"not_found","top_score":0.0,
                 "message":f"Part number '{source_pn}' not found.","fallback":False},
                [])

    source_record = df[mask].iloc[0].to_dict()
    src_mfr       = source_record.get("manufacturer","")

    if target_manufacturer and src_mfr and src_mfr.lower() == target_manufacturer.lower():
        pool = df[df["manufacturer"] == target_manufacturer].copy()
        pool = pool[pool["part_number"].astype(str).str.upper() != source_pn.strip().upper()]
    else:
        pool = df[df["manufacturer"] != src_mfr].copy()
        if target_manufacturer:
            pool = pool[pool["manufacturer"] == target_manufacturer].copy()

    results = find_matches(source_record, pool, top_n=top_n*3, weights=weights)

    fallback = False
    if results.empty or (len(results)>0 and results["match_score"].max()==0):
        source_relaxed = {**source_record, "shaft_type": None}
        pool_relaxed   = pool.copy(); pool_relaxed["shaft_type"] = None
        results  = find_matches(source_relaxed, pool_relaxed, top_n=top_n*3, weights=weights)
        fallback = True

    if results.empty:
        return (pd.DataFrame(),
                {"tier":"weak","top_score":0.0,
                 "message":"No compatible matches found.","fallback":fallback},
                [])

    blocked_pns = {x["match_pn"] for x in blocklist if x.get("query_pn")==source_pn}
    if blocked_pns:
        results = results[~results["part_number"].isin(blocked_pns)]
    boosted_pns = {x["match_pn"] for x in booklist if x.get("query_pn")==source_pn}
    if boosted_pns and not results.empty:
        is_boosted = results["part_number"].isin(boosted_pns)
        results = pd.concat([results[is_boosted], results[~is_boosted]])

    results = results.head(top_n)
    if results.empty:
        return (pd.DataFrame(),
                {"tier":"weak","top_score":0.0,
                 "message":"No compatible matches found after applying feedback filters.","fallback":fallback},
                [])

    top_score = results["match_score"].iloc[0]
    tier = ("strong" if top_score>=0.90 else "good" if top_score>=0.80 else "weak")
    msg  = {"strong":"Strong match — direct replacement candidate.",
            "good":  "Good match — minor differences, review before ordering.",
            "weak":  "Weak match — no close equivalent found."}[tier]

    explanations = []
    for _, res_row in results.iterrows():
        cand_mask = ((df["part_number"]==res_row["part_number"]) &
                     (df["manufacturer"]==res_row["manufacturer"]))
        if cand_mask.any():
            cand_record = df[cand_mask].iloc[0].to_dict()
            explanations.append(generate_explanation(source_record, cand_record, weights))

    status = {
        "tier":      tier,
        "message":   msg,
        "fallback":  fallback,
        "top_score": top_score,
        "ppr_badge": next((e.get("ppr_badge") for e in explanations if e.get("ppr_badge")), None),
    }
    return results, status, explanations
