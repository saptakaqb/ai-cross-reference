# -*- coding: utf-8 -*-
"""
matcher.py  v10
================
Scoring engine: v8 full logic (parameter precedence, active weight normalisation,
Gaussian decay, programmable PPR, partial-credit OC table, directional voltage/IP).
Interface: v9 calling convention + CockroachDB-compatible data model.

Tier 1  Hard Stops  (pre-gate, score=0, candidate excluded entirely):
    1. shaft_type mismatch (solid vs hollow)
    2. hollow bore diameter mismatch (|delta| > 1 mm)
    3. output voltage class cross (TTL/OC low-class <-> HTL high-class)
       push-pull = universal, never triggers a hard stop

Tier 2  Near-Hard   (high weight, heavy penalty):
    resolution_ppr, output_circuit_canonical, housing_diameter_mm, shaft_diameter_mm

Tier 3  Soft        (configurable weights, directional rules):
    supply_voltage, ip_rating, operating_temp, max_speed, connection, channels, flange, pins

Post-scoring caps:
    housing diff > 30%           -> total score capped at 0.75
    candidate speed < 90% source -> total score capped at 0.70

Active weight normalisation (v8 rule):
    score = sum(weight*sim for active fields) / sum(weight for active fields)
    A field is ACTIVE only when BOTH source AND candidate values are non-null.
    Missing data never deflates the score.
"""

import math
import pandas as pd
from typing import Optional

from schema import (
    OUTPUT_CIRCUIT_CANONICAL,
    SHAFT_TYPE_CANONICAL,
    OUTPUT_VOLTAGE_CLASS,
    IP_HIERARCHY,
    ppr_score as _ppr_score_fn,
)
from normalizer import safe_float

# ---------------------------------------------------------------------------
# Default weights  (sum = 1.0, matches v8 precedence framework)
# ---------------------------------------------------------------------------
DEFAULT_WEIGHTS = {
    "resolution_ppr":           0.25,
    "output_circuit_canonical": 0.14,
    "housing_diameter_mm":      0.14,
    "shaft_diameter_mm":        0.12,
    "supply_voltage_min_v":     0.04,
    "supply_voltage_max_v":     0.04,
    "ip_rating":                0.07,
    "operating_temp_min_c":     0.03,
    "operating_temp_max_c":     0.03,
    "max_speed_rpm_peak":       0.04,
    "max_speed_rpm_cont":       0.02,
    "num_output_channels":      0.02,
    "flange_type":              0.02,
    "connection_type":          0.02,
    "connector_pins":           0.02,
}

# Output circuit partial credit table — keys use canonical values from schema.py v11
_OC_SIMILARITY = {
    # TTL RS422 <-> Open Collector (same low-voltage class, passive pull-up)
    ("TTL RS422",        "Open Collector"):      0.5,
    ("Open Collector",   "TTL RS422"):           0.5,
    # Push-Pull <-> TTL RS422 (universal covers low; needs level converter)
    ("Push-Pull",        "TTL RS422"):           0.3,
    ("TTL RS422",        "Push-Pull"):           0.3,
    # Push-Pull <-> Open Collector
    ("Push-Pull",        "Open Collector"):      0.4,
    ("Open Collector",   "Push-Pull"):           0.4,
    # TTL/HTL Universal <-> specific types (universal by definition)
    ("TTL/HTL Universal","TTL RS422"):           0.8,
    ("TTL RS422",        "TTL/HTL Universal"):   0.8,
    ("TTL/HTL Universal","Push-Pull"):           0.8,
    ("Push-Pull",        "TTL/HTL Universal"):   0.8,
    ("TTL/HTL Universal","Open Collector"):      0.6,
    ("Open Collector",   "TTL/HTL Universal"):   0.6,
    # PP/LD Universal <-> TTL RS422 and Push-Pull (Lika universal circuit)
    ("PP/LD Universal",  "TTL RS422"):           0.8,
    ("TTL RS422",        "PP/LD Universal"):     0.8,
    ("PP/LD Universal",  "Push-Pull"):           0.8,
    ("Push-Pull",        "PP/LD Universal"):     0.8,
    ("PP/LD Universal",  "TTL/HTL Universal"):   0.9,
    ("TTL/HTL Universal","PP/LD Universal"):     0.9,
    ("PP/LD Universal",  "Open Collector"):      0.4,
    ("Open Collector",   "PP/LD Universal"):     0.4,
    # Sin/Cos <-> other (different signal type, rarely interchangeable)
    ("Sin/Cos",          "TTL RS422"):           0.1,
    ("TTL RS422",        "Sin/Cos"):             0.1,
    ("Sin/Cos",          "Push-Pull"):           0.1,
    ("Push-Pull",        "Sin/Cos"):             0.1,
}

_FLANGE_SIMILARITY = {
    ("servo",      "clamping"):    0.7,
    ("clamping",   "servo"):       0.7,
    ("servo",      "face_mount"):  0.8,
    ("face_mount", "servo"):       0.8,
    ("servo",      "synchro"):     0.7,
    ("synchro",    "servo"):       0.7,
    ("clamping",   "synchro"):     0.6,
    ("synchro",    "clamping"):    0.6,
}

_CONN_SIMILARITY = {
    ("M12",   "M23"):     0.7,
    ("M23",   "M12"):     0.7,
    ("M12",   "M8"):      0.6,
    ("M8",    "M12"):     0.6,
    ("M12",   "cable"):   0.4,
    ("cable", "M12"):     0.4,
    ("M23",   "cable"):   0.4,
    ("cable", "M23"):     0.4,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _sf(v) -> Optional[float]:
    return safe_float(v)

def _ss(v) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    return None if s.lower() in ("nan","none","") else s

def _circuit(v) -> Optional[str]:
    raw = _ss(v)
    if not raw: return None
    return OUTPUT_CIRCUIT_CANONICAL.get(raw, raw)

def _voltage_class(circuit: str) -> Optional[str]:
    if circuit is None: return None
    return OUTPUT_VOLTAGE_CLASS.get(str(circuit).strip())

# ---------------------------------------------------------------------------
# Tier 1: Hard stops
# ---------------------------------------------------------------------------
def _check_hard_stops(source: dict, candidate: dict) -> Optional[str]:
    """Returns reason string if a hard stop fires, else None."""
    s_shaft = _ss(source.get("shaft_type"))
    c_shaft = _ss(candidate.get("shaft_type"))

    # 1. Shaft type mismatch
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

    # 3. Output voltage class cross (TTL low-class <-> HTL high-class)
    s_oc = _circuit(source.get("output_circuit_canonical"))
    c_oc = _circuit(candidate.get("output_circuit_canonical"))
    if s_oc and c_oc:
        s_cls = _voltage_class(s_oc)
        c_cls = _voltage_class(c_oc)
        if (s_cls and c_cls
                and s_cls != "universal" and c_cls != "universal"
                and s_cls != "analog"    and c_cls != "analog"
                and s_cls != c_cls):
            return (f"Voltage class mismatch: source={s_oc} ({s_cls}), "
                    f"candidate={c_oc} ({c_cls})")
    return None

# ---------------------------------------------------------------------------
# Field-level similarity functions  (v8 exact implementations)
# ---------------------------------------------------------------------------
def _sim_ppr(source: dict, candidate: dict) -> tuple:
    return _ppr_score_fn(
        source_ppr  = _sf(source.get("resolution_ppr")),
        source_prog = bool(source.get("is_programmable")),
        source_min  = _sf(source.get("ppr_range_min")),
        source_max  = _sf(source.get("ppr_range_max")),
        cand_ppr    = _sf(candidate.get("resolution_ppr")),
        cand_prog   = bool(candidate.get("is_programmable")),
        cand_min    = _sf(candidate.get("ppr_range_min")),
        cand_max    = _sf(candidate.get("ppr_range_max")),
    )

def _sim_output_circuit(s_oc, c_oc) -> Optional[float]:
    if s_oc is None or c_oc is None: return None
    s_oc, c_oc = str(s_oc).strip(), str(c_oc).strip()
    if s_oc.lower() == c_oc.lower(): return 1.0
    return _OC_SIMILARITY.get((s_oc, c_oc), 0.0)

def _sim_housing(s_hd, c_hd) -> tuple:
    """Returns (score 0-1, cap_fired bool). Gaussian decay; cap fires at >30%."""
    s_hd, c_hd = _sf(s_hd), _sf(c_hd)
    if s_hd is None or c_hd is None: return None, False
    denom = max(s_hd, c_hd, 1e-9)
    rel_diff = abs(s_hd - c_hd) / denom
    cap_fired = rel_diff > 0.30
    score = max(0.0, 1.0 - rel_diff * 2.0)
    return score, cap_fired

def _sim_shaft_dia(s_dia, c_dia) -> Optional[float]:
    """±0.5mm tolerance, smooth decay beyond."""
    s_dia, c_dia = _sf(s_dia), _sf(c_dia)
    if s_dia is None or c_dia is None: return None
    diff = abs(s_dia - c_dia)
    if diff <= 0.5: return 1.0
    return max(0.0, 1.0 - (diff - 0.5) / max(s_dia, 1.0))

def _sim_voltage(s_min, s_max, c_min, c_max) -> Optional[float]:
    """Directional: candidate range must contain source range."""
    s_min, s_max = _sf(s_min), _sf(s_max)
    c_min, c_max = _sf(c_min), _sf(c_max)
    if None in (s_min, s_max, c_min, c_max): return None
    if s_max <= s_min or s_max < 3.0: return None
    span = max(s_max - s_min, 1.0)
    sim_lo = 1.0 if c_min <= s_min else max(0.0, 1.0 - (c_min - s_min) / span)
    sim_hi = 1.0 if c_max >= s_max else max(0.0, 1.0 - (s_max - c_max) / span)
    return (sim_lo + sim_hi) / 2.0

def _sim_ip(s_ip, c_ip) -> Optional[float]:
    """Directional: higher IP always acceptable. -1 rank = 0.5, -2+ = 0.0."""
    s_ip_f = _sf(s_ip)
    c_ip_f = _sf(c_ip)
    if s_ip_f is None or c_ip_f is None: return None
    s_rank = IP_HIERARCHY.get(int(s_ip_f), 0)
    c_rank = IP_HIERARCHY.get(int(c_ip_f), 0)
    delta = c_rank - s_rank
    if delta >= 0: return 1.0
    if delta == -1: return 0.5
    return 0.0

def _sim_temp(s_min, s_max, c_min, c_max) -> Optional[float]:
    """Hard boundary: c_max < s_max - 10 -> 0.0. Proportional decay otherwise."""
    s_min, s_max = _sf(s_min), _sf(s_max)
    c_min, c_max = _sf(c_min), _sf(c_max)
    if None in (s_min, s_max, c_min, c_max): return None
    if c_max < s_max - 10.0: return 0.0
    span = max(s_max - s_min, 1.0)
    sim_lo = 1.0 if c_min <= s_min else max(0.0, 1.0 - (c_min - s_min) / span * 2)
    sim_hi = 1.0 if c_max >= s_max else max(0.0, 1.0 - (s_max - c_max) / span * 2)
    return (sim_lo + sim_hi) / 2.0

def _sim_speed(s_peak, s_cont, c_peak) -> tuple:
    """Returns (score 0-1, cap_fired bool). Directional."""
    c_peak = _sf(c_peak)
    s_cont = _sf(s_cont) or _sf(s_peak)
    s_peak = _sf(s_peak)
    if c_peak is None or s_peak is None: return None, False
    cap_fired = bool(s_cont and c_peak < s_cont * 0.9)
    if c_peak >= s_peak: return 1.0, cap_fired
    return max(0.0, c_peak / s_peak), cap_fired

def _sim_channels(s_ch, c_ch) -> Optional[float]:
    if s_ch is None or c_ch is None: return None
    s_ch, c_ch = str(s_ch), str(c_ch)
    if s_ch == c_ch: return 1.0
    if s_ch in ("AB","A") and "ABN" in c_ch: return 0.8   # extra Z OK
    if "ABN" in s_ch and c_ch in ("AB","A"):  return 0.5   # Z lost
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

# ---------------------------------------------------------------------------
# Core scorer
# ---------------------------------------------------------------------------
def score_pair(source: dict, candidate: dict, weights: dict = None,
               verbose: bool = False) -> tuple:
    """
    Score source encoder vs candidate encoder.
    Returns (score 0-1, details dict)  [always; details is empty when verbose=False]
    Hard stops return (0.0, {_hard_stop: {...}}).

    Active weight normalisation: only fields where BOTH values are non-null count.
    Missing data never deflates the score.
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS
    # Auto-normalise (handles 1-10 integer inputs from UI sliders)
    _wsum = sum(weights.values()) or 1.0
    w = {k: v / _wsum for k, v in weights.items()}

    details = {}

    # -- Tier 1 hard stops ----------------------------------------------------
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
                details[key] = {"sim": round(sim, 4), "weight": wt, "label": label}

    # -- PPR ------------------------------------------------------------------
    sim_ppr, label_ppr = _sim_ppr(source, candidate)
    _accumulate("resolution_ppr", sim_ppr, label_ppr)

    # -- Output circuit -------------------------------------------------------
    s_oc = _circuit(source.get("output_circuit_canonical"))
    c_oc = _circuit(candidate.get("output_circuit_canonical"))
    sim_oc = _sim_output_circuit(s_oc, c_oc)
    label_oc = ("Exact" if sim_oc == 1.0 else
                f"~{int((sim_oc or 0)*100)}% compatible" if sim_oc else "Incompatible")
    _accumulate("output_circuit_canonical", sim_oc, label_oc)
    if verbose and s_oc:
        details.setdefault("output_circuit_canonical", {})
        details["output_circuit_canonical"].update({"source": s_oc, "candidate": c_oc})

    # -- Housing diameter -----------------------------------------------------
    sim_hd, cap_housing = _sim_housing(
        source.get("housing_diameter_mm"), candidate.get("housing_diameter_mm"))
    label_hd = ("Adapter may be needed" if cap_housing
                 else "Compatible" if sim_hd and sim_hd >= 0.9 else
                 f"~{int((sim_hd or 0)*100)}% compatible")
    _accumulate("housing_diameter_mm", sim_hd, label_hd)
    if verbose:
        details.setdefault("housing_diameter_mm", {})
        details["housing_diameter_mm"].update({
            "source": source.get("housing_diameter_mm"),
            "candidate": candidate.get("housing_diameter_mm"),
        })

    # -- Shaft diameter -------------------------------------------------------
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

    # -- Supply voltage -------------------------------------------------------
    sim_sv = _sim_voltage(
        source.get("supply_voltage_min_v"), source.get("supply_voltage_max_v"),
        candidate.get("supply_voltage_min_v"), candidate.get("supply_voltage_max_v"))
    label_sv = ("Compatible" if sim_sv and sim_sv >= 0.9
                else f"~{int((sim_sv or 0)*100)}% overlap")
    # Combined weight for min_v + max_v
    w_sv = (w.get("supply_voltage_min_v", 0) + w.get("supply_voltage_max_v", 0))
    if w_sv > 0 and sim_sv is not None:
        weighted_sum  += w_sv * sim_sv
        active_weight += w_sv
    if verbose:
        details["_voltage_range"] = {
            "sim": round(sim_sv, 4) if sim_sv is not None else None,
            "weight": w_sv,
            "label": label_sv,
            "source": f"{source.get('supply_voltage_min_v')}–{source.get('supply_voltage_max_v')} V",
            "candidate": f"{candidate.get('supply_voltage_min_v')}–{candidate.get('supply_voltage_max_v')} V",
        }

    # -- IP rating ------------------------------------------------------------
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

    # -- Temperature ----------------------------------------------------------
    sim_temp = _sim_temp(
        source.get("operating_temp_min_c"), source.get("operating_temp_max_c"),
        candidate.get("operating_temp_min_c"), candidate.get("operating_temp_max_c"))
    label_temp = ("Covers required range" if sim_temp and sim_temp >= 0.9
                  else f"~{int((sim_temp or 0)*100)}% coverage" if sim_temp else "Below minimum")
    w_temp = (w.get("operating_temp_min_c", 0) + w.get("operating_temp_max_c", 0))
    if w_temp > 0 and sim_temp is not None:
        weighted_sum  += w_temp * sim_temp
        active_weight += w_temp
    if verbose:
        details["_temp_range"] = {
            "sim": round(sim_temp, 4) if sim_temp is not None else None,
            "weight": w_temp,
            "label": label_temp,
            "source": f"{source.get('operating_temp_min_c')}–{source.get('operating_temp_max_c')} C",
            "candidate": f"{candidate.get('operating_temp_min_c')}–{candidate.get('operating_temp_max_c')} C",
        }

    # -- Speed ----------------------------------------------------------------
    sim_sp, cap_speed = _sim_speed(
        source.get("max_speed_rpm_peak"), source.get("max_speed_rpm_cont"),
        candidate.get("max_speed_rpm_peak"))
    label_sp = ("Adequate" if sim_sp == 1.0
                else "Below required" if cap_speed else f"~{int((sim_sp or 0)*100)}% of required")
    w_sp = (w.get("max_speed_rpm_peak", 0) + w.get("max_speed_rpm_cont", 0))
    if w_sp > 0 and sim_sp is not None:
        weighted_sum  += w_sp * sim_sp
        active_weight += w_sp
    if verbose:
        details["_speed"] = {
            "sim": round(sim_sp, 4) if sim_sp is not None else None,
            "weight": w_sp,
            "label": label_sp,
            "source_peak": source.get("max_speed_rpm_peak"),
            "candidate_peak": candidate.get("max_speed_rpm_peak"),
        }

    # -- Output channels ------------------------------------------------------
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

    # -- Flange type ----------------------------------------------------------
    sim_ft = _sim_flange(source.get("flange_type"), candidate.get("flange_type"))
    _accumulate("flange_type", sim_ft,
                "Match" if sim_ft == 1.0 else "Adapter may be needed")
    if verbose:
        details.setdefault("flange_type", {})
        details["flange_type"].update({
            "source": source.get("flange_type"),
            "candidate": candidate.get("flange_type"),
        })

    # -- Connection type ------------------------------------------------------
    sim_ct = _sim_connection(source.get("connection_type"), candidate.get("connection_type"))
    _accumulate("connection_type", sim_ct,
                "Match" if sim_ct == 1.0 else "Adapter available")
    if verbose:
        details.setdefault("connection_type", {})
        details["connection_type"].update({
            "source": source.get("connection_type"),
            "candidate": candidate.get("connection_type"),
        })

    # -- Connector pins -------------------------------------------------------
    sim_cp = _sim_pins(source.get("connector_pins"), candidate.get("connector_pins"))
    _accumulate("connector_pins", sim_cp,
                "Match" if sim_cp and sim_cp >= 1.0 else f"{int((sim_cp or 0)*100)}% pin compatible")
    if verbose:
        details.setdefault("connector_pins", {})
        details["connector_pins"].update({
            "source": source.get("connector_pins"),
            "candidate": candidate.get("connector_pins"),
        })

    # -- Active weight normalisation ------------------------------------------
    if active_weight == 0:
        return (0.0, details)

    total = weighted_sum / active_weight   # score in [0, 1]

    # -- Apply caps -----------------------------------------------------------
    if cap_housing:
        total = min(total, 0.75)
        details["_cap_housing"] = {
            "reason": "Housing diameter differs >30% — score capped at 0.75",
            "cap": 0.75,
        }
    if cap_speed:
        total = min(total, 0.70)
        details["_cap_speed"] = {
            "reason": "Candidate max speed < 90% of source — score capped at 0.70",
            "cap": 0.70,
        }

    return (round(total, 4), details)

# ---------------------------------------------------------------------------
# Explanation card  (v8 format for UI)
# ---------------------------------------------------------------------------
# (label, tier)  tier: 1=hard stop, 2=near-hard, 3=soft
_FIELD_META = {
    "resolution_ppr":           ("Resolution (PPR)",        2),
    "output_circuit_canonical": ("Output Circuit",          2),
    "housing_diameter_mm":      ("Housing Diameter",        2),
    "shaft_diameter_mm":        ("Shaft Diameter",          2),
    "_voltage_range":           ("Supply Voltage",          3),
    "ip_rating":                ("IP Rating",               3),
    "_temp_range":              ("Operating Temperature",   3),
    "_speed":                   ("Max Speed",               3),
    "num_output_channels":      ("Output Channels",         3),
    "flange_type":              ("Flange Type",             3),
    "connection_type":          ("Connection Type",         3),
    "connector_pins":           ("Connector Pins",          3),
}

def generate_explanation(source: dict, candidate: dict, weights: dict = None) -> dict:
    score, details = score_pair(source, candidate, weights, verbose=True)

    tier = ("strong"   if score >= 0.90 else
            "good"     if score >= 0.80 else
            "moderate" if score >= 0.50 else
            "weak"     if score > 0.0  else
            "no_match")

    hard_stop = details.get("_hard_stop", {}).get("reason")

    fields = []
    for key, (label, _tier) in _FIELD_META.items():
        if key not in details:
            continue
        d   = details[key]
        sim = d.get("sim")
        wt  = d.get("weight", 0)
        pts = round(wt * (sim or 0) * 100, 1)
        src_val = d.get("source") or d.get("source_peak") or "–"
        cnd_val = d.get("candidate") or d.get("candidate_peak") or "–"
        fields.append({
            "label":      label,
            "source_val": str(src_val),
            "cand_val":   str(cnd_val),
            "sim":        sim,
            "pts":        pts,
            "note":       d.get("label", ""),
            "tier":       3,   # All scored fields visible in breakdown
            "weight":     wt,
        })

    # Informational (not scored)
    for field, label, unit in [
        ("weight_g",           "Weight",       "g"),
        ("startup_torque_ncm", "Startup Torque","Ncm"),
        ("mttfd_years",        "MTTFd",        "years"),
        ("bearing_life_rev",   "Bearing Life", ""),
    ]:
        sv = source.get(field)
        cv = candidate.get(field)
        if sv or cv:
            fields.append({
                "label":      f"i {label}",
                "source_val": f"{sv} {unit}".strip() if sv else "–",
                "cand_val":   f"{cv} {unit}".strip() if cv else "–",
                "sim":        None, "pts": None, "note": "Informational",
                "tier": 0, "weight": 0,
            })

    caps = []
    if "_cap_housing" in details: caps.append(details["_cap_housing"]["reason"])
    if "_cap_speed"   in details: caps.append(details["_cap_speed"]["reason"])

    ppr_d  = details.get("resolution_ppr", {})
    ppr_badge = ppr_d.get("label","") if "Configurable" in ppr_d.get("label","") else None

    src_pn  = source.get("part_number")    or "Source"
    cnd_pn  = candidate.get("part_number") or "Candidate"
    cnd_mfr = candidate.get("manufacturer","")

    if hard_stop:
        summary = f"HARD STOP: {hard_stop}"
    elif tier == "strong":
        summary = (f"Strong match ({score*100:.0f}%). "
                   f"{cnd_mfr} {cnd_pn} is a direct replacement candidate.")
    elif tier == "good":
        summary = f"Good match ({score*100:.0f}%). Minor differences — review before ordering."
    elif tier == "moderate":
        summary = f"Moderate match ({score*100:.0f}%). Engineering review recommended."
    else:
        summary = f"Weak match ({score*100:.0f}%). No close equivalent found."

    return {
        "score":     score,
        "score_pct": f"{score*100:.2f}%",
        "tier":      tier,
        "hard_stop": hard_stop,
        "fields":    fields,
        "caps":      caps,
        "summary":   summary,
        "ppr_badge": ppr_badge,
    }

# ---------------------------------------------------------------------------
# Pre-filter  (v8 pattern — narrows pool before full scoring)
# ---------------------------------------------------------------------------
def _prefilter(pool_df: pd.DataFrame, source: dict) -> pd.DataFrame:
    df = pool_df.copy()

    s_shaft = _ss(source.get("shaft_type"))
    if s_shaft:
        s_hollow = "hollow" in s_shaft.lower()
        def _shaft_ok(x):
            xs = _ss(x)
            if not xs: return True
            return ("hollow" in xs.lower()) == s_hollow
        mask = df["shaft_type"].apply(_shaft_ok)
        filtered = df[mask]
        if len(filtered) > 0:
            df = filtered

    s_hd = _sf(source.get("housing_diameter_mm"))
    if s_hd and len(df) > 1000:
        def _hd_ok(x):
            c_hd = _sf(x)
            if c_hd is None: return True
            return abs(c_hd - s_hd) / max(s_hd, c_hd, 1e-9) <= 0.50
        mask = df["housing_diameter_mm"].apply(_hd_ok)
        filtered = df[mask]
        if len(filtered) > 0:
            df = filtered

    return df

# ---------------------------------------------------------------------------
# find_matches  (extended v9 signature)
# ---------------------------------------------------------------------------
def find_matches(source_row: dict, pool_df: pd.DataFrame,
                 target_mfr: str = None,
                 top_n: int = 5,
                 min_score: float = 0.0,
                 allow_shaft_relaxation: bool = True,
                 weights: dict = None) -> pd.DataFrame:
    """
    Score source_row against pool_df (optionally filtered to target_mfr).
    Returns DataFrame with columns: part_number, manufacturer, product_family, match_score (0-1).
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS

    pool = pool_df.copy()
    if target_mfr:
        pool = pool[pool["manufacturer"].str.lower() == target_mfr.strip().lower()]

    pool = _prefilter(pool, source_row)

    scores = []
    for _, row in pool.iterrows():
        cand = row.to_dict()
        score, _ = score_pair(source_row, cand, weights, verbose=False)
        if score > min_score:
            scores.append({
                "part_number":    cand.get("part_number"),
                "manufacturer":   cand.get("manufacturer"),
                "product_family": cand.get("product_family"),
                "match_score":    score,
            })

    if not scores:
        return pd.DataFrame()

    df = pd.DataFrame(scores).sort_values("match_score", ascending=False)
    df = df.drop_duplicates(subset=["part_number","manufacturer"])
    return df.head(top_n)

# ---------------------------------------------------------------------------
# find_matches_with_status  (UI entry point)
# ---------------------------------------------------------------------------
def find_matches_with_status(source_pn: str, df: pd.DataFrame,
                             target_manufacturer: str = None,
                             source_manufacturer: str = None,
                             top_n: int = 5,
                             weights: dict = None,
                             blocklist: list = None,
                             booklist: list = None) -> tuple:
    """
    Full pipeline for the Streamlit UI.
    Returns (results_df, status_dict, explanations_list).
    results_df: match_score is 0-1.
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS
    if blocklist is None:
        blocklist = []
    if booklist is None:
        booklist = []

    # Look up source
    mask = df["part_number"].str.upper() == source_pn.strip().upper()
    if source_manufacturer:
        m2 = mask & (df["manufacturer"].str.lower() == source_manufacturer.strip().lower())
        if m2.any():
            mask = m2
    if not mask.any():
        return (pd.DataFrame(),
                {"tier":"not_found","top_score":0.0,
                 "message":f"Part number '{source_pn}' not found.","fallback":False},
                [])

    source_record = df[mask].iloc[0].to_dict()
    src_mfr       = source_record.get("manufacturer","")

    # Build pool (exclude source manufacturer)
    pool = df[df["manufacturer"] != src_mfr].copy()
    if target_manufacturer:
        pool = pool[pool["manufacturer"] == target_manufacturer].copy()

    # Score
    results = find_matches(source_record, pool, top_n=top_n * 3, weights=weights)

    # Fallback: relax shaft type
    fallback = False
    if results.empty or (len(results) > 0 and results["match_score"].max() == 0):
        source_relaxed = {**source_record, "shaft_type": None}
        pool_relaxed   = pool.copy(); pool_relaxed["shaft_type"] = None
        results  = find_matches(source_relaxed, pool_relaxed, top_n=top_n * 3, weights=weights)
        fallback = True

    if results.empty:
        return (pd.DataFrame(),
                {"tier":"weak","top_score":0.0,
                 "message":"No compatible matches found.","fallback":fallback},
                [])

    # Apply blocklist — remove blocked (query_pn, match_pn) pairs
    blocked_pns = {x["match_pn"] for x in blocklist if x.get("query_pn") == source_pn}
    if blocked_pns:
        results = results[~results["part_number"].isin(blocked_pns)]

    # Apply booklist — move boosted matches to top
    boosted_pns = {x["match_pn"] for x in booklist if x.get("query_pn") == source_pn}
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
    tier = ("strong" if top_score >= 0.90 else
            "good"   if top_score >= 0.80 else "weak")
    msg = {
        "strong": "Strong match — direct replacement candidate.",
        "good":   "Good match — minor differences, review before ordering.",
        "weak":   "Weak match — no close equivalent found.",
    }[tier]

    # Build explanations
    explanations = []
    for _, res_row in results.iterrows():
        cand_mask = ((df["part_number"] == res_row["part_number"]) &
                     (df["manufacturer"] == res_row["manufacturer"]))
        if cand_mask.any():
            cand_record = df[cand_mask].iloc[0].to_dict()
            explanations.append(generate_explanation(source_record, cand_record, weights))

    status = {
        "tier":      tier,
        "message":   msg,
        "fallback":  fallback,
        "top_score": top_score,
        "ppr_badge": next((e.get("ppr_badge") for e in explanations
                           if e.get("ppr_badge")), None),
    }

    return results, status, explanations
