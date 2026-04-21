# -*- coding: utf-8 -*-
"""
normalizer.py - Parsing and normalisation helpers for encoder_crossref_v9.

All functions are pure: they accept a raw string/value and return a typed result
or None if parsing fails.
"""
import re
import math
from typing import Optional, Tuple


# ?? Generic safe helpers ???????????????????????????????????????????????????????

def safe_float(v) -> Optional[float]:
    """Convert v to float, return None on failure."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def first_float(s: str) -> Optional[float]:
    """Extract the first valid decimal number from a string."""
    if not s or str(s).strip() in ("", "nan", "None"):
        return None
    nums = re.findall(r"\d+(?:\.\d+)?", str(s))
    for n in nums:
        try:
            return float(n)
        except ValueError:
            continue
    return None


# ?? Voltage parsing ????????????????????????????????????????????????????????????

def parse_voltage_range(s) -> Tuple[Optional[float], Optional[float]]:
    """
    Parse supply voltage string to (min_v, max_v).
    Handles: "5-30V", "10...30 VDC", "5V", "4.5 to 5.5 VDC", "24 VDC"
    """
    if s is None or str(s).strip() in ("", "nan", "None"):
        return None, None
    s = str(s).strip()
    # range patterns: "5-30", "10...30", "4.5 to 5.5"
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:-|\.{2,3}|to)\s*(\d+(?:\.\d+)?)", s, re.I)
    if m:
        return float(m.group(1)), float(m.group(2))
    # single value
    m = re.search(r"\d+(?:\.\d+)?", s)
    if m:
        v = float(m.group())
        return v, v
    return None, None


# ?? Temperature parsing ????????????????????????????????????????????????????????

def parse_temp_range(s) -> Tuple[Optional[float], Optional[float]]:
    """
    Parse operating temperature string to (min_c, max_c).
    Handles many formats:
      "-40...+85 degC"       standard
      "-10 to 70"            'to' separator
      "-20/+70"              slash separator
      "-25 C...+85 C ..."    text between numbers
      "{'k': '-40 C...+85 C',...}"  Kubler dict-string (extracts first entry)
    Strategy: extract all plausible Celsius values (-100..200), return min and max.
    Fahrenheit values (>200 when context has 'F') are ignored.
    """
    if s is None or str(s).strip() in ("", "nan", "None"):
        return None, None
    s = str(s).strip()

    # If it looks like a Python dict string, extract the first quoted value
    if s.startswith("{") and ":" in s:
        m = re.search(r"'([^']{3,})'", s)
        if m:
            s = m.group(1)

    # Remove Fahrenheit segments: [...F...]
    s_clean = re.sub(r"\[[^\]]*F[^\]]*\]", " ", s, flags=re.I)
    s_clean = re.sub(r"\([^)]*F[^)]*\)", " ", s_clean, flags=re.I)

    # Extract all signed numbers in plausible Celsius range -100..200
    nums = []
    for m in re.finditer(r"[+-]?\d+(?:\.\d+)?", s_clean):
        try:
            v = float(m.group())
            if -100 <= v <= 200:
                nums.append(v)
        except ValueError:
            pass

    if len(nums) >= 2:
        return min(nums), max(nums)
    if len(nums) == 1:
        v = nums[0]
        return (v, None) if v < 0 else (None, v)
    return None, None


# ?? IP rating parsing ??????????????????????????????????????????????????????????

def parse_ip_rating(s) -> Optional[int]:
    """
    Extract numeric IP rating from string.
    "IP67", "IP 65", "IP67/IP69K", "up to IP69K" -> 67, 65, 69, 69
    """
    if s is None or str(s).strip() in ("", "nan", "None"):
        return None
    s = str(s).upper()
    # Find all IP numbers; return the highest
    matches = re.findall(r"IP\s?(\d{2,3}K?)", s)
    vals = []
    for m in matches:
        m2 = m.rstrip("K")
        try:
            vals.append(int(m2))
        except ValueError:
            pass
    return max(vals) if vals else None


# ?? Speed parsing ??????????????????????????????????????????????????????????????

def parse_speed_rpm(s) -> Optional[float]:
    """
    Extract maximum speed in RPM from string.
    "6000 rpm", "6000", "n_max = 6000 1/min"
    """
    if s is None or str(s).strip() in ("", "nan", "None"):
        return None
    try:
        f = float(str(s).strip())
        return f if 0 < f < 1_000_000 else None
    except ValueError:
        pass
    s = str(s)
    nums = re.findall(r"\d[\d.]*", s)
    for n in nums:
        # reject strings with multiple decimal points
        if n.count(".") > 1:
            continue
        try:
            f = float(n)
            if 0 < f < 1_000_000:
                return f
        except ValueError:
            continue
    return None


# ?? Frequency parsing ?????????????????????????????????????????????????????????

def parse_freq_hz(s) -> Optional[float]:
    """
    Parse output frequency to Hz.
    Handles: "100 kHz", "1000 kHz", "500000 Hz", "1 MHz"
    """
    if s is None or str(s).strip() in ("", "nan", "None"):
        return None
    s = str(s).strip()
    try:
        f = float(s)
        # If the source is already in kHz (Nidec), handled in ETL
        return f if f > 0 else None
    except ValueError:
        pass
    m = re.search(r"([\d.]+)\s*(MHz|kHz|Hz)", s, re.I)
    if m:
        val, unit = float(m.group(1)), m.group(2).upper()
        if unit == "MHZ":
            return val * 1_000_000
        elif unit == "KHZ":
            return val * 1_000
        return val
    return first_float(s)


# ?? Current / power parsing ????????????????????????????????????????????????????

def parse_current_ma(s) -> Optional[float]:
    """Parse current, return mA. Handles 'max 100mA', '140 mA', '0.14 A'."""
    if s is None or str(s).strip() in ("", "nan", "None"):
        return None
    s = str(s).strip()
    m = re.search(r"([\d.]+)\s*A\b", s, re.I)
    if m:
        return float(m.group(1)) * 1000  # A -> mA
    return first_float(s)


def parse_power_mw(s) -> Optional[float]:
    """Parse power consumption string to mW."""
    if s is None or str(s).strip() in ("", "nan", "None"):
        return None
    s = str(s).strip()
    m = re.search(r"([\d.]+)\s*W\b", s, re.I)
    if m:
        return float(m.group(1)) * 1000
    m = re.search(r"([\d.]+)\s*mW\b", s, re.I)
    if m:
        return float(m.group(1))
    m = re.search(r"([\d.]+)\s*mA\b", s, re.I)
    if m:
        # mA at 24V -> mW approx
        return float(m.group(1)) * 24
    return first_float(s)


# ?? Weight parsing ????????????????????????????????????????????????????????????

def parse_weight_g(s, unit_hint: str = "g") -> Optional[float]:
    """
    Parse weight to grams.
    unit_hint: 'g' or 'kg' - used when no unit suffix present in string.
    """
    if s is None or str(s).strip() in ("", "nan", "None"):
        return None
    s = str(s).strip()
    m = re.search(r"([\d.]+)\s*kg\b", s, re.I)
    if m:
        return float(m.group(1)) * 1000
    m = re.search(r"([\d.]+)\s*g\b", s, re.I)
    if m:
        return float(m.group(1))
    v = first_float(s)
    if v is None:
        return None
    return v * 1000 if unit_hint == "kg" else v


# ?? Torque / shaft load parsing ???????????????????????????????????????????????

def parse_torque_ncm(s) -> Optional[float]:
    """Parse starting torque to Ncm. Handles Nm, Ncm, mNm."""
    if s is None or str(s).strip() in ("", "nan", "None"):
        return None
    s = str(s).strip()
    m = re.search(r"([\d.]+)\s*Nm\b", s, re.I)
    if m:
        return float(m.group(1)) * 100  # Nm -> Ncm
    m = re.search(r"([\d.]+)\s*mNm\b", s, re.I)
    if m:
        return float(m.group(1)) / 10  # mNm -> Ncm
    m = re.search(r"([\d.]+)\s*Ncm\b", s, re.I)
    if m:
        return float(m.group(1))
    return first_float(s)


# ?? PPR / connector pin parsing ???????????????????????????????????????????????

def parse_ppr(s) -> Optional[float]:
    """Extract integer PPR value."""
    v = first_float(str(s))
    return v if v and v > 0 else None


def parse_pins(s) -> Optional[int]:
    """Extract connector pin count from connection type string."""
    if s is None:
        return None
    m = re.search(r"(\d+)\s*(?:pin|pol|p\b)", str(s), re.I)
    if m:
        return int(m.group(1))
    return None


# ?? Boolean coercion ??????????????????????????????????????????????????????????

def to_bool(v) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "yes", "1", "x", "yes (built-in)"):
        return True
    if s in ("false", "no", "0", "", "nan", "none"):
        return False
    return None
