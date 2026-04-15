"""
streamlit_app.py  v12
=====================
Kubler AI Cross-Reference Engine  —  AQB Solutions
Kubler-centric: target is always Kubler. KUB-prefixed login required.
"""

import os, sys, re, math, html, json, textwrap, csv, datetime
import pandas as pd
import streamlit as st
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from schema import (
    UNIFIED_SCHEMA, CANONICAL_COLUMNS, COLUMN_MAP,
    OUTPUT_CIRCUIT_CANONICAL, OUTPUT_VOLTAGE_CLASS,
)
from matcher import find_matches_with_status, generate_explanation, DEFAULT_WEIGHTS

# ── Weight helpers ────────────────────────────────────────────────────────────
_max_dw = max(DEFAULT_WEIGHTS.values()) if DEFAULT_WEIGHTS else 1
# Build default ints using schema T2/T3 keys (composite)
_SCHEMA_T23_KEYS = ["resolution_ppr","output_circuit_canonical","housing_diameter_mm",
                    "shaft_diameter_mm","ip_rating","operating_temp","supply_voltage",
                    "max_speed_rpm_peak","connection_type"]
# Map matcher weights back to schema composite keys
_COMPOSITE_DEFAULTS = {
    "operating_temp":   (DEFAULT_WEIGHTS.get("operating_temp_max_c",0.03) +
                         DEFAULT_WEIGHTS.get("operating_temp_min_c",0.03)),
    "supply_voltage":   (DEFAULT_WEIGHTS.get("supply_voltage_max_v",0.04) +
                         DEFAULT_WEIGHTS.get("supply_voltage_min_v",0.04)),
    "max_speed_rpm_peak": (DEFAULT_WEIGHTS.get("max_speed_rpm_peak",0.04) +
                           DEFAULT_WEIGHTS.get("max_speed_rpm_cont",0.02)),
}
_SCHEMA_WEIGHTS = {**{k:v for k,v in DEFAULT_WEIGHTS.items()
                       if k in ["resolution_ppr","output_circuit_canonical",
                                 "housing_diameter_mm","shaft_diameter_mm",
                                 "ip_rating","connection_type"]},
                   **_COMPOSITE_DEFAULTS}
DEFAULT_WEIGHT_INTS = {f: max(1, round(v/_max_dw*10)) for f,v in _SCHEMA_WEIGHTS.items()}

# Schema keys → matcher keys (composite → individual)
_SCHEMA_TO_MATCHER = {
    "operating_temp":   ["operating_temp_min_c","operating_temp_max_c"],
    "supply_voltage":   ["supply_voltage_min_v","supply_voltage_max_v"],
    "max_speed_rpm_peak": ["max_speed_rpm_peak","max_speed_rpm_cont"],
}

def _ints_to_normalized(ints):
    """Expand composite schema keys → individual matcher keys, then normalise."""
    expanded = {}
    for f, v in ints.items():
        targets = _SCHEMA_TO_MATCHER.get(f)
        if targets:
            for t in targets:
                expanded[t] = v   # same integer weight to each sub-key
        else:
            expanded[f] = v
    # Add any matcher keys not in schema (with default value 1)
    for k in DEFAULT_WEIGHTS:
        if k not in expanded:
            expanded[k] = 1
    total = sum(expanded.values())
    return {f: v/total for f,v in expanded.items()} if total else dict(DEFAULT_WEIGHTS)

def _normalize_ints(ints):
    mx = max(ints.values()) if ints else 1
    return {f: max(1, round(v/mx*10)) for f,v in ints.items()}

# ── API config ────────────────────────────────────────────────────────────────
def _load_api_config():
    api_key = ""; model = "claude-sonnet-4-20250514"
    _here = os.path.dirname(os.path.abspath(__file__))
    for _d in [_here, os.path.dirname(_here), os.getcwd()]:
        _cfg = os.path.join(_d, "config_claude.py")
        if os.path.exists(_cfg):
            try:
                import importlib.util
                spec = importlib.util.spec_from_file_location("config_claude", _cfg)
                cfg = importlib.util.module_from_spec(spec); spec.loader.exec_module(cfg)
                api_key = getattr(cfg,"CLAUDE_API_KEY","") or ""
                model   = getattr(cfg,"MODEL",model) or model
                if api_key: break
            except: pass
    if not api_key: api_key = os.environ.get("ANTHROPIC_API_KEY","") or os.environ.get("CLAUDE_API_KEY","")
    if not api_key:
        try: api_key = st.secrets.get("ANTHROPIC_API_KEY","") or st.secrets.get("CLAUDE_API_KEY","")
        except: pass
    return api_key, model

_API_KEY, _MODEL = _load_api_config()

# ── Analytics logger ──────────────────────────────────────────────────────────
_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "user_analytics.csv")
_LOG_FIELDS = [
    "timestamp","user_id","action","session_id",
    # Search context
    "part_number","detected_mfr",
    # Results summary
    "match_tier","num_results","top_match","top_score","all_match_pns","all_scores",
    # Weight config snapshot (JSON) — only on search events
    "weights_json",
    # Performance
    "query_duration_ms",
    # UI engagement flags
    "ai_tab_viewed","csv_downloaded",
]

def _log_event(user_id, action, part_number="", detected_mfr="", top_match="", top_score="",
               match_tier="", num_results="", all_match_pns="", all_scores="",
               weights_json="", query_duration_ms="", ai_tab_viewed="", csv_downloaded=""):
    try:
        exists = os.path.exists(_LOG_PATH)
        with open(_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_LOG_FIELDS)
            if not exists: writer.writeheader()
            writer.writerow({
                "timestamp":         datetime.datetime.utcnow().isoformat(),
                "user_id":           user_id,
                "action":            action,
                "session_id":        st.session_state.get("session_id",""),
                "part_number":       part_number,
                "detected_mfr":      detected_mfr,
                "match_tier":        match_tier,
                "num_results":       num_results,
                "top_match":         top_match,
                "top_score":         top_score,
                "all_match_pns":     all_match_pns,
                "all_scores":        all_scores,
                "weights_json":      weights_json,
                "query_duration_ms": query_duration_ms,
                "ai_tab_viewed":     ai_tab_viewed,
                "csv_downloaded":    csv_downloaded,
            })
    except Exception: pass

# ── Feedback override system ──────────────────────────────────────────────────
_FEEDBACK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feedback_overrides.json")

def _load_feedback():
    try:
        if os.path.exists(_FEEDBACK_PATH):
            with open(_FEEDBACK_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("blocklist", []), data.get("booklist", [])
    except Exception:
        pass
    return [], []

def _save_feedback(blocklist, booklist):
    try:
        with open(_FEEDBACK_PATH, "w", encoding="utf-8") as f:
            json.dump({"blocklist": blocklist, "booklist": booklist}, f, indent=2)
    except Exception as e:
        pass

def _add_feedback(query_pn, match_pn, match_mfr, action, user_id, reason=""):
    """action: 'block' or 'boost'"""
    bl, bo = _load_feedback()
    entry = {
        "query_pn": query_pn, "match_pn": match_pn, "match_mfr": match_mfr,
        "user": user_id, "reason": reason,
        "ts": datetime.datetime.utcnow().isoformat()
    }
    if action == "block":
        # Remove from booklist if previously boosted
        bo = [x for x in bo if not (x["query_pn"]==query_pn and x["match_pn"]==match_pn)]
        # Add to blocklist if not already there
        if not any(x["query_pn"]==query_pn and x["match_pn"]==match_pn for x in bl):
            bl.append(entry)
    elif action == "boost":
        # Remove from blocklist if previously blocked
        bl = [x for x in bl if not (x["query_pn"]==query_pn and x["match_pn"]==match_pn)]
        # Add to booklist if not already there
        if not any(x["query_pn"]==query_pn and x["match_pn"]==match_pn for x in bo):
            bo.append(entry)
    _save_feedback(bl, bo)

def _feedback_status(query_pn, match_pn):
    """Returns 'blocked', 'boosted', or None."""
    bl, bo = _load_feedback()
    if any(x["query_pn"]==query_pn and x["match_pn"]==match_pn for x in bl):
        return "blocked"
    if any(x["query_pn"]==query_pn and x["match_pn"]==match_pn for x in bo):
        return "boosted"
    return None

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AQB Solutions | Encoder Cross-Reference Engine",
    page_icon="🔄", layout="wide", initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,600;0,9..40,800&family=DM+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;}
[data-testid="stSidebar"]{min-width:290px!important;max-width:290px!important;background:#0B2545!important;}
[data-testid="stSidebar"]>div:first-child{min-width:290px!important;padding-top:.75rem!important;}
[data-testid="stSidebar"] *{color:#fff!important;}
[data-testid="stSidebar"] label{color:#b8d4f0!important;font-size:11px!important;text-transform:uppercase;letter-spacing:.06em;}
[data-testid="stSidebar"] .stTextInput input{background:#0f3060!important;color:#fff!important;border-color:#2a5a90!important;}
[data-testid="stSidebar"] .stTextInput input::placeholder{color:#7aa0c8!important;}
[data-testid="stSidebar"] .stSlider>div>div>div{background:#1a4a80!important;}
[data-testid="stSidebar"] hr{border-color:#1a4a80!important;opacity:.5;margin:5px 0!important;}
[data-testid="stSidebar"] [data-testid="stFormSubmitButton"]:first-of-type button{
    background:#E8A020!important;color:#fff!important;font-weight:800!important;
    font-size:14px!important;border:none!important;border-radius:8px!important;
    width:100%!important;padding:.4rem 1rem!important;}
[data-testid="stSidebar"] [data-testid="stFormSubmitButton"]:last-of-type button{
    background:#1a4a80!important;color:#fff!important;font-weight:700!important;
    border:none!important;border-radius:8px!important;padding:.4rem .6rem!important;}
[data-testid="stSidebar"] [data-testid="stBaseButton-secondary"] button,
[data-testid="stSidebar"] button:not([data-testid="stFormSubmitButton"] button){
    background:#1a4a80!important;color:#fff!important;font-weight:700!important;
    border:1px solid rgba(255,255,255,.15)!important;border-radius:8px!important;
    padding:.4rem .6rem!important;}
[data-testid="stAppViewContainer"]{background:#f0f4fa;}
[data-testid="stDecoration"]{display:none!important;height:0!important;}
[data-testid="stMainBlockContainer"]{padding:3.5rem 2rem 2rem!important;}
.aqb-header{background:linear-gradient(135deg,#0B2545 0%,#1356a0 60%,#3a7fd4 100%);
    color:white;padding:14px 24px 12px;border-radius:12px;margin-bottom:18px;
    display:flex;align-items:center;gap:16px;box-shadow:0 4px 24px rgba(11,37,69,.3);}
.aqb-title{font-size:19px;font-weight:800;margin:0;line-height:1.2;letter-spacing:-.01em;}
.db-stats{display:flex;gap:6px;margin-top:7px;flex-wrap:wrap;}
.db-stat{background:rgba(255,255,255,.12);padding:3px 10px;border-radius:20px;font-size:11px;color:rgba(255,255,255,.9);}
.query-card{background:white;border-left:5px solid #1356a0;border-radius:10px;
    padding:16px 20px;margin-bottom:18px;box-shadow:0 2px 12px rgba(11,37,69,.08);}
.query-label{font-size:11px;font-weight:700;color:#1356a0;text-transform:uppercase;letter-spacing:.07em;}
.query-pn{font-size:21px;font-weight:800;color:#0B1E38;margin:4px 0 10px;font-family:'DM Mono',monospace;}
.spec-pill{display:inline-block;background:#e8f0fb;border:1px solid #c5d8f0;border-radius:20px;
    padding:3px 11px;font-size:12px;font-weight:600;color:#0B2545;margin:2px 3px;}
.tier-strong{background:#d1fae5;color:#065f46;border:1px solid #6ee7b7;}
.tier-good{background:#fef3c7;color:#92400e;border:1px solid #fcd34d;}
.tier-weak{background:#fee2e2;color:#991b1b;border:1px solid #fca5a5;}
.tier-banner{border-radius:8px;padding:6px 14px;font-size:12px;font-weight:700;display:inline-block;margin-bottom:12px;}
.match-card{background:white;border-radius:12px;margin-bottom:16px;
    box-shadow:0 2px 14px rgba(11,37,69,.09);overflow:hidden;border:1px solid #dde8f5;}
.match-head{display:flex;align-items:center;padding:15px 20px 10px;gap:14px;}
.rank-badge{width:30px;height:30px;border-radius:50%;display:flex;align-items:center;
    justify-content:center;font-size:12px;font-weight:800;flex-shrink:0;}
.rank-1{background:#fef3c7;color:#7d5a00;}.rank-2{background:#dbeafe;color:#1e3a8a;}
.rank-3{background:#fce7f3;color:#831843;}.rank-n{background:#f0f4fa;color:#3a7fd4;}
.match-pn{font-size:15px;font-weight:800;color:#0B1E38;font-family:'DM Mono',monospace;}
.match-family{font-size:11px;color:#3a7fd4;margin-top:2px;font-weight:600;}
.score-block{margin-left:auto;text-align:right;}
.score-num{font-size:28px;font-weight:900;line-height:1;}
.score-lbl{font-size:10px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;}
.score-strong{color:#059669;}.score-good{color:#d97706;}.score-weak{color:#dc2626;}
.specs-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(145px,1fr));
    gap:1px;background:#e8f0fb;border-top:1px solid #dde8f5;border-bottom:1px solid #dde8f5;}
.spec-cell{background:white;padding:9px 13px;}
.spec-k{font-size:10px;color:#3a7fd4;text-transform:uppercase;letter-spacing:.05em;font-weight:700;}
.spec-v{font-size:13px;font-weight:700;color:#0B1E38;margin-top:2px;}
.spec-na{font-size:12px;color:#c0cfe0;font-style:italic;}
.prog-badge{display:inline-block;background:#ede9fe;color:#5b21b6;border-radius:12px;
    padding:2px 8px;font-size:10px;font-weight:700;margin-top:3px;}
.nl-box{background:linear-gradient(135deg,#f0f7ff 0%,#e8f0fb 100%);border:1px solid #c5d8f0;
    border-radius:10px;padding:14px 18px;margin:12px 0;font-size:13.5px;line-height:1.65;color:#0B2545;}
.pm-table{width:100%;border-collapse:collapse;font-size:13px;}
.pm-table th{background:#0B2545;color:white;padding:9px 14px;text-align:left;font-size:11px;
    text-transform:uppercase;letter-spacing:.05em;}
.pm-table td{padding:8px 14px;border-bottom:1px solid #eef3fa;vertical-align:top;}
.pm-table tr:nth-child(even) td{background:#f7faff;}
.pm-table tr:hover td{background:#edf3fc;}
.pm-label{font-weight:700;color:#1356a0;font-size:11px;text-transform:uppercase;}
.pm-val{font-family:'DM Mono',monospace;font-size:12px;color:#0B1E38;}
.pm-na{color:#c0cfe0;font-style:italic;}
.schema-table{width:100%;border-collapse:collapse;font-size:12px;}
.schema-table th{background:#0B2545;color:white;padding:8px 12px;text-align:left;
    font-size:11px;text-transform:uppercase;letter-spacing:.05em;position:sticky;top:0;}
.schema-table td{padding:7px 12px;border-bottom:1px solid #eef3fa;vertical-align:top;}
.schema-table tr:nth-child(even) td{background:#f7faff;}
.tier-chip{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:800;text-transform:uppercase;}
.tier-chip-1{background:#fee2e2;color:#991b1b;}.tier-chip-2{background:#fef3c7;color:#92400e;}
.tier-chip-3{background:#dbeafe;color:#1e40af;}.tier-chip-0{background:#f3f4f6;color:#374151;}
.hardstop-banner{background:#fee2e2;border:1px solid #fca5a5;border-radius:8px;
    padding:10px 16px;color:#991b1b;font-weight:700;font-size:13px;margin:8px 0;}
.detect-badge{display:inline-block;background:#10b981;border:1px solid #059669;border-radius:6px;
    padding:3px 10px;font-size:11px;font-weight:700;color:#ffffff;margin-top:4px;}
.detect-warn{display:inline-block;background:#f59e0b;border:1px solid #d97706;border-radius:6px;
    padding:3px 10px;font-size:11px;font-weight:700;color:#ffffff;margin-top:4px;}
.login-box{max-width:420px;margin:80px auto;background:white;border-radius:16px;
    padding:40px;box-shadow:0 8px 40px rgba(11,37,69,.15);}
.login-title{font-size:22px;font-weight:800;color:#0B2545;margin-bottom:4px;}
.login-sub{font-size:13px;color:#6b7280;margin-bottom:24px;}
.status-dot-green{display:inline-block;width:8px;height:8px;background:#10b981;
    border-radius:50%;margin-right:6px;vertical-align:middle;}
.status-dot-red{display:inline-block;width:8px;height:8px;background:#ef4444;
    border-radius:50%;margin-right:6px;vertical-align:middle;}
.kubler-target-badge{background:rgba(255,255,255,.10);border:1px solid rgba(255,255,255,.20);border-radius:8px;
    padding:8px 12px;font-size:12px;color:#fff!important;margin-bottom:8px;}
.kubler-target-badge strong{color:#fff!important;}
.kubler-target-badge .badge-sub{color:rgba(200,220,255,.80)!important;font-size:11px;}
/* ── Admin selectbox — white box, dark text ── */
[data-testid="stSidebar"] .stSelectbox>label{
    color:#ffd580!important;font-size:11px!important;
    text-transform:uppercase!important;letter-spacing:.06em!important;font-weight:700!important;}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"]>div:first-child{
    background:#ffffff!important;border:2px solid #3a7fd4!important;border-radius:8px!important;}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] span{color:#0B1E38!important;}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] div{color:#0B1E38!important;}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] p{color:#0B1E38!important;}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] svg{fill:#1356a0!important;}
[data-baseweb="popover"] li,[data-baseweb="option"]{
    color:#0B1E38!important;background:#ffffff!important;}
[data-baseweb="popover"] li:hover{background:#e8f0fb!important;}
</style>
""", unsafe_allow_html=True)

# ── Login gate ────────────────────────────────────────────────────────────────
_VALID_USERS = {
    "KUB001":    "kubler2024",
    "KUB002":    "kubler2024",
    "KUB003":    "aqb2024",
    "AQBADMIN":  "aqbadmin2024",   # admin — any-to-any matching
}
_ADMIN_USERS = {"AQBADMIN"}
_USER_COMPANY = {"KUB": "Kubler", "AQB": "AQB Solutions"}   # prefix → company

if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
    st.session_state["user_id"] = ""
    st.session_state["company"] = ""
    st.session_state["session_id"] = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

if not st.session_state["logged_in"]:
    # Full-page branded login — branding + form in same column, no gap
    st.markdown("""
    <style>
    [data-testid="stAppViewContainer"]{
        background:linear-gradient(135deg,#0B2545 0%,#1356a0 55%,#1a6bc4 100%)!important;
        min-height:100vh;
    }
    [data-testid="stMainBlockContainer"]{
        padding-top:8vh!important;
        padding-bottom:0!important;
    }
    [data-testid="stSidebar"]{display:none!important;}
    [data-testid="stDecoration"]{display:none!important;}
    /* Card styling applied to the column block */
    div[data-testid="column"]:nth-child(2)>div:first-child{
        background:white;border-radius:20px;padding:36px 40px 32px;
        box-shadow:0 24px 80px rgba(0,0,0,.35),0 4px 20px rgba(0,0,0,.2);
    }
    .lhdr{display:flex;align-items:center;gap:14px;margin-bottom:16px;}
    .licon{width:46px;height:46px;background:linear-gradient(135deg,#0B2545,#1356a0);
        border-radius:11px;display:flex;align-items:center;justify-content:center;
        font-size:22px;flex-shrink:0;}
    .lbrand{font-size:10px;font-weight:700;color:#1356a0;text-transform:uppercase;
        letter-spacing:.10em;margin-bottom:3px;}
    .ltitle{font-size:18px;font-weight:800;color:#0B1E38;line-height:1.2;}
    .lrule{height:1px;background:#e5e9f0;margin:14px 0 12px;}
    .ltag{font-size:12px;color:#6b7280;margin-bottom:4px;line-height:1.5;}
    .lfoot{text-align:center;padding:14px 0 4px;}
    .lfoot-txt{font-size:11px;color:#9ca3af;}
    .lfoot-ai{display:inline-block;background:#f3f4f6;border-radius:20px;
        padding:3px 11px;font-size:10px;font-weight:700;color:#6b7280;margin-top:6px;}
    /* Fix input labels inside the card */
    div[data-testid="column"]:nth-child(2) label{color:#374151!important;font-size:13px!important;}
    div[data-testid="column"]:nth-child(2) input{color:#0B1E38!important;}
    div[data-testid="column"]:nth-child(2) [data-testid="stFormSubmitButton"] button{
        background:linear-gradient(135deg,#1356a0,#0B2545)!important;
        color:white!important;font-weight:700!important;border:none!important;
        border-radius:8px!important;padding:.5rem 1rem!important;margin-top:4px!important;}
    </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # Branding header — inside same column as form, no gap
        st.markdown("""
        <div class="lhdr">
          <div class="licon">🔄</div>
          <div>
            <div class="lbrand">AQB Solutions</div>
            <div class="ltitle">Encoder Cross-Reference<br>Intelligence Platform</div>
          </div>
        </div>
        <div class="lrule"></div>
        <div class="ltag">AI-powered cross-referencing — find the best replacement
          across 7 manufacturers and 700,000+ configurations instantly.</div>
        """, unsafe_allow_html=True)

        with st.form("login_form"):
            uid = st.text_input("User ID", placeholder="e.g. KUB001")
            pwd = st.text_input("Password", type="password", placeholder="••••••••")
            login_btn = st.form_submit_button("Sign In →", use_container_width=True)

        if login_btn:
            uid_clean = uid.strip().upper()
            if uid_clean in _VALID_USERS and _VALID_USERS[uid_clean] == pwd.strip():
                prefix = uid_clean[:3]
                company = _USER_COMPANY.get(prefix, "AQB Solutions")
                st.session_state["logged_in"] = True
                st.session_state["user_id"]   = uid_clean
                st.session_state["company"]   = company
                _log_event(uid_clean, "login")
                st.rerun()
            else:
                st.error("Invalid credentials. Please check your User ID and password.")

        st.markdown("""
        <div class="lfoot">
          <div class="lfoot-txt">Authorised AQB Solutions personnel only</div>
          <div class="lfoot-ai">⚡ Powered by Claude AI</div>
        </div>
        """, unsafe_allow_html=True)

    st.stop()

# ── Session init ──────────────────────────────────────────────────────────────
_USER_ID  = st.session_state["user_id"]
_COMPANY  = st.session_state["company"]
_IS_ADMIN = (_USER_ID in _ADMIN_USERS)

for k, v in [("pn_value",""),("results",None),("status",None),("exps",None),
              ("weight_ints",dict(DEFAULT_WEIGHT_INTS)),("weights",dict(DEFAULT_WEIGHTS)),
              ("admin_src_mfr","(Any competitor)"),("admin_tgt_mfr","Kubler")]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner="Loading encoder database …")
def load_unified():
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, "data", "competitor_unified.csv")
    if os.path.exists(path):
        return pd.read_csv(path, low_memory=False)
    st.error("competitor_unified.csv not found in data/. Please run make_kubler_compatible_samples.py first.")
    st.stop()

@st.cache_resource(show_spinner=False)
def _db_connected():
    """Check if CockroachDB is reachable (lightweight check)."""
    try:
        from db_load import test_connection
        return test_connection()
    except Exception:
        return False

UNIFIED_DF   = load_unified()
_DB_ONLINE   = _db_connected()
ALL_MFRS     = sorted(UNIFIED_DF["manufacturer"].dropna().unique().tolist())
COMP_MFRS    = [m for m in ALL_MFRS if m != "Kubler"]

# ── Manufacturer auto-detect patterns ────────────────────────────────────────
_MFR_PATTERNS = [
    ("Sick",        re.compile(r"^DFS|^DBS|^DGS|^DUS|^AFS|^AFM", re.I)),
    ("Wachendorff", re.compile(r"^WDG", re.I)),
    ("Lika",        re.compile(r"^C5[08]|^CB|^CK5[89]|^I5[89]|^MI5[89]|^MC5|^CX5|^IX5|^IP5|^IQ5", re.I)),
    ("Baumer",      re.compile(r"^EIL|^EBD|^EB2|^HOG|^GXP|^BDG", re.I)),
    ("EPC",         re.compile(r"^8[05]2S|^858S|^725|^755|^15[ST]|^25[TSF]|^30M|^58T|^702|^TR[123P]", re.I)),
    ("Nidec",       re.compile(r"^AV\d{1,2}", re.I)),
    ("Kubler",      re.compile(r"^K[58][08]I|^K[IS]40|^A02|^5[02][02][05]|^8\.[05]", re.I)),
]

def _detect_mfr(pn):
    pn = str(pn).strip()
    if not pn: return None
    mask = UNIFIED_DF["part_number"].astype(str).str.upper() == pn.upper()
    if mask.any():
        return UNIFIED_DF[mask].iloc[0].get("manufacturer")
    for mfr, pat in _MFR_PATTERNS:
        if pat.match(pn): return mfr
    return None

# ── Helpers ───────────────────────────────────────────────────────────────────
_BAD = {None,"nan","None","none","",float("nan")}

def _safe(v):
    if v is None: return None
    try:
        if math.isnan(float(v)): return None
    except: pass
    return v

def _fmt(v, unit="", decimals=0):
    if _safe(v) is None: return "—"
    try:
        n=float(v)
        s=f"{int(round(n)):,}" if decimals==0 else f"{n:.{decimals}f}"
        return f"{s} {unit}".strip() if unit else s
    except: return str(v)

def _fmt_voltage(r):
    lo,hi=_safe(r.get("supply_voltage_min_v")),_safe(r.get("supply_voltage_max_v"))
    if lo is None and hi is None: return None
    return f"{lo}–{hi} V DC" if lo!=hi else f"{lo} V DC"

def _fmt_temp(r):
    lo,hi=_safe(r.get("operating_temp_min_c")),_safe(r.get("operating_temp_max_c"))
    if lo is None and hi is None: return None
    return f"{lo}…{hi} °C"

def _fmt_ppr(r):
    """Format PPR — show range for programmable encoders."""
    if str(r.get("is_programmable","")).lower() in ("true","1"):
        lo=_safe(r.get("ppr_range_min")); hi=_safe(r.get("ppr_range_max"))
        if lo and hi: return f"{int(lo):,}–{int(hi):,} PPR"
        if hi: return f"1–{int(hi):,} PPR"
        return "Programmable"
    ppr=_safe(r.get("resolution_ppr"))
    return f"{int(float(ppr)):,} PPR" if ppr else "—"

def _score_color(pct):
    if pct>=90: return "score-strong","bar-high"
    if pct>=80: return "score-good","bar-mid"
    return "score-weak","bar-low"

def _enrich_results(results_df, unified_df):
    """Convert matcher DataFrame → list of enriched dicts with all schema fields."""
    if results_df is None or (hasattr(results_df,'empty') and results_df.empty):
        return []
    out = []
    for _, row in results_df.iterrows():
        score = float(row.get('match_score', 0))
        score_pct = round(score * 100, 1)
        # Get full record from unified DB
        mask = ((unified_df['part_number'].astype(str) == str(row['part_number'])) &
                (unified_df['manufacturer'].astype(str) == str(row['manufacturer'])))
        if mask.any():
            full = unified_df[mask].iloc[0].to_dict()
        else:
            full = row.to_dict()
        full['match_score'] = score
        full['score_pct']   = score_pct
        out.append(full)
    return out

def _tier_css(tier): return {"strong":"tier-strong","good":"tier-good"}.get(tier,"tier-weak")
def _tier_label(tier):
    return {"strong":"✅ Strong match","good":"⚠ Review recommended",
            "weak":"❌ Weak / no close match","no_match":"❌ Not found"}.get(tier,tier)

# ── Query card labeled grid ───────────────────────────────────────────────────
def _query_card_grid(r):
    """Labeled parameter grid for the queried encoder card."""
    cells = [
        ("Resolution",    _fmt_ppr(r),                                              r.get("resolution_ppr") or r.get("is_programmable")),
        ("Output Circuit",r.get("output_circuit_canonical"),                        r.get("output_circuit_canonical")),
        ("Housing Ø",     f"{_fmt(r.get('housing_diameter_mm'))} mm",               r.get("housing_diameter_mm")),
        ("Shaft Ø",       f"{_fmt(r.get('shaft_diameter_mm'),decimals=2)} mm",      r.get("shaft_diameter_mm")),
        ("Shaft Type",    str(r.get("shaft_type") or "").capitalize(),              r.get("shaft_type")),
        ("IP Rating",     f"IP{r.get('ip_rating')}",                                r.get("ip_rating")),
        ("Supply Voltage",_fmt_voltage(r),                                           r.get("supply_voltage_min_v")),
        ("Temp Range",    _fmt_temp(r),                                              r.get("operating_temp_min_c")),
        ("Max Speed",     f"{_fmt(r.get('max_speed_rpm_peak'))} rpm",               r.get("max_speed_rpm_peak")),
        ("Connection",    r.get("connection_type"),                                  r.get("connection_type")),
        ("Shock",         r.get("shock_resistance"),                                 r.get("shock_resistance")),
        ("Vibration",     r.get("vibration_resistance"),                             r.get("vibration_resistance")),
    ]
    out = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:8px;margin-top:12px;">'
    for label, val, chk in cells:
        is_na = _safe(chk) is None or str(val) in ("—","None","nan","")
        v_str = "—" if is_na else html.escape(str(val))
        v_col = "#9ca3af" if is_na else "#0B1E38"
        out += (f'<div style="background:#f0f4fa;border-radius:8px;padding:7px 11px;">'
                f'<div style="font-size:10px;font-weight:700;color:#3a7fd4;text-transform:uppercase;'
                f'letter-spacing:.05em;margin-bottom:2px;">{html.escape(label)}</div>'
                f'<div style="font-size:12px;font-weight:600;color:{v_col};">{v_str}</div>'
                f'</div>')
    return out + "</div>"

def _specs_grid(r):
    cells=[
        ("Resolution",  _fmt_ppr(r),                                   True),
        ("Output Circuit", r.get("output_circuit_canonical"),           r.get("output_circuit_canonical")),
        ("Housing Ø",   f"{_fmt(r.get('housing_diameter_mm'))} mm",    r.get("housing_diameter_mm")),
        ("Shaft Ø",     f"{_fmt(r.get('shaft_diameter_mm'),decimals=2)} mm", r.get("shaft_diameter_mm")),
        ("Shaft Type",  str(r.get("shaft_type") or "").capitalize(),   r.get("shaft_type")),
        ("IP Rating",   f"IP{r.get('ip_rating')}",                     r.get("ip_rating")),
        ("Supply V",    _fmt_voltage(r),                                r.get("supply_voltage_min_v")),
        ("Max Speed",   f"{_fmt(r.get('max_speed_rpm_peak'))} rpm",    r.get("max_speed_rpm_peak")),
        ("Temp Range",  _fmt_temp(r),                                   r.get("operating_temp_min_c")),
        ("Connection",  r.get("connection_type"),                       r.get("connection_type")),
        ("Shock",       r.get("shock_resistance"),                      r.get("shock_resistance")),
        ("Vibration",   r.get("vibration_resistance"),                  r.get("vibration_resistance")),
        ("Weight",      f"{_fmt(r.get('weight_g'))} g",                r.get("weight_g")),
    ]
    out='<div class="specs-grid">'
    for label,val,chk in cells:
        is_na=_safe(chk) is None or str(val) in ("—","None","nan","")
        v_str="—" if is_na else html.escape(str(val))
        v_cls="spec-na" if is_na else "spec-v"
        out+=(f'<div class="spec-cell"><div class="spec-k">{html.escape(label)}</div>'
              f'<div class="{v_cls}">{v_str}</div></div>')
    return out+"</div>"

# ── Native parameter names per manufacturer ───────────────────────────────────
# canonical_col → native field name as it appears in that manufacturer's datasheet
_NATIVE_NAMES = {
    "Kubler": {
        "resolution_ppr":             "Resolution",
        "ppr_range_min":              "Programmable range min.",
        "ppr_range_max":              "Programmable range max.",
        "is_programmable":            "Programmable",
        "output_circuit_canonical":   "Output circuit",
        "output_signals":             "Output signals",
        "num_output_channels":        "No. of output channels",
        "max_output_freq_hz":         "Max. output frequency",
        "supply_voltage_min_v":       "Supply voltage min.",
        "supply_voltage_max_v":       "Supply voltage max.",
        "output_current_ma":          "Output current",
        "power_consumption_typ_mw":   "Power consumption",
        "reverse_polarity_protection":"Reverse polarity protection",
        "short_circuit_protection":   "Short-circuit protection",
        "housing_diameter_mm":        "Housing diameter",
        "shaft_diameter_mm":          "Shaft diameter",
        "shaft_type":                 "Shaft type",
        "flange_type":                "Flange / stator coupling",
        "connection_type":            "Connection type",
        "connector_pins":             "No. of connector pins",
        "ip_rating":                  "Protection class",
        "operating_temp_min_c":       "Operating temp. min.",
        "operating_temp_max_c":       "Operating temp. max.",
        "max_speed_rpm_peak":         "Max. operating speed",
        "shock_resistance":           "Resistance to shocks",
        "vibration_resistance":       "Resistance to vibrations",
        "weight_g":                   "Weight",
        "startup_torque_ncm":         "Starting torque",
        "shaft_load_radial_n":        "Shaft load — radial",
        "shaft_load_axial_n":         "Shaft load — axial",
        "moment_of_inertia":          "Mass moment of inertia",
        "encoder_type":               "Encoder type",
        "sensing_method":             "Measurement principle",
        "product_family":             "Product family",
    },
    "Sick": {
        "resolution_ppr":             "Pulses per revolution",
        "ppr_range_min":              "Programmable range min.",
        "ppr_range_max":              "Programmable range max.",
        "is_programmable":            "Programmable/configurable",
        "output_circuit_canonical":   "Output function",
        "output_signals":             "Output signals",
        "num_output_channels":        "Number of signal channels",
        "max_output_freq_hz":         "Output frequency",
        "supply_voltage_min_v":       "Supply voltage (min.)",
        "supply_voltage_max_v":       "Supply voltage (max.)",
        "output_current_ma":          "Load current",
        "power_consumption_typ_mw":   "Power consumption",
        "reverse_polarity_protection":"Reverse polarity protection",
        "short_circuit_protection":   "Short-circuit protection of the outputs",
        "housing_diameter_mm":        "Housing diameter",
        "shaft_diameter_mm":          "Shaft diameter",
        "shaft_type":                 "Mechanical design",
        "flange_type":                "Flange type / stator coupling",
        "connection_type":            "Connection type",
        "connector_pins":             "Connector pins",
        "ip_rating":                  "Enclosure rating",
        "operating_temp_min_c":       "Operating temperature (min.)",
        "operating_temp_max_c":       "Operating temperature (max.)",
        "max_speed_rpm_peak":         "Operating speed",
        "shock_resistance":           "Resistance to shocks",
        "vibration_resistance":       "Resistance to vibration",
        "weight_g":                   "Unit weight",
        "startup_torque_ncm":         "Start-up torque",
        "shaft_load_radial_n":        "Permissible shaft loading (radial)",
        "shaft_load_axial_n":         "Permissible shaft loading (axial)",
        "moment_of_inertia":          "Moment of inertia of the rotor",
        "encoder_type":               "Encoder type",
        "sensing_method":             "Sensing technology",
        "product_family":             "Product group",
    },
    "Wachendorff": {
        "resolution_ppr":             "Pulse count",
        "ppr_range_min":              "Programmable range min.",
        "ppr_range_max":              "Programmable range max.",
        "is_programmable":            "Programmable",
        "output_circuit_canonical":   "Output circuit",
        "output_signals":             "Output signals",
        "num_output_channels":        "Number of channels",
        "max_output_freq_hz":         "Max. output frequency",
        "supply_voltage_min_v":       "Operating voltage (min.)",
        "supply_voltage_max_v":       "Operating voltage (max.)",
        "output_current_ma":          "Output current",
        "power_consumption_typ_mw":   "Current consumption",
        "reverse_polarity_protection":"Reverse polarity protection",
        "short_circuit_protection":   "Short-circuit protection",
        "housing_diameter_mm":        "Flange diameter",
        "shaft_diameter_mm":          "Shaft / bore diameter",
        "shaft_type":                 "Shaft type",
        "flange_type":                "Flange type",
        "connection_type":            "Connection",
        "connector_pins":             "No. of pins",
        "ip_rating":                  "Protection class",
        "operating_temp_min_c":       "Operating temperature (min.)",
        "operating_temp_max_c":       "Operating temperature (max.)",
        "max_speed_rpm_peak":         "Max. speed",
        "shock_resistance":           "Shock resistance",
        "vibration_resistance":       "Vibration resistance",
        "weight_g":                   "Weight",
        "startup_torque_ncm":         "Starting torque",
        "shaft_load_radial_n":        "Shaft load radial",
        "shaft_load_axial_n":         "Shaft load axial",
        "moment_of_inertia":          "Rotor moment of inertia",
        "encoder_type":               "Encoder type",
        "sensing_method":             "Scanning method",
        "product_family":             "Series",
    },
    "Lika": {
        "resolution_ppr":             "Resolution [PPR]",
        "ppr_range_min":              "Min. resolution",
        "ppr_range_max":              "Max. resolution",
        "is_programmable":            "Programmable",
        "output_circuit_canonical":   "Electrical interface",
        "output_signals":             "Output signals",
        "num_output_channels":        "Number of channels",
        "max_output_freq_hz":         "Max. output frequency",
        "supply_voltage_min_v":       "Supply voltage (min.)",
        "supply_voltage_max_v":       "Supply voltage (max.)",
        "output_current_ma":          "Output current",
        "power_consumption_typ_mw":   "Current absorption",
        "housing_diameter_mm":        "Body diameter",
        "shaft_diameter_mm":          "Shaft / bore diameter",
        "shaft_type":                 "Shaft type",
        "flange_type":                "Flange",
        "connection_type":            "Connector / cable",
        "connector_pins":             "No. of pins",
        "ip_rating":                  "Protection degree",
        "operating_temp_min_c":       "Working temperature (min.)",
        "operating_temp_max_c":       "Working temperature (max.)",
        "max_speed_rpm_peak":         "Max. rotation speed",
        "shock_resistance":           "Shock",
        "vibration_resistance":       "Vibrations",
        "weight_g":                   "Weight [g]",
        "startup_torque_ncm":         "Starting torque",
        "shaft_load_radial_n":        "Radial load",
        "shaft_load_axial_n":         "Axial load",
        "moment_of_inertia":          "Moment of inertia",
        "encoder_type":               "Encoder type",
        "sensing_method":             "Scanning",
        "product_family":             "Series",
    },
    "Baumer": {
        "resolution_ppr":             "Pulse count per revolution",
        "ppr_range_min":              "Min. pulse count",
        "ppr_range_max":              "Max. pulse count",
        "is_programmable":            "Programmable",
        "output_circuit_canonical":   "Output circuit",
        "output_signals":             "Output signals",
        "num_output_channels":        "Number of channels",
        "max_output_freq_hz":         "Max. output frequency",
        "supply_voltage_min_v":       "Supply voltage (min.)",
        "supply_voltage_max_v":       "Supply voltage (max.)",
        "output_current_ma":          "Output current",
        "housing_diameter_mm":        "Flange diameter",
        "shaft_diameter_mm":          "Shaft diameter",
        "shaft_type":                 "Shaft type",
        "flange_type":                "Flange type",
        "connection_type":            "Connection",
        "connector_pins":             "No. of pins",
        "ip_rating":                  "Degree of protection",
        "operating_temp_min_c":       "Operating temperature (min.)",
        "operating_temp_max_c":       "Operating temperature (max.)",
        "max_speed_rpm_peak":         "Max. speed",
        "shock_resistance":           "Shock resistance",
        "vibration_resistance":       "Vibration resistance",
        "weight_g":                   "Weight",
        "startup_torque_ncm":         "Starting torque",
        "shaft_load_radial_n":        "Radial load",
        "shaft_load_axial_n":         "Axial load",
        "moment_of_inertia":          "Moment of inertia",
        "encoder_type":               "Encoder type",
        "sensing_method":             "Measuring principle",
        "product_family":             "Type",
    },
    "EPC": {
        "resolution_ppr":             "PPR",
        "is_programmable":            "Programmable",
        "output_circuit_canonical":   "Output type",
        "supply_voltage_min_v":       "Supply voltage (min.)",
        "supply_voltage_max_v":       "Supply voltage (max.)",
        "housing_diameter_mm":        "Housing diameter",
        "shaft_diameter_mm":          "Shaft diameter",
        "shaft_type":                 "Shaft configuration",
        "flange_type":                "Mounting style",
        "connection_type":            "Termination",
        "ip_rating":                  "IP rating",
        "operating_temp_min_c":       "Operating temperature (min.)",
        "operating_temp_max_c":       "Operating temperature (max.)",
        "max_speed_rpm_peak":         "Max. speed",
        "shock_resistance":           "Shock",
        "vibration_resistance":       "Vibration",
        "weight_g":                   "Weight",
        "encoder_type":               "Encoder type",
        "product_family":             "Series",
    },
    "Nidec": {
        "resolution_ppr":             "Pulses/Revolution",
        "is_programmable":            "Programmable",
        "output_circuit_canonical":   "Output",
        "supply_voltage_min_v":       "Supply voltage (min.)",
        "supply_voltage_max_v":       "Supply voltage (max.)",
        "shaft_diameter_mm":          "Shaft diameter",
        "shaft_type":                 "Shaft type",
        "connection_type":            "Termination",
        "ip_rating":                  "Environmental protection",
        "operating_temp_min_c":       "Operating temperature (min.)",
        "operating_temp_max_c":       "Operating temperature (max.)",
        "max_speed_rpm_peak":         "Maximum speed",
        "shock_resistance":           "Shock",
        "vibration_resistance":       "Vibration",
        "weight_g":                   "Weight",
        "encoder_type":               "Encoder type",
        "product_family":             "Series",
    },
}

def _native(mfr, col):
    """Return native field name for a manufacturer+column, fallback to canonical label."""
    return (_NATIVE_NAMES.get(mfr, {}).get(col)
            or _NATIVE_NAMES.get("Kubler", {}).get(col)
            or col.replace("_", " ").title())

# ── 4-column parameter mapping ────────────────────────────────────────────────
_SKIP_PM = {"manufacturer","source_pdf","order_pattern",
            "oc_shaft_type","oc_flange","oc_ppr","oc_interface","oc_connector","part_number"}

def _param_mapping_html(source_rec, target_rec, target_mfr="Kubler"):
    """4-column table: Target param | Target value | Source param | Source value."""
    src_mfr     = str(source_rec.get("manufacturer","Competitor"))
    src_mfr_esc = html.escape(src_mfr)
    tgt_mfr_esc = html.escape(target_mfr)

    display_fields = [c for c in CANONICAL_COLUMNS if c not in _SKIP_PM]

    def _val(rec, col):
        v = rec.get(col)
        if _safe(v) is None or str(v) in ("nan","None",""):
            return None
        return str(v)

    rows = ""
    for col in display_fields:
        tgt_v = _val(target_rec, col)
        src_v = _val(source_rec, col)
        if tgt_v is None and src_v is None:
            continue
        tgt_name = html.escape(_native(target_mfr, col))
        src_name = html.escape(_native(src_mfr, col))
        tgt_disp = html.escape(tgt_v) if tgt_v else '<span style="color:#c0cfe0;font-style:italic;">—</span>'
        src_disp = html.escape(src_v) if src_v else '<span style="color:#c0cfe0;font-style:italic;">—</span>'
        # Highlight row when values differ
        differ = (tgt_v and src_v and tgt_v != src_v)
        row_bg = "background:#fffbeb;" if differ else ""
        rows += (f'<tr style="{row_bg}">'
                 f'<td style="font-size:11px;font-weight:600;color:#1356a0;white-space:nowrap;">{tgt_name}</td>'
                 f'<td style="font-family:monospace;font-size:12px;color:#0B1E38;">{tgt_disp}</td>'
                 f'<td style="font-size:11px;font-weight:600;color:#3B6D11;white-space:nowrap;'
                 f'border-left:2px solid #e5e7eb;">{src_name}</td>'
                 f'<td style="font-family:monospace;font-size:12px;color:#0B1E38;">{src_disp}</td>'
                 f'</tr>')

    note = ('<p style="font-size:11px;color:#6b7280;margin:8px 0 0;">'
            '⚡ Amber rows = values differ. Field names shown as per each manufacturer\'s datasheet. '
            'Values sourced from manufacturer datasets via unified schema.</p>')
    return (
        f'<div style="overflow-x:auto;">'
        f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
        f'<thead><tr>'
        f'<th style="background:#0B2545;color:#93c5fd;padding:9px 12px;text-align:left;'
        f'font-size:10px;text-transform:uppercase;letter-spacing:.05em;white-space:nowrap;">'
        f'🎯 {tgt_mfr_esc} parameter</th>'
        f'<th style="background:#0B2545;color:#93c5fd;padding:9px 12px;text-align:left;'
        f'font-size:10px;text-transform:uppercase;letter-spacing:.05em;">{tgt_mfr_esc} value</th>'
        f'<th style="background:#0B2545;color:#86efac;padding:9px 12px;text-align:left;'
        f'font-size:10px;text-transform:uppercase;letter-spacing:.05em;border-left:2px solid #1a4a80;'
        f'white-space:nowrap;">🏭 {src_mfr_esc} parameter</th>'
        f'<th style="background:#0B2545;color:#86efac;padding:9px 12px;text-align:left;'
        f'font-size:10px;text-transform:uppercase;letter-spacing:.05em;">{src_mfr_esc} value</th>'
        f'</tr></thead>'
        f'<tbody>{rows}</tbody></table></div>{note}'
    )

# ── AI explanation ────────────────────────────────────────────────────────────
def _build_prompt(source, candidate, exp, target_mfr="Kübler"):
    score_pct = exp.get("score_pct","—"); tier=exp.get("tier","unknown")
    hard_stop = exp.get("hard_stop"); caps = exp.get("caps",[])
    src_mfr   = source.get("manufacturer","Competitor")

    # Build a full field-by-field comparison including ALL scored fields
    field_lines = []
    for f in exp.get("fields",[]):
        sim  = f.get("sim")
        pts  = f.get("pts")
        tier_f = f.get("tier",0)
        label  = f.get("label","")
        sv     = f.get("source_val","—")
        cv     = f.get("cand_val","—")
        note   = f.get("note","")
        if sim is not None:
            if sim >= 0.95:   sev = "MATCH"
            elif sim >= 0.7:  sev = "MINOR_DIFF"
            elif sim >= 0.3:  sev = "SIGNIFICANT_DIFF"
            else:              sev = "CRITICAL_MISMATCH"
            field_lines.append(f"  [{sev}] {label}: source={sv}, {target_mfr}={cv}, sim={sim:.2f} | {note}")
        elif tier_f == 0 and (sv != "—" or cv != "—"):
            field_lines.append(f"  [INFO] {label}: source={sv}, {target_mfr}={cv}")

    caps_text = "\n".join(f"  - {c}" for c in caps) if caps else "  None"

    return textwrap.dedent(f"""
    You are an expert industrial encoder applications engineer at AQB Solutions.
    A sales engineer needs to cross-reference a {src_mfr} encoder to find the best {target_mfr} replacement.

    SOURCE encoder ({src_mfr}):
      Part Number  : {source.get('part_number')}
      Family       : {source.get('product_family')}
      PPR          : {_fmt_ppr(source)}
      Output       : {source.get('output_circuit_canonical')}
      Housing Ø    : {_fmt(source.get('housing_diameter_mm'))} mm
      Shaft        : {source.get('shaft_type')}, Ø{_fmt(source.get('shaft_diameter_mm'),decimals=2)} mm
      IP           : IP{source.get('ip_rating')}
      Supply V     : {_fmt_voltage(source)}
      Temp         : {_fmt_temp(source)}
      Max Speed    : {_fmt(source.get('max_speed_rpm_peak'))} rpm

    {target_mfr.upper()} CANDIDATE:
      Part Number  : {candidate.get('part_number')}
      Family       : {candidate.get('product_family')}
      PPR          : {_fmt_ppr(candidate)}
      Output       : {candidate.get('output_circuit_canonical')}
      Housing Ø    : {_fmt(candidate.get('housing_diameter_mm'))} mm
      Shaft        : {candidate.get('shaft_type')}, Ø{_fmt(candidate.get('shaft_diameter_mm'),decimals=2)} mm
      IP           : IP{candidate.get('ip_rating')}
      Supply V     : {_fmt_voltage(candidate)}
      Temp         : {_fmt_temp(candidate)}
      Max Speed    : {_fmt(candidate.get('max_speed_rpm_peak'))} rpm

    SCORING RESULT:
      Overall score : {score_pct}  [{tier.upper()}]
      Hard stop     : {hard_stop or 'None'}
      Score caps    :
    {caps_text}

    Field-by-field analysis (ALL scored fields):
    {chr(10).join(field_lines) if field_lines else '  No scored fields available.'}

    Write an engineer-facing technical explanation following this EXACT structure:

    VERDICT: One sentence — is this a direct drop-in replacement, needs minor adaptation, or not recommended?

    PARAMETER ANALYSIS:
    For EVERY field marked [CRITICAL_MISMATCH] or [SIGNIFICANT_DIFF]:
    • ⚠ [Field name]: Explain WHY this difference matters technically (e.g. "PPR 2000 vs 1024 means controller must be reconfigured — 2× scaling error at PLC"). Be specific with numbers.

    For fields marked [MINOR_DIFF]:
    • ℹ [Field name]: Brief note on the difference and whether it's acceptable.

    For fields marked [MATCH] (only list the top 3 most important):
    • ✅ [Field name]: Confirmed match.

    BEFORE ORDERING:
    List 2–3 specific engineering checks required before placing an order.

    Use bullet points only. Be precise with numbers. No prose paragraphs. Max 200 words.
    """).strip()

@st.cache_data(show_spinner=False, ttl=3600)
def _nl_explanation(src_pn, cand_pn, score_pct, prompt):
    if not _API_KEY:
        return "• AI explanation unavailable — add ANTHROPIC_API_KEY to config or environment."
    try:
        resp=requests.post("https://api.anthropic.com/v1/messages",
            headers={"Content-Type":"application/json","x-api-key":_API_KEY,"anthropic-version":"2023-06-01"},
            json={"model":_MODEL,"max_tokens":400,"messages":[{"role":"user","content":prompt}]},
            timeout=20)
        if resp.status_code!=200:
            return f"• AI explanation unavailable (HTTP {resp.status_code})."
        text="".join(b.get("text","") for b in resp.json().get("content",[]) if b.get("type")=="text")
        return text.strip() or "• No explanation returned."
    except Exception as e:
        return f"• (Explanation unavailable: {e})"

# ── Schema tab HTML ───────────────────────────────────────────────────────────
def _schema_table_html():
    tier_labels={1:"Hard Stop",2:"Near-Hard",3:"Soft",0:"Info"}
    tier_chip={1:"tier-chip-1",2:"tier-chip-2",3:"tier-chip-3",0:"tier-chip-0"}
    rows=""
    for col in CANONICAL_COLUMNS:
        meta=UNIFIED_SCHEMA.get(col) or next(
            (m for m in UNIFIED_SCHEMA.values() if m.get("col")==col), None)
        if meta:
            tier=meta["tier"]; wt=meta["weight"]
            chip=f'<span class="tier-chip {tier_chip[tier]}">{tier_labels[tier]}</span>'
            wt_str=f"{wt:.2f}" if wt>0 else ("Hard Stop" if tier==1 else "—")
            label=html.escape(meta["label"]); rule=html.escape(meta.get("scoring_rule",""))
        else:
            chip='<span class="tier-chip tier-chip-0">Info</span>'
            wt_str="—"; label=html.escape(COLUMN_MAP.get(col,col)); rule=""
        rows+=(f'<tr><td style="font-family:DM Mono,monospace;font-size:11px;color:#1356a0;">{html.escape(col)}</td>'
               f'<td>{label}</td><td>{chip}</td>'
               f'<td style="font-family:DM Mono,monospace;font-size:11px;">{wt_str}</td>'
               f'<td style="font-size:11px;color:#6b7280;">{rule}</td></tr>')
    return (f'<table class="schema-table"><thead><tr>'
            f'<th>Column</th><th>Label</th><th>Tier</th><th>Weight</th><th>Scoring Rule</th>'
            f'</tr></thead><tbody>{rows}</tbody></table>')

# ── Header ────────────────────────────────────────────────────────────────────
total_rows=len(UNIFIED_DF)
mfr_counts=UNIFIED_DF.groupby("manufacturer").size()
stats_html="".join(f'<span class="db-stat">{m}: {c:,}</span>' for m,c in mfr_counts.items())

st.markdown(f"""
<div class="aqb-header">
  <div style="font-size:28px;">🔄</div>
  <div>
    <div class="aqb-title">AQB Solutions — AI Powered Encoder Cross-Reference Engine</div>
    <div class="db-stats">{stats_html}
      <span class="db-stat">Total: {total_rows:,}</span>
      <span class="db-stat">👤 {_USER_ID}</span>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    # ── Admin: any-to-any manufacturer selectors ──────────────────────────────
    if _IS_ADMIN:
        st.markdown('<div style="font-size:11px;color:#ffd580;text-transform:uppercase;'
                    'letter-spacing:.07em;font-weight:700;margin-bottom:6px;">⚙ Admin Mode</div>',
                    unsafe_allow_html=True)
        all_mfrs_for_admin = sorted(UNIFIED_DF["manufacturer"].dropna().unique().tolist())
        src_mfr_options = ["(Any competitor)"] + all_mfrs_for_admin
        tgt_mfr_options = all_mfrs_for_admin

        _admin_src = st.selectbox("Source manufacturer",
            src_mfr_options,
            index=src_mfr_options.index(st.session_state["admin_src_mfr"])
                  if st.session_state["admin_src_mfr"] in src_mfr_options else 0,
            key="sb_admin_src")
        _admin_tgt = st.selectbox("Target manufacturer",
            tgt_mfr_options,
            index=tgt_mfr_options.index(st.session_state["admin_tgt_mfr"])
                  if st.session_state["admin_tgt_mfr"] in tgt_mfr_options else
                  (tgt_mfr_options.index("Kubler") if "Kubler" in tgt_mfr_options else 0),
            key="sb_admin_tgt")
        st.session_state["admin_src_mfr"] = _admin_src
        st.session_state["admin_tgt_mfr"] = _admin_tgt
        _TGT_MFR  = _admin_tgt
        _SRC_FILTER = None if _admin_src == "(Any competitor)" else _admin_src
        st.markdown("---")
    else:
        _TGT_MFR    = "Kubler"
        _SRC_FILTER = None   # auto-detect from part number
        # Kubler target badge
        st.markdown(f"""
        <div class="kubler-target-badge">
          🎯 <strong>Target:</strong> Kübler<br>
          <span class="badge-sub">Logged in as {_USER_ID}</span>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('<div style="font-size:11px;color:#b8d4f0;text-transform:uppercase;'
                'letter-spacing:.07em;margin-bottom:2px;">Competitor Part Number</div>',
                unsafe_allow_html=True)

    with st.form("search_form"):
        pn_input = st.text_input("Part number", value=st.session_state["pn_value"],
            placeholder="Enter competitor part number…", label_visibility="collapsed")
        top_n = st.slider("Top N results", 1, 10, 5)
        col_btn, col_clr = st.columns([3,1])
        with col_btn:
            _btn_label = f"🔍  Find {_TGT_MFR} Match" if _IS_ADMIN else "🔍  Find Kübler Match"
            submitted = st.form_submit_button(_btn_label)
        with col_clr:
            cleared = st.form_submit_button("✕")

    # Auto-detect
    detected_mfr = _detect_mfr(pn_input) if pn_input.strip() else None
    if pn_input.strip():
        if detected_mfr and detected_mfr != _TGT_MFR:
            st.markdown(f'<div class="detect-badge">✓ Detected: {detected_mfr}</div>',
                        unsafe_allow_html=True)
            # Admin mismatch warning — selected source filter ≠ detected manufacturer
            if _IS_ADMIN and _SRC_FILTER and _SRC_FILTER != detected_mfr:
                st.markdown(
                    f'<div style="background:#d97706;border-radius:6px;'
                    f'padding:5px 11px;font-size:12px;font-weight:700;color:#ffffff;margin-top:5px;">'
                    f'⚠ Part looks like {detected_mfr} — Source set to {_SRC_FILTER}</div>',
                    unsafe_allow_html=True)
        elif detected_mfr == _TGT_MFR:
            st.markdown(f'<div class="detect-warn">⚠ This looks like a {_TGT_MFR} part number</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown('<div style="font-size:11px;color:#8bafd4;margin-top:4px;">'
                        '— Manufacturer not auto-detected</div>', unsafe_allow_html=True)

    if cleared:
        st.session_state.update({"pn_value":"","results":None,"status":None,"exps":None})
        st.rerun()

    if submitted and pn_input.strip():
        st.session_state["pn_value"] = pn_input.strip()
        src_mfr = detected_mfr if (detected_mfr and detected_mfr != _TGT_MFR) else None
        if _IS_ADMIN and _SRC_FILTER:
            src_mfr = _SRC_FILTER
        _t_search_start = datetime.datetime.utcnow()
        with st.spinner("Matching…"):
            _bl, _bo = _load_feedback()
            results, status, exps = find_matches_with_status(
                pn_input.strip(), UNIFIED_DF,
                target_manufacturer=_TGT_MFR,
                source_manufacturer=src_mfr,
                top_n=top_n,
                weights=st.session_state["weights"],
                blocklist=_bl,
                booklist=_bo,
            )
        results_list = _enrich_results(results, UNIFIED_DF)
        st.session_state.update({"results":results_list,"status":status,"exps":exps})
        # Build analytics payload
        top_m   = results_list[0].get("part_number","") if results_list else ""
        top_s   = f"{results_list[0].get('score_pct',0):.2f}" if results_list else ""
        tier    = status.get("tier","") if status else ""
        all_pns = "|".join(str(r.get("part_number","")) for r in results_list)
        all_sc  = "|".join(f"{r.get('score_pct',0):.2f}" for r in results_list)
        dur_ms  = round((datetime.datetime.utcnow() - _t_search_start).total_seconds()*1000)
        wt_snap = json.dumps({k:round(v,4) for k,v in st.session_state["weights"].items()})
        _log_event(_USER_ID,"search",
                   part_number=pn_input.strip(), detected_mfr=detected_mfr or "",
                   top_match=top_m, top_score=top_s,
                   match_tier=tier, num_results=len(results_list),
                   all_match_pns=all_pns, all_scores=all_sc,
                   weights_json=wt_snap, query_duration_ms=dur_ms)
        st.rerun()

    st.markdown("---")
    st.markdown('<div style="font-size:11px;color:#b8d4f0;text-transform:uppercase;'
                'letter-spacing:.06em;margin:2px 0;">📊 Database</div>', unsafe_allow_html=True)
    for mfr,cnt in mfr_counts.items():
        st.markdown(f'<div style="display:flex;justify-content:space-between;font-size:12px;padding:2px 0;">'
                    f'<span>{mfr}</span><span style="font-weight:700;color:#3a7fd4;">{cnt:,}</span>'
                    f'</div>', unsafe_allow_html=True)

    # Feedback count display
    _bl_s, _bo_s = _load_feedback()
    if _bl_s or _bo_s:
        st.markdown(f'<div style="font-size:11px;color:#b8d4f0;padding:4px 0;">'
                    f'💬 Feedback: {len(_bo_s)} boosted · {len(_bl_s)} blocked</div>',
                    unsafe_allow_html=True)

    # Bottom status bar
    st.markdown("---")
    db_dot  = "status-dot-green" if _DB_ONLINE else "status-dot-red"
    db_text = "CockroachDB connected" if _DB_ONLINE else "CockroachDB offline"
    src_text= "competitor_unified.csv"
    st.markdown(f"""
    <div style="font-size:11px;color:#b8d4f0;padding:4px 0;">
      <span class="{db_dot}"></span>{db_text}<br>
      <span style="color:#8bafd4;">📄 Data: {src_text}</span>
    </div>
    """, unsafe_allow_html=True)

    # Logout
    st.markdown("---")
    if st.button("Sign out", use_container_width=True):
        _log_event(_USER_ID, "logout")
        for k in ["logged_in","user_id","company","results","status","exps","pn_value"]:
            st.session_state.pop(k, None)
        st.rerun()

# ── Tabs ──────────────────────────────────────────────────────────────────────
(tab_search, tab_weights,
 tab_schema, tab_precedence, tab_about) = st.tabs([
    "🔍 Search Results",
    "⚖ Configure Weights",
    "📋 Schema",
    "📐 Parameter Precedence",
    "ℹ About",
])

results = st.session_state.get("results")
status  = st.session_state.get("status")
exps    = st.session_state.get("exps")
pn_val  = st.session_state.get("pn_value","")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Search Results
# ══════════════════════════════════════════════════════════════════════════════
with tab_search:
    if not pn_val:
        st.markdown("""
        <div class="empty-state" style="text-align:center;padding:64px 20px;color:#3a7fd4;
             background:white;border-radius:12px;border:2px dashed #c5d8f0;">
          <div style="font-size:52px;margin-bottom:12px;">🔍</div>
          <div style="font-size:18px;font-weight:700;margin-bottom:8px;">
            Enter a competitor part number to find the best Kübler replacement
          </div>
          <div style="font-size:13px;color:#6b7280;">
            Supports: Lika · Wachendorff · Sick · Baumer · EPC · Nidec
          </div>
        </div>""", unsafe_allow_html=True)
    elif status and status.get("error"):
        st.error(status["error"])
    elif not results:
        st.warning(f"No {_TGT_MFR} matches found for **{pn_val}**. "
                   "Try relaxing weights or verify the part number.")
    else:
        # Find source record
        src_mask = UNIFIED_DF["part_number"].astype(str).str.upper()==pn_val.upper()
        if not src_mask.any():
            src_mask = UNIFIED_DF["part_number"].astype(str).str.upper().str.startswith(pn_val.upper())
        src_rec  = UNIFIED_DF[src_mask].iloc[0].to_dict() if src_mask.any() else {}
        src_mfr  = src_rec.get("manufacturer","Competitor") if src_rec else detected_mfr or "Competitor"

        # ── Query card with labeled fields ─────────────────────────────────
        st.markdown(f"""
        <div class="query-card">
          <div class="query-label">Queried Encoder — {html.escape(str(src_mfr))}</div>
          <div class="query-pn">{html.escape(pn_val)}</div>
          {_query_card_grid(src_rec)}
        </div>""", unsafe_allow_html=True)

        if status and status.get("fallback"):
            st.info("ℹ Shaft-type hard stop relaxed — showing cross-shaft-type results.")

        top_tier = status.get("tier","weak") if status else "weak"
        st.markdown(f'<div class="tier-banner {_tier_css(top_tier)}">{_tier_label(top_tier)}</div>',
                    unsafe_allow_html=True)

        # ── Match cards with sub-tabs ───────────────────────────────────────
        for i, res in enumerate(results):
            score_pct = res.get("score_pct", 0)
            sc_cls    = "score-strong" if score_pct>=90 else ("score-good" if score_pct>=80 else "score-weak")
            rank_cls  = f"rank-{i+1}" if i<3 else "rank-n"
            exp       = exps[i] if exps and i<len(exps) else {}
            hard_stop = exp.get("hard_stop")

            with st.container():
                # Card header + specs grid
                st.markdown(f"""
                <div class="match-card">
                  <div class="match-head">
                    <div class="rank-badge {rank_cls}">#{i+1}</div>
                    <div>
                      <div class="match-pn">{html.escape(str(res.get('part_number','—')))}</div>
                      <div class="match-family">{html.escape(str(res.get('product_family','')))} · {html.escape(_TGT_MFR)}</div>
                    </div>
                    <div class="score-block">
                      <div class="score-num {sc_cls}">{score_pct:.2f}<span style="font-size:16px;">%</span></div>
                      <div class="score-lbl">Match Score</div>
                    </div>
                  </div>
                  {f'<div class="hardstop-banner">⛔ Hard stop: {html.escape(str(hard_stop))}</div>' if hard_stop else ''}
                  {_specs_grid(res)}
                </div>""", unsafe_allow_html=True)

                # Sub-tabs: AI Explanation + Parameter Mapping
                with st.expander("🤖 AI Explanation — click to expand/collapse", expanded=False):
                    if not _API_KEY:
                        st.warning("AI explanation unavailable — add ANTHROPIC_API_KEY to Streamlit secrets or config_claude.py.")
                    else:
                        prompt = _build_prompt(src_rec, res, exp, target_mfr=_TGT_MFR)
                        with st.spinner("Getting AI analysis…"):
                            text = _nl_explanation(pn_val, str(res.get("part_number","")), str(score_pct), prompt)

                        # Render with color-coded sections
                        lines = [l.strip() for l in text.split("\n") if l.strip()]
                        st.markdown('<div class="nl-box" style="padding:16px 20px;">', unsafe_allow_html=True)
                        for line in lines:
                            if line.startswith("VERDICT:"):
                                verdict_text = line[len("VERDICT:"):].strip()
                                color = "#065f46" if score_pct>=90 else ("#92400e" if score_pct>=80 else "#991b1b")
                                bg    = "#d1fae5" if score_pct>=90 else ("#fef3c7" if score_pct>=80 else "#fee2e2")
                                st.markdown(
                                    f'<div style="background:{bg};border-radius:8px;padding:10px 14px;'
                                    f'font-weight:700;color:{color};margin-bottom:12px;font-size:13.5px;">'
                                    f'🏁 {html.escape(verdict_text)}</div>',
                                    unsafe_allow_html=True)
                            elif line in ("PARAMETER ANALYSIS:", "BEFORE ORDERING:"):
                                st.markdown(f"**{line}**")
                            elif line.startswith("• ⚠") or line.startswith("- ⚠"):
                                st.markdown(
                                    f'<div style="background:#fef3c7;border-left:3px solid #f59e0b;'
                                    f'padding:6px 12px;border-radius:4px;margin:4px 0;font-size:13px;">'
                                    f'{html.escape(line)}</div>', unsafe_allow_html=True)
                            elif line.startswith("• ✅") or line.startswith("- ✅"):
                                st.markdown(
                                    f'<div style="background:#d1fae5;border-left:3px solid #10b981;'
                                    f'padding:6px 12px;border-radius:4px;margin:4px 0;font-size:13px;">'
                                    f'{html.escape(line)}</div>', unsafe_allow_html=True)
                            elif line.startswith("• ℹ") or line.startswith("- ℹ"):
                                st.markdown(
                                    f'<div style="background:#dbeafe;border-left:3px solid #3b82f6;'
                                    f'padding:6px 12px;border-radius:4px;margin:4px 0;font-size:13px;">'
                                    f'{html.escape(line)}</div>', unsafe_allow_html=True)
                            elif line.startswith("•") or line.startswith("-"):
                                st.markdown(line)
                            else:
                                st.markdown(line)
                        st.markdown('</div>', unsafe_allow_html=True)

                        # Log AI view once per card per search
                        _ai_log_key = f"_ai_logged_{pn_val}_{i}"
                        if not st.session_state.get(_ai_log_key):
                            _log_event(_USER_ID, "view_ai_explanation",
                                       part_number=pn_val,
                                       detected_mfr=str(src_mfr),
                                       ai_tab_viewed="yes")
                            st.session_state[_ai_log_key] = True

                with st.expander("📊 Parameter Mapping — click to expand/collapse", expanded=False):
                    pm_html = _param_mapping_html(src_rec, res, target_mfr=_TGT_MFR)
                    st.markdown(pm_html, unsafe_allow_html=True)
                    _log_event(_USER_ID, "view_param_mapping",
                               part_number=pn_val, top_match=res.get("part_number",""))

                # ── Feedback buttons — label + 👍👎 grouped on the right ──────
                match_pn  = str(res.get("part_number",""))
                fb_status = _feedback_status(pn_val, match_pn)
                fb_key    = f"_fb_{pn_val}_{i}"

                _fb_left, _fb_right = st.columns([5, 3])
                with _fb_right:
                    _fc1, _fc2, _fc3 = st.columns([4, 1, 1])
                    with _fc1:
                        if fb_status == "blocked":
                            st.markdown('<span style="font-size:11px;color:#991b1b;'
                                        'font-weight:700;line-height:2.2;">⛔ Poor match</span>',
                                        unsafe_allow_html=True)
                        elif fb_status == "boosted":
                            st.markdown('<span style="font-size:11px;color:#065f46;'
                                        'font-weight:700;line-height:2.2;">✅ Good match</span>',
                                        unsafe_allow_html=True)
                        else:
                            st.markdown('<span style="font-size:11px;color:#9ca3af;'
                                        'line-height:2.2;">Rate this match:</span>',
                                        unsafe_allow_html=True)
                    with _fc2:
                        thumb_up = st.button("👍", key=f"{fb_key}_up",
                                             help="Good match — boost this result",
                                             disabled=(fb_status == "boosted"))
                    with _fc3:
                        thumb_dn = st.button("👎", key=f"{fb_key}_dn",
                                             help="Poor match — exclude this result",
                                             disabled=(fb_status == "blocked"))

                if thumb_up:
                    _add_feedback(pn_val, match_pn, res.get("manufacturer",""), "boost", _USER_ID)
                    _log_event(_USER_ID, "feedback_boost", part_number=pn_val, top_match=match_pn)
                    st.toast("✅ Match boosted — will rank higher in future searches.", icon="✅")
                    st.rerun()
                if thumb_dn:
                    _add_feedback(pn_val, match_pn, res.get("manufacturer",""), "block", _USER_ID)
                    _log_event(_USER_ID, "feedback_block", part_number=pn_val, top_match=match_pn)
                    st.toast("⛔ Match blocked — will be excluded from future searches.", icon="⛔")
                    st.rerun()

            st.markdown("<div style='margin-bottom:8px'></div>", unsafe_allow_html=True)

        st.session_state["_src_rec"] = src_rec
        st.session_state["_src_mfr"] = str(src_mfr)

        # ── Download CSV ───────────────────────────────────────────────────
        st.markdown("---")
        _DISPLAY_COLS = [
            "part_number","product_family","manufacturer","match_score",
            "resolution_ppr","ppr_range_min","ppr_range_max","is_programmable",
            "output_circuit_canonical","housing_diameter_mm","shaft_diameter_mm","shaft_type",
            "ip_rating","supply_voltage_min_v","supply_voltage_max_v",
            "operating_temp_min_c","operating_temp_max_c","max_speed_rpm_peak",
            "connection_type","shock_resistance","vibration_resistance","weight_g",
            "startup_torque_ncm","shaft_load_radial_n","shaft_load_axial_n",
        ]
        cur_weights = st.session_state.get("weights", {})
        download_rows = []
        for r in results:
            row = {c: r.get(c,"") for c in _DISPLAY_COLS if c in r or c=="match_score"}
            row["match_score"] = f"{r.get('score_pct',0):.2f}%"
            row["query_part_number"]  = pn_val
            row["query_manufacturer"] = str(src_mfr)
            for mk, wv in cur_weights.items():
                row[f"weight__{mk}"] = round(wv, 4)
            download_rows.append(row)
        if download_rows:
            dl_df     = pd.DataFrame(download_rows)
            csv_bytes = dl_df.to_csv(index=False).encode("utf-8")
            clicked   = st.download_button(
                label="📥 Download Results as CSV",
                data=csv_bytes,
                file_name=f"kubler_matches_{pn_val.replace('/','_')}.csv",
                mime="text/csv",
                use_container_width=False,
            )
            if clicked:
                _log_event(_USER_ID, "csv_download",
                           part_number=pn_val, detected_mfr=str(src_mfr),
                           top_match=results[0].get("part_number","") if results else "",
                           csv_downloaded="yes")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Configure Weights
# ══════════════════════════════════════════════════════════════════════════════
with tab_weights:
    st.markdown("### ⚖ Configure Matching Weights")
    st.markdown("Sliders are on a **1–10 scale**. Backend normalises to sum = 1.0 automatically. "
                "Click **Apply Weights** to activate for the next search.")

    tier2_fields = {f:m for f,m in UNIFIED_SCHEMA.items() if m["tier"]==2}
    tier3_fields = {f:m for f,m in UNIFIED_SCHEMA.items() if m["tier"]==3 and m["weight"]>0}

    new_ints={}
    wt_col1, wt_col2 = st.columns(2)
    with wt_col1:
        st.markdown("**Tier 2 — Near-Hard** *(high baseline)*")
        for field, meta in tier2_fields.items():
            cur=st.session_state["weight_ints"].get(field, DEFAULT_WEIGHT_INTS.get(field,5))
            new_ints[field]=st.slider(
                f"{meta['label']} ({meta['weight']:.0%} default)", 1, 10, cur,
                key=f"wt_{field}")

    with wt_col2:
        st.markdown("**Tier 3 — Soft** *(configurable)*")
        for field, meta in tier3_fields.items():
            cur=st.session_state["weight_ints"].get(field, DEFAULT_WEIGHT_INTS.get(field,5))
            new_ints[field]=st.slider(
                f"{meta['label']} ({meta['weight']:.0%} default)", 1, 10, cur,
                key=f"wt_{field}")

    c1, c2 = st.columns([2,1])
    with c1:
        if st.button("✅ Apply Weights", use_container_width=True):
            st.session_state["weight_ints"]=new_ints
            st.session_state["weights"]=_ints_to_normalized(new_ints)
            st.success("Weights applied. Run a new search to use them.")
    with c2:
        if st.button("↺ Reset to Default"):
            st.session_state["weight_ints"]=dict(DEFAULT_WEIGHT_INTS)
            st.session_state["weights"]=dict(DEFAULT_WEIGHTS)
            st.rerun()

    norm = _ints_to_normalized(new_ints)
    total_check = sum(norm.values())
    st.markdown(f"*Normalised weight total: {total_check:.3f} (always = 1.0)*")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Schema
# ══════════════════════════════════════════════════════════════════════════════
with tab_schema:
    st.markdown("### 📋 Unified Schema — All 43 Fields")
    st.markdown(
        f"Complete field reference for the unified encoder database. "
        f"Tier 1 = hard stop (excluded if triggered). "
        f"Tier 2 = near-hard (high weight). "
        f"Tier 3 = soft (configurable). "
        f"Info = display only, not scored."
    )
    st.markdown(_schema_table_html(), unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("#### Output Circuit Aliases")
    st.markdown("All raw manufacturer circuit strings normalised to these canonical labels:")
    canon_groups={}
    for raw,canon in OUTPUT_CIRCUIT_CANONICAL.items():
        canon_groups.setdefault(canon,[]).append(raw)
    cols=st.columns(3)
    for i,(canon,aliases) in enumerate(sorted(canon_groups.items())):
        with cols[i%3]:
            st.markdown(f"**{canon}**")
            st.markdown(", ".join(f"`{a}`" for a in sorted(set(aliases))[:10]))

# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — Parameter Precedence
# ══════════════════════════════════════════════════════════════════════════════
with tab_precedence:
    st.markdown("### 📐 Parameter Precedence Framework")
    st.markdown("Engineering rationale for every scored field, ordered by priority.")

    leg1,leg2,leg3,leg4=st.columns(4)
    for col_obj, bg, border, text_color, title, desc in [
        (leg1,"#fee2e2","#fca5a5","#991b1b","Tier 1 — Hard Stop","Score=0, result excluded"),
        (leg2,"#fef3c7","#fcd34d","#92400e","Tier 2 — Near-Hard","High weight (0.12–0.25)"),
        (leg3,"#dbeafe","#93c5fd","#1e40af","Tier 3 — Soft","Configurable weights"),
        (leg4,"#f3f4f6","#d1d5db","#374151","Tier 0 — Info","Display only, not scored"),
    ]:
        with col_obj:
            st.markdown(f'<div style="background:{bg};border:1px solid {border};border-radius:8px;'
                        f'padding:10px 14px;text-align:center;">'
                        f'<div style="font-size:11px;font-weight:800;color:{text_color};text-transform:uppercase;">{title}</div>'
                        f'<div style="font-size:11px;color:{text_color};margin-top:4px;">{desc}</div>'
                        f'</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # Full precedence table
    rows=""
    tier_labels={1:"Hard Stop",2:"Near-Hard",3:"Soft",0:"Info"}
    tier_chips={1:'<span class="tier-chip tier-chip-1">Hard Stop</span>',
                2:'<span class="tier-chip tier-chip-2">Near-Hard</span>',
                3:'<span class="tier-chip tier-chip-3">Soft</span>',
                0:'<span class="tier-chip tier-chip-0">Info</span>'}
    current_ints=st.session_state.get("weight_ints",DEFAULT_WEIGHT_INTS)
    for i,(field,meta) in enumerate(UNIFIED_SCHEMA.items()):
        tier=meta["tier"]; wt=meta["weight"]
        wt_str=f"{wt:.2f}" if wt>0 else ("Hard Stop" if tier==1 else "—")
        cur_int=current_ints.get(field,"—")
        cur_str=f"{cur_int}/10" if isinstance(cur_int,int) else "—"
        rows+=(f'<tr>'
               f'<td style="color:#6b7280;font-size:11px;">{i+1}</td>'
               f'<td style="font-weight:700;color:#0B1E38;">{html.escape(meta["label"])}</td>'
               f'<td style="font-family:DM Mono,monospace;font-size:11px;color:#1356a0;">'
               f'{html.escape(meta.get("col",field))}</td>'
               f'<td>{tier_chips[tier]}</td>'
               f'<td style="font-family:DM Mono,monospace;font-size:11px;">{wt_str}</td>'
               f'<td style="font-size:11px;">{cur_str}</td>'
               f'<td style="font-size:11px;color:#374151;">{html.escape(meta.get("scoring_rule",""))}</td>'
               f'<td style="font-size:11px;color:#6b7280;">{html.escape(meta.get("rationale",""))}</td>'
               f'</tr>')

    st.markdown(f"""
    <table class="schema-table"><thead><tr>
    <th>#</th><th>Field</th><th>Schema Column</th><th>Tier</th>
    <th>Default Wt</th><th>Current Wt</th><th>Scoring Rule</th><th>Engineering Rationale</th>
    </tr></thead><tbody>{rows}</tbody></table>
    """, unsafe_allow_html=True)

with tab_about:
    st.markdown("### ℹ About — AQB Solutions Encoder Cross-Reference Platform")

    total_rows_about = len(UNIFIED_DF)

    st.markdown(f"""
## What This App Does

The **AQB Solutions AI Powered Encoder Cross-Reference Engine** helps sales engineers and technical staff find the best matching replacement encoder across multiple manufacturers — instantly and with engineering-grade precision.

Given any competitor encoder part number, the tool:
1. Looks up the encoder's full technical specification from its database of **{total_rows_about:,} encoder configurations**
2. Scores every candidate replacement encoder using a **tiered parameter matching engine**
3. Returns the top matches ranked by compatibility score, with an **AI-generated technical explanation** of each match

---

## How It Works — The Scoring Engine

Matching is done across **12 scored parameters** in 3 tiers:

| Tier | Behaviour | Parameters |
|---|---|---|
| **Tier 1 — Hard Stop** | Score = 0, candidate excluded | Shaft type (solid vs hollow), hollow bore Δ > 1 mm, TTL ↔ HTL voltage class cross |
| **Tier 2 — Near-Hard** | High fixed weight (0.12–0.25) | Resolution (PPR), Output circuit, Housing diameter, Shaft diameter |
| **Tier 3 — Soft** | User-configurable weight | IP rating, Temperature range, Supply voltage, Max speed, Connection type |

**Active weight normalisation:** If a field is missing for either encoder, it is excluded from scoring and the remaining weights are renormalised to sum to 1.0 — so missing data never unfairly deflates a score.

**Match score grading:**
- ✅ **≥ 90%** — Strong match — direct replacement candidate, order with confidence
- ⚠ **80–89%** — Good match — minor differences, verify before ordering
- ❌ **< 80%** — Weak match — engineering review required before substitution

---

## How to Use This App

**Step 1 — Enter a part number**
Type or paste a competitor encoder part number into the sidebar. The app will auto-detect the manufacturer from the part number pattern. Supported: Lika · Wachendorff · Sick · Baumer · EPC · Nidec.

**Step 2 — Run the search**
Click "Find Kübler Match". The engine scores all {total_rows_about:,} encoders in under a second and returns the top N results.

**Step 3 — Review results**
Each match card shows:
- **Score** (0–100%) with colour coding
- **Specs grid** — key parameters at a glance
- **🤖 AI Explanation** (expandable) — field-by-field technical analysis: what matches, what differs, and what to check before ordering
- **📊 Parameter Mapping** (expandable) — side-by-side table of every parameter in each manufacturer's native field names

**Step 4 — Rate matches**
Use **👍 / 👎** buttons to mark matches as good or poor. Boosted matches rise to the top; blocked matches are excluded — for this part number in all future searches.

**Step 5 — Adjust weights (optional)**
Open the **⚖ Configure Weights** tab to shift emphasis between parameters. All weights normalise to 1.0 automatically.

**Step 6 — Download**
Use the **📥 Download Results as CSV** button to export the ranked matches with all parameters and weight settings.

---

## Admin Mode (AQBADMIN)

Admin login unlocks **any-to-any matching** — set both source and target manufacturer freely via dropdowns in the sidebar. Useful for cross-referencing between any two manufacturer families, not just to Kübler.

---

## Database
**{total_rows_about:,} total encoder configurations** across {UNIFIED_DF['manufacturer'].nunique()} manufacturers:
""")
    for mfr, cnt in UNIFIED_DF["manufacturer"].value_counts().items():
        st.markdown(f"- **{mfr}:** {cnt:,} configurations")

    st.markdown(f"""
---
*Version 14 · AQB Solutions · Powered by Claude AI (Anthropic)*
*Logged in as: **{_USER_ID}** ({_COMPANY})*
    """)