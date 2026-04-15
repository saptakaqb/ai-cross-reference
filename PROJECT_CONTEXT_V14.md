# Encoder Cross-Reference Tool — PROJECT CONTEXT V14
**Date:** 2026-04-15
**Current release ZIP:** encoder_crossref_v14.zip
**GitHub:** saptakaqb/ai-cross-reference
**Stack:** Python · Streamlit · Claude API · CSV

## What Changed This Session (V13 → V14)

### 1. New Database — competitor_unified.csv (713,870 rows × 43 cols)
| Manufacturer | Rows | Families |
|---|---|---|
| Kübler | 424,784 | 12 (all, rule-compliant ETL) |
| EPC | 200,000 | 6 (858S, 802S, 755A shaft/hollow, 776, 260) |
| Nidec | 60,000 | 4 (AV20, AV25, AV30B, AV44) |
| Wachendorff | 20,219 | 7 |
| Sick | 7,354 | 22 (circuit now from part number) |
| Lika | 962 | 20 |
| Baumer | 551 | 52 |

New ETL scripts: `kubler_etl_rules.py`, `epc_etl_rules.py`
Assembler: `make_kubler_compatible_samples.py` (fully vectorized, all 7 sources)

### 2. Any-to-Any Matching (Admin Login)
- KUBADMIN login → sidebar shows "Source manufacturer" + "Target manufacturer" dropdowns
- KUB-prefix login → target fixed to Kübler (unchanged)
- `find_matches_with_status()` now accepts `source_manufacturer` param

### 3. Native Parameter Names in Parameter Mapping
- `_param_mapping_html(source_rec, target_rec, target_mfr=)` — dynamic target mfr
- Header shows "🎯 {TargetMfr} parameter | value | 🏭 {SourceMfr} parameter | value"
- All 7 manufacturers have native field names in `_NATIVE_NAMES` dict

### 4. AI Explanation Improvements
- `_build_prompt()` now receives `target_mfr` parameter
- ALL scored fields classified as MATCH / MINOR_DIFF / SIGNIFICANT_DIFF / CRITICAL_MISMATCH
- Structured output: VERDICT → PARAMETER ANALYSIS → BEFORE ORDERING
- Color-coded rendering: green (✅ match), amber (⚠ diff), blue (ℹ info)

### 5. Thumbs Up/Down Feedback (Direct Override)
- 👍/👎 buttons below each match card
- `feedback_overrides.json` — blocklist + booklist persisted locally
- Blocker: excluded from results for that query_pn
- Booster: floated to top of results for that query_pn
- `find_matches_with_status()` applies filters before returning results
- Sidebar shows "💬 Feedback: N boosted · N blocked"

## Credentials (PoC only)
- KUB001 / kubler2024
- KUB002 / kubler2024
- KUB003 / aqb2024
- AQBADMIN / aqbadmin2024  ← admin, any-to-any matching ← any-to-any matching

## Tasks Remaining (V15)
1. Login screen redesign (professional dark blue + Kübler orange)
2. Lika/Wachendorff re-filter with larger raw data
3. Nidec AV45/AV56A/AV56S integration (from nidec_final.csv.gz)
4. Full independent left×right PPR for Nidec dual-output families
5. Feedback persistence to CockroachDB (v14 deferred)
6. Weight retuning from feedback patterns (v15 target)
