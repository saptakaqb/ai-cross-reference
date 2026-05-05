# V17 Changelog

## Release: April 30, 2026
## DB: 741,626 rows × 47 columns × 10 manufacturers

---

### Schema — 3 new fields (47 columns total)
| Field | Tier | Description |
|---|---|---|
| `is_atex_certified` | **T1 Hard Stop** | ATEX/Ex zone certification. Source ATEX → candidate MUST be ATEX |
| `shaft_insulation_v` | T0 Warning flag | Shaft insulation voltage (e.g. "3.5 kV"). Advisory only — not scored |
| `corrosion_protection_class` | T3 Soft | ISO 12944-2 class (C4, C5-M, CX). Scored when source has a rating |

### Matcher improvements
- **ATEX T1 hard stop** — ATEX-certified source cannot match non-ATEX candidate
- **sensing_method now scored T3** (weight 0.04) — optical vs magnetic → 0.30 sim instead of ignored
- **corrosion_protection_class scored T3** (weight 0.02) — ISO 12944-2 class comparison
- **Tighter PPR penalty curve** — ≥95% match now scores 0.78 (was 0.95); ≥99%=0.92
- **Completeness penalty** — prevents inflated scores when few fields are comparable. No penalty when ≥70% of weight has data.
- **Score precision** — raw float returned, displayed to exactly 2 decimal places (e.g. 86.38%, not 86.4%)
- **Warning flags** — SENSING_MISMATCH and SHAFT_INSULATION_MISSING appear on match cards
- **Field symbols** — ✅🟢🟡🟠🔴 based on similarity value, sorted worst-first

### UI improvements
- **Inter font** replaces DM Sans (cleaner for data-dense technical displays)
- **JetBrains Mono** replaces DM Mono for part numbers and spec values
- **Refreshed color palette** — deeper navy (#0D1B2A), Google Blue (#1a73e8)
- **Match cards** — cleaner border treatment, subtle bottom-border header separator
- **Score display** — 30px bold with 2 decimal places
- **Warning banners** — amber left-border banners below hard-stop for advisory warnings
- **AI prompt** — warnings section included first; fields sorted worst→best with 🔴🟡✅ symbols

### Bug fixes
- `product_url` removed from parameter comparison table (URL button still shows on cards)
- Baumer HD patterns added to MFR auto-detect (HOG, HMG, POG, HOGS, EExOG)
- `is_atex_certified`, `shaft_insulation_v`, `corrosion_protection_class` added to Baumer+Kubler native names
