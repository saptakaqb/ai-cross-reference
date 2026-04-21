# Kübler Encoder — Datasheet Rules Reference
**Generated:** 2026-04-14  
**Scope:** 4 datasheets covering 6 encoder families  
**Purpose:** Documents every constraint from each datasheet used to limit the combinatorial explosion of order codes during ETL generation.

---

## General Principles (all encoders)

1. **Special-length cables** (connection types A, B, E, F, L, M, N etc.) are custom-order. They are NOT enumerated by cable length. Each special-length connection type gets **one representative row** per circuit/shaft combination.
2. **P03 / special signal format** — optional on request only. Excluded from generation.
3. **Ex 2/22 / ATEX variants** — optional on request. Excluded.
4. **Salt spray tested variants** — optional on request. Excluded.
5. **Other pulse rates on request** — only the discrete PPR values listed in the datasheet are generated.
6. **US-only variants** (footnoted) — included in generation but flagged; can be filtered with `--eu_only`.

---

## Datasheet 1: KIS40 / KIH40 (`KIS40KIH40_en.pdf`)

Two encoders on one datasheet: **KIS40** (solid shaft) and **KIH40** (blind hollow shaft).

### Shared specs
- Housing diameter: 40 mm
- IP rating: IP64
- Temperature: -20 °C … +70 °C
- Max speed: 4,500 rpm
- Weight: ~170 g
- Shock: 1,000 m/s², 6 ms
- Vibration: 100 m/s², 55–2,000 Hz
- Sensing: Optical

### KIS40 Order Code: `8.KIS40.1{a}{b}{c}{d}.{e}`

| Param | Code | Description |
|---|---|---|
| Flange | `1` | Clamping-synchro flange ø40mm — **only one option** |
| Shaft `b` | `3` | ø6 × 12.5mm with flat |
| | `5` | ø¼" × 12.5mm with flat |
| | `6` | ø8 × 12.5mm with flat |
| Output/supply `c` | `3` | Open collector NPN with inv / 10–30V |
| | `4` | Push-pull with inv / 10–30V |
| | `6` | RS422 with inv / 5V |
| | `7` | Open collector NPN without inv / 10–30V |
| | `8` | Push-pull without inv / 10–30V |
| | `A` | Open collector NPN with inv / 5–30V |
| | `B` | Push-pull with inv / 5–30V |
| | `C` | RS422 with inv / 5–30V |
| Connection `d` | `1` | Axial cable 2m PVC |
| | `2` | Radial cable 2m PVC |
| | `4` | Radial 0.5m PVC + M12 5-pin |
| | `6` | Radial 0.5m PVC + M12 8-pin |
| | `A` | Axial cable special length (**1 rep row**) |
| | `B` | Radial cable special length (**1 rep row**) |
| PPR `e` | 22 values | 10, 25, 50, 60, 100, 120, 150, 200, 250, 360, 400, 500, 512, 600, 1000, 1024, 1500, 1800, 2000, 2048, 2500, 2560 |

**Constraints:**
- `f` (P03 special format): optional on request — **excluded**
- Special lengths for A/B: 3, 5, 8, 10, 15m available — **not enumerated, 1 row each**
- No footnoted US-only options exist for KIS40

### KIH40 Order Code: `8.KIH40.{a}{b}{c}{d}.{e}`

| Param | Code | Description |
|---|---|---|
| Flange `a` | `2` | With spring element, long |
| | `5` | With stator coupling ø46mm |
| Bore `b` | `2` | ø6mm |
| | `3` | ø¼" |
| | `4` | ø8mm |
| Output/supply `c` | Same 8 options as KIS40 | |
| Connection `d` | Same 6 options as KIS40 | |
| PPR `e` | Same 22 values as KIS40 | |

**Constraints:**
- Insertion depth: max 18mm, min 15mm
- Same special-length and P03 rules as KIS40

**Valid combinations:** 1 flange × 3 shaft × 8 circuits × 6 connections × 22 PPR = **3,168 KIS40** + 2 flanges × 3 bores × 8 circuits × 6 connections × 22 PPR = **6,336 KIH40** = **9,504 total** (before special-length consolidation → ~**7,392 after** treating A/B as 1 row each)

---

## Datasheet 2: 5000 / 5020 (`50005020_en.pdf`)

Two encoders on one datasheet: **5000** (solid shaft) and **5020** (through hollow shaft).

### Shared specs
- Housing diameter: 58 mm
- IP65: max speed 12,000 rpm (6,000 continuous)
- IP66/67: max speed 6,000 rpm (3,000 continuous)
- Weight: ~400 g
- Shock: 3,000 m/s², 6 ms (MIL connectors: 2,500 m/s²)
- Vibration: 300 m/s², 10–2,000 Hz (MIL: 100 m/s²)
- Temperature: -40 °C … +85 °C (cable fixed: -30°C min; moving: -20°C min)
- Sensing: Optical

### 5000 Order Code: `8.5000.{a}{b}{c}{d}.{e}`

**Flange (a) and IP level:**

| Code | Description | IP | US? |
|---|---|---|---|
| `5` | Synchro ø50.8mm | IP66/67 | No |
| `6` | Synchro ø50.8mm | IP65 | No |
| `7` | Clamping ø58mm | IP66/67 | No |
| `8` | Clamping ø58mm | IP65 | No |
| `A` | Synchro ø58mm | IP66/67 | No |
| `B` | Synchro ø58mm | IP65 | No |
| `C` | Square 63.5mm | IP66/67 | No |
| `D` | Square 63.5mm | IP65 | No |
| `G` | Euro ø115mm | IP66/67 | US¹ |
| `1` | Servo ø50.8mm | IP66/67 | US¹ |
| `2` | Servo ø50.8mm | IP65 | US¹ |
| `3` | Square 52.3mm | IP66/67 | US¹ |
| `4` | Square 52.3mm | IP65 | US¹ |
| `E` | Servo ø63.5mm | IP66/67 | US¹ |
| `F` | Servo ø63.5mm | IP65 | US¹ |

**Shaft (b):**

| Code | Description | Constraint |
|---|---|---|
| `1` | ø6 × 10mm, flat | — |
| `2` | ø¼ × ⅝" | — |
| `6` | ø8 × 15mm, flat | — |
| `3` | ø10 × 20mm, flat | — |
| `4` | ø3/8 × ⅝" | — |
| `5` | ø12 × 20mm, flat | — |
| `7` | ø¼ × ⅞" | US only¹ |
| `8` | ø3/8 × ⅞" | US only¹ |
| `B` | ø11 × 33mm feather key | **Only with flange G**² |

**Output circuit (c):**

| Code | Description |
|---|---|
| `4` | RS422 / 5V DC |
| `1` | RS422 / 5–30V DC |
| `2` | Push-pull 7272 / 5–30V DC |
| `5` | Push-pull / 10–30V DC |
| `3` | Open collector / 5–30V DC (US only¹) |
| `8` | Push-pull no-cap / 5–30V DC (US only¹, **no CE**⁶) |

**Connection (d):**

| Code | Type | Inv signal? | Constraint |
|---|---|---|---|
| `1` | Axial cable 1m PVC | yes | — |
| `A` | Axial cable special PVC | yes | **1 rep row** |
| `2` | Radial cable 1m PVC | yes | — |
| `B` | Radial cable special PVC | yes | **1 rep row** |
| `P` | Axial M12 5-pin | **no inv** | Only c=5 or c=3⁵ |
| `R` | Radial M12 5-pin | **no inv** | Only c=5 or c=3⁵ |
| `3` | Axial M12 8-pin | yes | — |
| `4` | Radial M12 8-pin | yes | — |
| `7` | Axial M23 12-pin | yes | — |
| `8` | Radial M23 12-pin | yes | — |
| `Y` | Radial MIL 10-pin | yes | — |
| `W` | Radial MIL 7-pin | **no inv** | Only c=5 or c=3⁵ |
| `9` | Radial MIL 6-pin | **no inv** | US only¹, c=3 only⁵ |
| `L` | Radial cable+M12 8-pin special | yes | **1 rep row** |
| `M` | Radial cable+M23 12-pin special | yes | **1 rep row** |
| `N` | Radial cable+Sub-D 9-pin special | yes | **1 rep row** |

**PPR (e):** 53 values — 1, 2, 4, 5, 10, 12, 14, 20, 25, 28, 30, 32, 36, 50, 60, 64, 80, 100, 120, 125, 150, 180, 200, 240, 250, 256, 300, 342, 360, 375, 400, 500, 512, 600, 625, 720, 800, 900, 1000, 1024, 1200, 1250, 1500, 1800, 2000, 2048, 2500, 3000, 3600, 4000, 4096, 5000

**Constraint footnotes:**
- ¹ US version — included but flagged `is_us_variant=True`
- ² Shaft B only with flange G
- ⁵ Without inverted signal — these connections only valid with output c=5 (or c=3 US)
- ⁶ Circuit 8: no CE marking — valid combination but flagged

### 5020 Order Code: `8.5020.{a}{b}{c}{d}.{e}`

**Flange (a):**

| Code | Description | IP | US? |
|---|---|---|---|
| `1` | Spring element long | IP66/67 | No |
| `2` | Spring element long | IP65 | No |
| `3` | Torque stop long | IP66/67 | No |
| `4` | Torque stop long | IP65 | No |
| `7` | Stator coupling ø65mm | IP66/67 | No |
| `8` | Stator coupling ø65mm | IP65 | No |
| `C` | Stator coupling ø63mm | IP66/67 | No |
| `D` | Stator coupling ø63mm | IP65 | No |
| `5` | Stator coupling ø57.2mm | IP66/67 | US¹ |
| `6` | Stator coupling ø57.2mm | IP65 | US¹ |

**Bore (b):** `1`=6mm, `2`=¼", `9`=8mm, `4`=3/8", `3`=10mm, `5`=12mm, `6`=½", `A`=14mm, `8`=15mm, `7`=5/8"

**Output (c):** Same 6 options as 5000

**Connection (d):**

| Code | Type | Notes |
|---|---|---|
| `1` | Radial cable 1m PVC | — |
| `A` | Radial cable special PVC | **1 rep row** |
| `E` | Tangential cable 1m PVC | Hollow shaft only ✓ |
| `F` | Tangential cable special PVC | **1 rep row** |
| `R` | Radial M12 5-pin | No inv, c=5 only⁵ |
| `2` | Radial M12 8-pin | — |
| `4` | Radial M23 12-pin | — |
| `6` | Radial MIL 7-pin | — |
| `7` | Radial MIL 10-pin | — |
| `H` | Tang 0.3m PVC + M12 8-pin | — |
| `L` | Tang special + M12 8-pin | **1 rep row** |
| `M` | Tang special + M23 12-pin | **1 rep row** |
| `N` | Tang special + Sub-D 9-pin | **1 rep row** |

**PPR (e):** Same 53 values as 5000

---

## Datasheet 3: K58I shaft + K58I hollow (`K58I_en.pdf`)

Two encoder types on one datasheet. **Programmable PPR: 1–5,000.**

### Shared specs
- Housing diameter: 58 mm
- IP65: max speed 12,000 rpm (6,000 continuous)
- IP66/67: max speed 6,000 rpm (3,000 continuous)
- Temperature: -40 °C … +85 °C
- Sensing: Optical (metal code disk)

### K58I Shaft Order Code: `K58I.O{a}.{b}.{c}{d}{e}{f}.{g}{h}{i}{k}.{l}`

| Param | Code | Description | Constraint |
|---|---|---|---|
| Interface `a` | `PP` | Push-pull | Only supply c=2 (5–30V) |
| | `RS` | RS422 | Supply c=1 or c=2 |
| PPR `b` | `XXXXX` | 1–5,000 (programmable) | `is_programmable=True`, `ppr_range_min=1`, `ppr_range_max=5000` |
| Supply `c` | `1` | 5V DC | — |
| | `2` | 5–30V DC | — |
| Version `d` | `S1` | Shaft with flat | Not with f=11 |
| | `S3` | Feather key | **Only with f=11** |
| Flange `e` | `C5` | Clamping flange | — |
| | `S5` | Synchro flange | — |
| | `Q5` | Square 63.5mm | — |
| | `E5` | Euro flange | — |
| | `V5` | Servo flange | — |
| Shaft `f` (with S1) | `06` | ø6 × 10mm | — |
| | `08` | ø8 × 15mm | — |
| | `10` | ø10 × 20mm | — |
| | `12` | ø12 × 20mm | — |
| | `1A` | ø¼ × ⅝" | — |
| | `1B` | ø¼ × ⅞" | — |
| | `2A` | ø3/8 × ⅝" | — |
| | `2B` | ø3/8 × ⅞" | — |
| Shaft `f` (with S3) | `11` | ø11 × 33mm feather key | **Only with d=S3** |
| IP `g` | `65` | IP65 | — |
| | `6A` | IP66/67 | — |
| Position `h` | `A` | Axial | **Not with k=D,E,J** |
| | `R` | Radial | — |
| Connection `i` | `1` | PVC cable | — |
| | `3` | PUR cable, open-ended | Ex 2/22, IP66/67 only |
| | `C` | Connector on housing | — |
| Cable/connector `k` | `1` | Open-ended | Only with i=1,3 |
| | `2` | M12 8-pin | — |
| | `5` | M12 8-pin special assign | **Not with i=1,3** |
| | `3` | M12 5-pin | — |
| | `6` | M12 5-pin special assign | **Not with i=1,3** |
| | `4` | M23 12-pin | — |
| | `D` | MIL 7-pin | **Not with i=1,3** |
| | `H` | MIL 7-pin special assign | **Not with i=1,3** |
| | `E` | MIL 10-pin | **Not with i=1,3** |
| | `J` | MIL 10-pin special assign | **Not with i=1,3** |
| Cable length `l` | `0010`–`0100` | 1m, 2m, 3m, 5m, 10m | Only when i=1 or i=3 |

**Key cross-parameter rules:**
- `PP` interface → must use supply `c=2` (5–30V)
- `RS` interface → can use `c=1` (5V) or `c=2` (5–30V)
- `d=S3` (feather key) → only with `f=11`; `d=S1` → not with `f=11`
- `h=A` (axial) → not with `k=D, E, J` (MIL connectors)
- `k=5,6,H,J` (special assignment connectors) → not with `i=1,3`
- `k=D,H,E,J` (MIL connectors) → not with `i=1,3`
- `i=3` (PUR cable) → open-ended only, Ex 2/22, IP66/67 only (not generated as standard)
- Cable length `l` → only applies when `i=1` or `i=3`; when `i=C`, no cable length

### K58I Hollow Order Code: same pattern, different `d`, `e`, `f`, `h`

| Param | Code | Description | Constraint |
|---|---|---|---|
| Version `d` | `H1` | Through hollow, flange-side clamp | — |
| | `H2` | Through hollow, flange-side + isolation | — |
| | `C1` | Through hollow, cover-side clamp | — |
| | `C2` | Through hollow, cover-side + isolation | **Only bore < ø14mm** |
| Mounting ≤15mm `e` | `15` | Spring element long | — |
| | `25` | Stator coupling ø63mm | — |
| | `35` | Stator coupling ø65mm | — |
| | `45` | Torque stop | — |
| Mounting >15mm `e` | `55` | Torque stop | — |
| | `65` | Stator coupling | — |
| | `75` | Spring element long | — |
| Bore ≤15mm `f` | `06` | 6mm | — |
| | `08` | 8mm | — |
| | `10` | 10mm | — |
| | `12` | 12mm | — |
| | `14` | 14mm | **H1 and C1 only** |
| | `15` | 15mm | **H1 and C1 only** |
| | `1A` | 6.35mm (¼") | — |
| | `2A` | 9.525mm (3/8") | — |
| | `3A` | 12.7mm (½") | — |
| Bore >15mm `f` | `16` | 16mm | — |
| | `20` | 20mm | — |
| | `22` | 22mm | **Not H2** |
| | `24` | 24mm | **H1 only** |
| | `25` | 25mm | **H1 only** |
| | `4A` | 15.875mm (5/8") | — |
| | `5A` | 19.05mm (3/4") | — |
| | `6A` | 22.23mm (7/8") | **H2 only** |
| | `7A` | 25.4mm (1") | **H1 only** |
| Position `h` | `R` | Radial | — |
| | `T` | Tangential | **H1/H2 only; i=1 (PVC) only; bore ≤15mm only** |

**Additional hollow-specific rules:**
- Bore ≤15mm → use mounting options `e` = 15, 25, 35, 45
- Bore >15mm → use mounting options `e` = 55, 65, 75
- `C2` only valid for bore < 14mm (so: 06, 08, 10, 12, 1A, 2A, 3A)
- Tangential `h=T`: only H1/H2; only cable PVC `i=1`; only bore ≤15mm
- All other shaft-version rules (k, i, l) same as shaft version

---

## Datasheet 4: A020 (`A020_en.pdf`)

Large hollow shaft, optical, through hollow.

### Specs
- IP: IP65 only
- Max speed: 3,000 rpm (3,500 short-term)
- Temperature: -40 °C … +70 °C (connector: -40°C; fixed cable: -30°C; moving: -20°C)
- Weight: ~700 g
- Shock: 1,000 m/s², 6 ms
- Vibration: 100 m/s², 10–2,000 Hz
- Sensing: Optical

### Order Code: `8.A020.{a}{b}{c}{d}.{e}`

| Param | Code | Description | Constraint |
|---|---|---|---|
| Flange `a` | `2` | Spring element short | — |
| | `3` | Spring element long | — |
| | `5` | Torque stop long | — |
| Bore `b` | `C` | 20mm | — |
| | `6` | 24mm | — |
| | `5` | 25mm | — |
| | `3` | 28mm | — |
| | `A` | 30mm | — |
| | `2` | 38mm | — |
| | `B` | 40mm | — |
| | `1` | 42mm | — |
| | `4` | 1" (25.4mm) | — |
| Output/supply `c` | `1` | RS422 with inv / 5V | — |
| | `4` | RS422 with inv / 10–30V | — |
| | `2` | Push-pull no-inv / 10–30V | — |
| | `5` | Push-pull with inv / 5–30V | — |
| | `3` | Push-pull with inv / 10–30V | — |
| | `A` | PP 7272 / 5–30V | — |
| | `8` | SinCos 1Vpp / 5V | **PPR ≥ 1024 only** |
| | `9` | SinCos 1Vpp / 10–30V | **PPR ≥ 1024 only** |
| Connection `d` | `1` | Radial cable 1m PVC | — |
| | `A` | Radial cable special PVC | **1 rep row** |
| | `2` | Radial M23 12-pin | — |
| | `E` | Radial M12 8-pin | — |
| PPR `e` | 12 values | 50, 360, 512, 600, 1000, 1024, 1500, 2000, 2048, 2500, 4096, 5000 | SinCos needs ≥1024 |

**Constraints:**
- SinCos (c=8,9): only with PPR `e` ∈ {1024, 1500, 2000, 2048, 2500, 4096, 5000}
- Special cable A: 1 representative row only
- No Ex 2/22 in standard listing

---

## Datasheet 4 (continued): A02H (`A02H_en.pdf`)

Heavy-duty large hollow shaft, optical.

### Specs
- IP: IP65 (IP40 for Sub-D connection G)
- Max speed: 3,000 rpm
- Temperature: -40 °C … +70 °C
- Sensing: Optical

### Order Code: `8.A02H.{a}{b}{c}{d}.{e}`

| Param | Code | Description | Constraint |
|---|---|---|---|
| Flange `a` | `1` | Without mounting aid | — |
| | `2` | Spring element short | — |
| | `3` | Spring element long | — |
| | `5` | Torque stop long | — |
| | `6` | Torque stop short | **US only**¹ |
| Bore `b` | `C` | 20mm | — |
| | `6` | 24mm | — |
| | `5` | 25mm | — |
| | `3` | 28mm | — |
| | `A` | 30mm | — |
| | `H` | 35mm | — |
| | `2` | 38mm | — |
| | `B` | 40mm | — |
| | `1` | 42mm | — |
| | `4` | 1" (25.4mm) | — |
| | `D` | ½" | **US only**¹ |
| | `E` | ⅝" | **US only**¹ |
| | `F` | ¾" | **US only**¹ |
| | `G` | 1⅛" | **US only**¹ |
| | `N` | 1¼" | **US only**¹ |
| Output `c` | `1` | RS422 / 5V | — |
| | `4` | RS422 / 10–30V | — |
| | `5` | Push-pull / 5–30V | — |
| | `3` | Push-pull / 10–30V | — |
| | `8` | SinCos / 5V | **PPR ≥ 1024** |
| | `9` | SinCos / 10–30V | **PPR ≥ 1024** |
| | `A` | PP 7272 / 5–30V | — |
| | `D` | RS422 / 5–30V | **US only**¹ |
| Connection `d` | `1` | Radial cable 1m PVC | — |
| | `A` | Radial cable special PVC | **1 rep row** |
| | `2` | Radial M23 12-pin | — |
| | `E` | Radial M12 8-pin | — |
| | `G` | Sub-D 9-pin double-row | **IP40 only**² |
| | `R` | Radial M12 5-pin | **No inv, not SinCos**⁴ |
| | `K` | MIL 7-pin | **US only**¹, **no inv, not SinCos**⁴ |
| | `D` | MIL 10-pin | **US only**¹ |
| PPR `e` | 12 values | Same as A020 | SinCos needs ≥1024 |

**Constraint footnotes:**
- ¹ US only — include but flag `is_us_variant=True`
- ² Connection G = IP40, not IP65
- ⁴ Connections R, K = without inverted signal; cannot combine with SinCos (c=8,9)
- SinCos (c=8,9): only PPR ≥ 1024
- Ex 2/22: only PUR cable (on request) — excluded
