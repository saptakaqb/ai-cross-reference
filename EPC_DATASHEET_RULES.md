# EPC Encoder Datasheet Rules Reference
**Generated:** 2026-04-14  
**Source:** Encoder Products Company (EPC) Accu-Coder® datasheets  
**Families covered:** 858S, 802S, 755A (shaft), 755A (hollow bore), 776, 260  
**Selection rationale:** Chosen to mirror Kübler's portfolio — 58mm solid (858S≈5000/K58I), 
50.8mm solid (802S≈5000), 38mm solid (755A≈KIS40), 38mm hollow (755A_HB≈KIH40/K58I_hollow), 
large thru-bore (776≈K80I/A020), commutated hollow (260≈A020/A02H)

---

## General EPC Rules (all families)

1. **CPR (Cycles Per Revolution)** = PPR. EPC uses CPR; map to resolution_ppr in schema.
2. **High temperature option (H)** limits max CPR — many resolutions marked 'a' in CPR table are limited to 85°C max (not 100°C). Contact-CS-only CPRs marked '*' are excluded from standard generation.
3. **Low temperature option (L)**: not available for CPR ≥ 3000.
4. **8-28V output types (H5, P5)**: only available for standard temperature (S), CPR 60-3000 only, not with CE option, not with 2400/2540/2880 CPR.
5. **M12 connectors**: available on side mount (S) only, not end mount (E).
6. **Line Driver (HV, H5)**: not available with 5-pin M12 connector.
7. **Non-standard cable lengths** (e.g. G/6 = 6 ft): treated as one representative row — not enumerated per length.
8. **CE option**: not available with H5/P5 output types.
9. **Max frequency code** is a separate ordering parameter — generates distinct rows per frequency class.
10. **Channels**: A (single), Q (quad), R (quad+index), K (reverse quad), D (reverse quad+index) — all are valid for all families unless noted.
11. **Commutation**: only on Model 260; not available with M12 connector or voltage temp option V.
12. **IP sealing**: IP50 standard (no seal); IP64/IP66/IP67 optional (price adder). Sealing code is part of the order string.

---

## Model 858S — 58mm Stainless Steel Solid Shaft

**Comparable to:** Kübler 5000 (shaft), K58I shaft  
**Housing:** Ø58mm (European Size 58), stainless steel 316  
**Shaft type:** Solid (through shaft)

### Order Code: `858S - [MOUNT] [TEMP] [CPR] [SHAFT] [CHANNELS] [OUTPUT] [FREQ] [SEAL] [CONNLOC] [CONNTYPE] [CERT]`

| Parameter | Code | Description | Constraint |
|---|---|---|---|
| Mounting | A | 20-type clamping flange | Shaft has 15.58mm flat |
| | B | 26-type synchro flange | Shaft without flat |
| Temperature | S | 0°C to 70°C | Standard |
| | L | -40°C to 70°C | Not with CPR ≥ 3000 |
| | H | 0°C to 100°C | Some CPRs limited to 85°C |
| Shaft size | 07 | 0.250" (6.35mm) | — |
| | 20 | 0.375" (9.525mm) | — |
| | 06 | 6mm | — |
| | 21 | 10mm | — |
| Channels | A | Channel A only | — |
| | (blank) | A leads B (quad) | — |
| | Q | Quad A & B | — |
| | R | Quad + Index | — |
| | K | Reverse Quad A & B | — |
| | D | Reverse Quad + Index | — |
| Output | OC | Open Collector | 5-28V, 100mA/ch |
| | PU | Pull-Up (OC + 2.2kΩ) | 5-28V, 100mA/ch |
| | PP | Push-Pull | 5-28V, 20mA/ch |
| | HV | Line Driver (RS422) | 5-28V in / 5V out or 5-28V; not with 5-pin M12 |
| | H5 | Line Driver (5V supply) | 8-28V in/5V out; temp=S only; CPR 60-3000; not CE |
| | P5 | Push-Pull (5V out) | 8-28V in/5V out; temp=S only; CPR 60-3000; not CE |
| Max freq | 1 | 100 kHz (standard) | — |
| | 2 | 200 kHz | CPR ≤ 3000 |
| | 5 | 250 kHz | CPR > 3000 |
| | 3 | 500 kHz | CPR > 6000; standard cable lengths only |
| | 4 | 1 MHz | CPR > 10,000; standard cable lengths only |
| Sealing | N | IP50 (no seal, standard) | — |
| | 1 | IP66 | — |
| | 2 | IP64 | — |
| | 5 | IP67 | — |
| Conn location | E | End (axial) | M12 NOT available with end mount |
| | S | Side (radial) | M12 available |
| Connector | J | 5-pin M12 | Side only; not with HV/H5 |
| | G | Gland, 24" cable | One rep row per cable type |
| | K | 8-pin M12 standard wiring | Side only |
| | Z | 8-pin M12 optional wiring | Side only |
| Cert | N | None | — |
| | CE | CE marked | Not with H5/P5 |

### 858S CPR List (excluding '*' contact-only and 'a' high-temp-limited)
Standard temperature (S): 125, 200, 250, 300, 360, 400, 500, 512, 600, 625, 635, 665, 720, 889, 1000, 1024, 1200, 1204, 1250, 1270, 1440, 1500, 1800, 2000, 2048, 2400, 2500, 2540, 2880, 3000, 3600, 4000, 4096, 5000, 6000, 7200, 7500, 9000, 10000, 10240, 12000, 12500, 14400, 15000, 18000, 20000, 20480, 25000, 30000

For ETL generation — use standard subset (CPR ≤ 5000) to keep comparable to Kübler range:
**858S_CPR** = [100, 200, 250, 300, 360, 400, 500, 512, 600, 625, 720, 1000, 1024, 1200, 1250, 1270, 1440, 1500, 1800, 2000, 2048, 2400, 2500, 3000, 3600, 4000, 4096, 5000]

### 858S Specifications
- Input voltage: 4.75–28V DC (4.75–24V for 70–100°C)
- Input current: 100mA max no load
- Max shaft speed: 8000 RPM
- Shaft load: 80 lb radial and axial (355N)
- Starting torque: 1.0 oz-in IP64; 3.0 oz-in IP66; 7.0 oz-in IP67
- Weight: 1.5 lb (680g)
- Shock: 75g @ 11ms
- Vibration: 20g @ 58–500Hz
- Housing: 316 stainless steel

**Cross-parameter rules:**
- H5/P5: temp=S only, CPR 60–3000 only, not CE
- HV/H5: not with 5-pin M12 (J)
- L (low temp): not with CPR ≥ 3000
- M12 connectors (J,K,Z): side mount (S) only
- Freq 3 (500kHz): CPR > 6000 only
- Freq 4 (1MHz): CPR > 10,000 only
- CE: not with H5, P5

---

## Model 802S — 2.0" (50.8mm) Stainless Steel Solid Shaft

**Comparable to:** Kübler 5000, K58I shaft (slightly smaller housing)  
**Housing:** Ø2.0" (50.8mm), stainless steel 316  
**Shaft type:** Solid

### Order Code: `802S - [TEMP] [SHAFT] [CPR] [CHANNELS] [OUTPUT] [FREQ] [MOUNT] [SEAL] [CONNLOC] [CONNTYPE] [CERT]`

| Parameter | Code | Description | Constraint |
|---|---|---|---|
| Temperature | S | 0°C to 70°C | — |
| | L | -40°C to 70°C | Not with CPR ≥ 3000 |
| | H | 0°C to 100°C | Some CPRs 85°C max |
| Shaft | 07 | 0.250" (6.35mm) | — |
| | 20 | 0.375" (9.525mm) | — |
| | 21 | 10mm | — |
| | 30 | 0.375" (9.525mm) w/ Size25 adapter | J or K mount only |
| Channels | same as 858S | | |
| Output | OC,PU,PP,HV,H5,P5 | same as 858S | same constraints |
| Max freq | 1,2,5,3,4 | same as 858S | same CPR constraints |
| Mounting | F | Flange 1.181" female pilot | — |
| | L | Flange 0.687" male pilot | — |
| | G | Flange 1.250" male pilot | — |
| | K | Flange Size25 w/30 shaft | Shaft 30 only |
| | S | Servo 1.181" female pilot | — |
| | U | Servo 0.687" male pilot | — |
| | T | Servo 1.250" male pilot | — |
| | J | Servo Size25 w/30 shaft | Shaft 30 only |
| Sealing | N,1,2,5 | same as 858S | — |
| Conn location | E,S | same as 858S | — |
| Connector | G,J,K,Z | same as 858S | same constraints |
| Cert | N,CE | same as 858S | — |

### 802S CPR: same list as 858S
### 802S Specifications: same as 858S (identical electrical + mechanical)

**Additional cross-parameter rules:**
- Shaft 30 (Size25 adapter): only with J or K mounting
- K mounting only available with shaft 30

---

## Model 755A — 1.5" (38mm) Solid Shaft (Servo/Flange Mount)

**Comparable to:** Kübler KIS40 (small solid shaft)  
**Housing:** Ø1.5" (38.1mm)  
**Shaft type:** Solid

### Order Code: `755A - [SHAFT] [TEMP] [CPR] [CHANNELS] [OUTPUT] [FREQ] [MOUNT] [CONNTYPE] [CERT]`

| Parameter | Code | Description | Constraint |
|---|---|---|---|
| Shaft | 07 | 1/4" (6.35mm) | — |
| | 08 | 5mm | — |
| | 06 | 6mm | — |
| | 32 | 1/4" extended (6.35×0.500") | Servo S1/S2/S3 only |
| | 20 | 6mm × 0.500" | — |
| | 19 | 1/4" × 0.500" | — |
| Temperature | S | 0°C to 70°C | — |
| | L | -40°C to 70°C | Not with CPR ≥ 3000 |
| | H | 0°C to 100°C | Some CPRs limited to 85°C |
| Channels | A,Q,R,K,D | same as 858S | — |
| Output | OC,PU,PP,HV,H5,P5 | same constraints | — |
| Max freq | 1,2,5,3,4 | same as 858S | — |
| Mount | S | Standard servo (1.570" BC) | — |
| | MF | Square flange 1.575" | — |
| | S1 | Servo 0.547" boss | Shaft 32 only |
| | S2 | Servo 0.750" boss | Shaft 32 only |
| | S3 | Servo alternative | Shaft 32 only |
| Connector | S | 18" standard cable | One rep row (non-standard lengths: S/6, S/10 etc → not enumerated) |
| | C01 | 8-pin Molex | — |
| | C02 | Terminal block | — |
| | J00 | 18" cable + 5-pin M12 | Not with HV/H5 |
| | K00 | 18" cable + 8-pin M12 | — |
| Cert | N,CE | — | CE not with H5/P5 |

### 755A CPR: same large list as 858S
### 755A Specifications
- Input voltage: 4.75–28V DC (4.75–24V for 70–100°C)
- Max shaft speed: 7500 RPM
- Shaft load: radial 5 lb (22N), axial 3 lb (13N)
- Weight: 3.10 oz (88g) servo mount
- Shock: 50g @ 11ms
- Vibration: 10g @ 58–500Hz
- No IP sealing (standard); no IP option listed

**Cross-parameter rules:**
- S1/S2/S3 mount: only with shaft 32
- H5/P5: temp=S, CPR 60–3000, not CE
- J00 (5-pin M12): not with HV/H5
- L: not with CPR ≥ 3000

---

## Model 755A Hollow Bore — 1.5" Blind Hollow Bore

**Comparable to:** Kübler KIH40 (small hollow), K58I_hollow (small bores)  
**Housing:** Ø1.5" (38.1mm)  
**Shaft type:** Blind hollow bore (not through-bore)

### Order Code: `755A - [BORE] [TEMP] [CPR] [CHANNELS] [OUTPUT] [FREQ] [MOUNT] [CONNTYPE] [CERT]`

| Parameter | Code | Description | Constraint |
|---|---|---|---|
| Bore | 15 | 3/16" (4.76mm) | — |
| | 16 | 4mm | — |
| | 01 | 1/4" (6.35mm) | — |
| | 18 | 5mm | — |
| | 03 | 5/16" (7.94mm) | — |
| | 04 | 6mm | — |
| | 02 | 3/8" (9.525mm) | — |
| | 14 | 8mm | Large bore flex (deeper bore depth 0.750") |
| | 10 | 1/2" (12.7mm) | Large bore flex |
| | 05 | 10mm | Large bore flex |
| | 11 | 5/8" (15.875mm) | Large bore flex |
| | 12 | 12mm | Large bore flex |
| | 17 | 3/4" (19.05mm) | Large bore flex |
| | 13 | 14mm | Large bore flex |
| Temperature | L,S,H | same as 755A shaft | — |
| Channels | A,Q,R,K,D | — | — |
| Output | OC,PU,PP,HV,H5,P5 | same constraints | — |
| Max freq | 1,2,5,3,4 | same as 858S | — |
| Mount | S | Standard flex mount | — |
| | SF | Slotted flex mount (rotational adj.) | — |
| Connector | S,C01,C02,J00,K00 | same as 755A shaft | — |
| Cert | N,CE | — | — |

### 755A Hollow Bore CPR: same list (slightly different table but same values)
### 755A Hollow Bore Specifications: same electrical as shaft version
- Weight: 3.50 oz (99g) typical
- Bore tolerance: -0.0000" / +0.0006"
- User shaft radial runout: 0.007" max
- User shaft axial endplay: ±0.030" max
- No IP sealing listed as standard

---

## Model 776 — 4.3" Large Thru-Bore

**Comparable to:** Kübler K80I, A020 (large hollow shaft)  
**Housing:** Ø4.3" (109mm) — significantly larger  
**Shaft type:** Through-bore only  
**Important:** CPR limited to 4096 max (vs 30,000 for 858S)

### Order Code: `776 - [HOUSING] [TEMP] [CPR] [CHANNELS] [OUTPUT] [BORE] [FLEXMOUNT] [MATINGCONN] [CONNTYPE] [CERT]`

| Parameter | Code | Description | Constraint |
|---|---|---|---|
| Housing style | A | Enclosed (no shaft access) | — |
| | B | Thru-bore (shaft accessible) | — |
| Temperature | S | 0°C to 70°C | 5–28V max |
| | H | 0°C to 100°C | 5–24V max |
| Channels | Q,R,K,D | standard | No single-channel A listed |
| Output | OC | Open Collector | 100mA/ch |
| | PU | Pull-Up (OC+2.2kΩ) | 100mA/ch |
| | PP | Push-Pull | 20mA/ch |
| | HV | Line Driver (RS422) | NOT with 5-pin M12 or 6-pin MS; only 7-pin MS |
| Bore | G | 1-7/16" (36.5mm) | — |
| | C | 1-1/2" (38.1mm) | — |
| | D | 1-5/8" (41.3mm) | — |
| | F | 1-3/4" (44.5mm) | — |
| | E | 1-7/8" (47.6mm) | — |
| | L | 35mm | — |
| | I | 38mm | — |
| | J | 40mm | — |
| | M | 42mm | — |
| | N | 43mm | — |
| Anti-rotation | N | None | — |
| | A | Style A flex mount | — |
| Mating conn | N | No mating connector | — |
| | Y | Yes (mating connector included) | — |
| Connector | P | Gland nut with 24" cable | — |
| | W | 6-pin MS3 | Not with HV |
| | Y | 7-pin MS3 | HV available with 7-pin MS (without index Z) |
| | X | 10-pin MS | — |
| | J | 5-pin M12 | NOT with HV |
| | K | 8-pin M12 | Requires extended housing |
| | 9D | 9-pin D-sub | Standard housing |
| Cert | N,CE | — | — |

### 776 CPR List (limited to 4096 max)
**776_CPR** = [60, 100, 120, 240, 250, 256, 500, 512, 1000, 1024, 2048, 2500, 4096]

Note: Price adder for CPR > 1024. No high-frequency or high-CPR options like 858S.

### 776 Specifications
- Input voltage: 4.75–28V DC (4.75–24V for 70–100°C)
- Input current: 100mA max no load
- Max freq: 200 kHz (fixed, no frequency code selection)
- Max shaft speed: 3500 RPM
- Shock: 50g @ 11ms
- Vibration: 10g @ 58–500Hz
- Sealing: IP50 standard (no seal option listed beyond this)
- Weight: 1.0 lb cable/D-sub; 1.5 lb MS connector option
- Moment of inertia: 3.3×10⁻³ oz-in-sec²

**Cross-parameter rules:**
- HV: NOT with 5-pin M12 (J), NOT with 6-pin MS (W); only 7-pin MS (Y) but without index Z
- MS connectors (W,X,Y), M12 (J,K): require extended housing (different depth)
- Temperature H: max 24V DC
- NO frequency code (fixed 200kHz — no user selection)
- NO seal options beyond IP50 listed

---

## Model 260 — 2.0" Hollow Bore + Thru-Bore (with optional commutation)

**Comparable to:** Kübler A020 (large hollow), 5020 (thru-bore)  
**Housing:** Ø2.0" (50.8mm)  
**Shaft type:** Blind hollow bore (B), front-clamp thru-bore (T), rear-clamp thru-bore (R)  
**Special:** Optional commutation tracks (4/6/8/10/12 pole) for brushless servo motors

### Order Code: `260 - [COMMUTATION] [HOUSING] [TEMP] [BORE] [CHANNELS] [CPR] [OUTPUT] [FREQ] [SEALING] [MOUNT] [CONNTYPE] [CERT]`

| Parameter | Code | Description | Constraint |
|---|---|---|---|
| Commutation | N | None | — |
| | C4 | 4-pole | — |
| | C6 | 6-pole | — |
| | C8 | 8-pole | — |
| | C10 | 10-pole | — |
| | C12 | 12-pole | — |
| Housing style | B | Blind hollow bore | — |
| | T | Front-clamp thru-bore | — |
| | R | Rear-clamp thru-bore | — |
| Temperature | L | -40°C to 70°C | 4.75–28V supply |
| | S | 0°C to 70°C | 4.75–28V supply |
| | H | 0°C to 100°C | 5–16V supply only |
| | V | 0°C to 120°C | 5V supply only; contact CS for availability |
| Bore | 01 | 1/4" (6.35mm) | — |
| | 02 | 3/8" (9.525mm) | — |
| | 76 | 7/16" (11.11mm) | — |
| | 10 | 1/2" (12.7mm) | — |
| | 11 | 5/8" (15.875mm) | — |
| | 06 | 5mm | — |
| | 04 | 6mm | — |
| | 14 | 8mm | — |
| | 05 | 10mm | — |
| | 09 | 11mm | — |
| | 12 | 12mm | — |
| | 13 | 14mm | — |
| | 15 | 15mm | — |
| Channels | Q,R,K,D | standard | — |
| Output | OC | Open Collector | 20mA/ch (lower than 858S — 20mA not 100mA!) |
| | PP | Push-Pull | 20mA/ch |
| | HV | Line Driver (RS422) | Not with 5-pin body-mount M12 |
| | OD | Open Collector Differential | — |
| Freq | 1 | Standard (200kHz ≤2540 CPR; 500kHz ≤5000; 1MHz ≤10k) | — |
| | 2 | Extended (300kHz for CPR 2000/2048/2500/2540) | — |
| Sealing | 1 | IP50 thru-bore | — |
| | 2 | IP64 thru-bore | — |
| | 3 | IP64 hollow bore | — |
| | 4 | IP50 hollow bore | — |
| Mount | SD | 1.575" (40mm) BC flex | — |
| | SF | 1.811" (46mm) BC slotted flex | — |
| | SL | 2.36" (60mm) BC flex | — |
| | XF | 2.250" BC 3-point flex | — |
| | NF | 2.375" BC 3-point flex | — |
| | FA | 1.06"–1.81" flex arm | — |
| | FB | 1.50"–3.13" flex arm | — |
| Connector | S | 18" cable | One rep row |
| | J00 | 18" cable + 5-pin M12 | Not with commutation; not with V temp |
| | K00 | 18" cable + 8-pin M12 | Not with commutation; not with V temp |
| | SMJ | 5-pin body-mount M12 | Not with commutation; not with HV |
| | SMK | 8-pin body-mount M12 | Not with commutation |
| | SMZ | 8-pin body-mount M12 optional wiring | Not with commutation |
| | SMH | 10-pin body-mount bayonet | Not with commutation |
| Cert | N,CE | — | — |

### 260 CPR List
**260_CPR** = [200, 250, 254, 256, 300, 360, 400, 500, 512, 600, 720, 800, 1000, 1024, 1200, 1250, 1270, 1500, 1800, 2000, 2048, 2500, 2540, 3000, 3600, 4000, 4096, 5000, 6000, 7200, 8192, 10000]

### 260 Specifications
- Input voltage: 4.75–28V (standard/L); 5–16V (H); 5V (V)
- Input current: 130mA max (< 100mA typical)
- Output: OC 20mA/ch (not 100mA like 858S — important difference)
- Max freq: 200kHz (CPR 1–2540), 500kHz (2541–5000), 1MHz (5001–10000)
- Max shaft speed: 7500 RPM
- Bore tolerance: -0.0000"/+0.0006"
- Starting torque: 0.50 oz-in IP50 thru; 0.30 oz-in IP50 hollow; 2.50 oz-in IP64 thru; 2.0 oz-in IP64 hollow
- Moment of inertia: 3.9×10⁻⁴ oz-in-sec²
- Weight: 3.5 oz (99g) typical
- Shock: 50g @ 11ms
- Vibration: 10g @ 58–500Hz

**Cross-parameter rules:**
- Commutation (C4–C12): NOT with M12 connectors (J00, K00, SMJ, SMK, SMZ, SMH), NOT with V temp
- Temperature V: 5V supply only; contact CS — NOT generated in standard ETL
- Temperature H: 5–16V supply
- HV: NOT with SMJ (5-pin body-mount M12)
- L temp: 4.75–28V
- OD output: has differential outputs (A, A', B, B', Z, Z')

---

## Output Type Electrical Specs (all EPC families)

| Output | Input V | Supply current | Load/ch | Signal HIGH | Signal LOW | Notes |
|---|---|---|---|---|---|---|
| OC (Open Collector) | 4.75–28V | 100mA (260: <100mA) | 100mA sink (260: 20mA) | — | < 0.4V | External pull-up needed |
| PU (Pull-Up OC) | 4.75–28V | 100mA | 100mA | — | < 0.4V | Internal 2.2kΩ pull-up |
| PP (Push-Pull) | 4.75–28V | 100mA | 20mA | ~+V | < 0.4V | HTL/TTL universal |
| HV (Line Driver) | 4.75–28V in / 5V RS422 out | 100mA | 20mA | min 2.5V | max 0.5V | RS422 at 5V supply; complement outputs A'/B'/Z' |
| H5 (LD 5V) | 8–28V in / 5V out | 100mA | 20mA | min 2.5V | max 0.5V | Only temp=S, CPR 60–3000 |
| P5 (PP 5V) | 8–28V in / 5V out | 100mA | 20mA | min 2.5V | max 0.5V | Only temp=S, CPR 60–3000 |

**Canonical mapping to schema:**
- OC → "Open Collector"
- PU → "Open Collector" (with internal pull-up — note in connection_type)
- PP → "Push-Pull"
- HV → "TTL RS422" (Line Driver = RS422 compatible at 5V supply)
- H5 → "TTL RS422" (5V line driver)
- P5 → "Push-Pull" (5V version)
