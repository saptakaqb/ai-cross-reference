# Per-Circuit Electrical Characteristics — All Datasheets
# Extracted 2026-04-14 from Kübler datasheets

## KIS40 / KIH40 (KIS40KIH40_en.pdf)

| Circuit | Order code c | Supply V | Power (typ/max) | Load/ch | Pulse freq | SIG HIGH | Rise/Fall | Rev pol |
|---|---|---|---|---|---|---|---|---|
| RS422 (TTL) | 6 (5V) / C (5-30V) | 5V or 5-30V | 40mA/90-165mA | ±20mA | 250kHz | min 2.5V | 200ns | no(5V)/yes(5-30V) |
| Push-pull 7272 | 4/8 (10-30V) / B (5-30V) | 10-30V or 5-30V | 50mA/100mA | ±20mA | 250kHz | min +V-2.0V | 1µs | yes |
| Open Collector NPN | 3/7 (10-30V) / A (5-30V) | 10-30V or 5-30V | 100mA/— | 20mA sink | 250kHz | — | — | yes |

## 5000 / 5020 (50005020_en.pdf)

| Circuit | Order code c | Supply V | Power typ/max | Load/ch | Pulse freq | SIG HIGH | Rise/Fall | Rev pol |
|---|---|---|---|---|---|---|---|---|
| RS422 TTL (7272 compat) | 1 | 5-30V DC | 40mA/90mA | ±20mA | 300kHz | min 2.5V | 200ns | YES |
| RS422 TTL | 4 | 5V DC ±5% | 40mA/90mA | ±20mA | 300kHz | min 2.5V | 200ns | NO |
| Push-pull HTL/TTL 7272 | 2 | 5-30V DC | 50mA/100mA | ±20mA | 300kHz | min +V-2.0V | 1µs | NO |
| Push-pull | 5 | 10-30V DC | 50mA/100mA | ±20mA | 300kHz | min +V-1.0V | 1µs | YES |
| Push-pull no-cap | 8 | 5-30V DC | 50mA/100mA | ±20mA | 300kHz | min +V-2.0V | 1µs | NO |
| Open Collector | 3 | 5-30V DC | 100mA/— | 20mA sink@30V | 300kHz | — | — | NO |

Note: c=8 is "no CE". MIL connectors: shock=2500m/s², vibration=100m/s².
Temperature: connector=-40°C, cable fixed=-30°C, cable moving=-20°C.

## K58I shaft + hollow (K58I_en.pdf)

| Circuit | Order code a | Supply V | Power typ/max | Load/ch | Pulse freq | SIG HIGH | Rise/Fall | Rev pol |
|---|---|---|---|---|---|---|---|---|
| RS422 TTL | RS (c=2, 5-30V) | 5-30V DC | 40mA/90mA | ±20mA | 300kHz | min 2.5V | 200ns | YES |
| RS422 TTL | RS (c=1, 5V) | 5V DC ±5% | 40mA/90mA | ±20mA | 300kHz | min 2.5V | 200ns | NO |
| Push-pull HTL/TTL 7272 | PP (c=2, 5-30V) | 5-30V DC | 40mA/90mA | ±20mA | 300kHz | min 2.5V (+V-1V) | 200ns | YES |

Note: K58I has ONLY 2 interfaces (RS, PP). Supply c=1 only valid with RS.
Shock (shaft + hollow ≤15mm): 3000m/s², 6ms. Hollow >15mm: 2000m/s².
Vibration: 5-8.7Hz ±0.35mm; 8.7-200Hz 30m/s²; 200-2000Hz 300m/s².
Temperature: connector=-40°C/+85°C; PVC fixed=-30°C/+80°C; PVC flex=-5°C/+80°C; PUR fixed=-40°C/+80°C.

## K58I-PR shaft + hollow (K58IPR_en.pdf)

| Circuit | Order code a | Supply V | Power typ/max | Load/ch | Pulse freq | SIG HIGH | Rise/Fall | Rev pol |
|---|---|---|---|---|---|---|---|---|
| RS422 TTL | RS | 5-30V DC | 40mA/90mA | ±20mA | 300kHz | min 2.5V | 200ns | YES |
| Push-pull HTL/TTL | PP | 5-30V DC | 40mA/90mA | ±20mA | 300kHz | min 2.5V | 200ns | YES |

Note: K58I-PR has ONLY c=2 (5-30V) — both RS and PP use same supply.
BOTH circuits have reverse polarity protection (unlike K58I RS@5V).
Additional cable type: TPE cable (i=2) — full -40°C to +110°C range.
Temperature: connector=-40°C/+110°C; PVC fixed=-30°C/+80°C; PVC flex=-5°C/+80°C;
             TPE fixed=-40°C/+110°C; TPE flex=-25°C/+110°C;
             PUR fixed=-40°C/+80°C; PUR flex=-20°C/+80°C.
Shock (shaft + hollow ≤15mm): 3000m/s², 6ms. Hollow >15mm: 2000m/s².

## K80I (K80I_en.pdf)

| Circuit | Order code a | Supply V | Power typ/max | Load/ch | Pulse freq | SIG HIGH | Rise/Fall | Rev pol |
|---|---|---|---|---|---|---|---|---|
| RS422 TTL | RS (c=2, 5-30V) | 5-30V DC | 40mA/90mA | ±20mA | 300kHz | min 2.5V | 200ns | YES |
| RS422 TTL | RS (c=1, 5V) | 5V DC ±5% | 40mA/90mA | ±20mA | 300kHz | min 2.5V | 200ns | NO |
| Push-pull HTL/TTL 7272 | PP (c=2) | 5-30V DC | 40mA/90mA | ±20mA | 300kHz | min 2.5V | 200ns | YES |
| SinCos 1Vss | SC (c=1, 5V) | 5V DC ±5% | 65mA/110mA | — | 180kHz (-3dB) | 1Vss ±20% | — | NO |
| SinCos 1Vss | SC (c=2, 5-30V) | 5-30V DC | 65mA/110mA | — | 180kHz (-3dB) | 1Vss ±20% | — | YES |

Constraint: SC (SinCos) only with PPR=1024 (b=01024).
Constraint: SC not with M12-5pin (k=3) or MIL-7pin (k=D,H).
Max speed: IP65=5000rpm/2500cont; IP66/67=2400rpm/1200cont.
Shock: 2000m/s², 6ms. Vibration: 5-8.7Hz ±0.35mm; 8.7-200Hz 30m/s²; 200-2000Hz 150m/s².
Temperature: connector=-40°C/+85°C; PVC fixed=-30°C/+80°C; PVC flex=-5°C/+80°C; PUR fixed=-40°C/+80°C.

## K80I-PR (K80IPR_en.pdf)

| Circuit | Order code a | Supply V | Power typ/max | Load/ch | Rev pol |
|---|---|---|---|---|---|
| RS422 TTL | RS | 5-30V DC | 40mA/90mA | ±20mA | YES |
| Push-pull HTL/TTL 7272 | PP | 5-30V DC | 40mA/90mA | ±20mA | YES |

Note: K80I-PR has ONLY c=2 (5-30V). No SinCos. No 5V-only RS.
Max speed: IP65=5000rpm/2500cont; IP66/67=2400rpm/1200cont.
Shock: 2000m/s², 6ms. Weight: 0.8kg.

## A020 (A020_en.pdf)

Note: A020 datasheet only shows SinCos electrical table.
For RS422 / Push-pull circuits, use standard specs (no explicit table).

| Circuit | Order code c | Supply V | Power typ/max | Load/ch | Rev pol |
|---|---|---|---|---|---|
| RS422 w/inv | 1 | 5V DC | standard | ±20mA | no |
| RS422 w/inv | 4 | 10-30V DC | standard | ±20mA | yes |
| Push-pull no-inv | 2 | 10-30V DC | standard | ±20mA | yes |
| Push-pull w/inv | 5 | 5-30V DC | standard | ±20mA | yes |
| Push-pull w/inv | 3 | 10-30V DC | standard | ±20mA | yes |
| Push-pull 7272 | A | 5-30V DC | standard | ±20mA | yes |
| SinCos 1Vpp | 8 | 5V DC ±5% | 65mA/110mA | — | NO |
| SinCos 1Vpp | 9 | 10-30V DC | 65mA/110mA | — | YES |

SinCos only ≥1024 PPR. Max speed 3000rpm (3500 short-term). IP65 only.

## A02H (A02H_en.pdf)

Notably different: Push-pull has HIGHER load capacity (±30mA) and different voltage levels.

| Circuit | Order code c | Supply V | Power typ/max | Load/ch | SIG HIGH | Rise/Fall | Rev pol |
|---|---|---|---|---|---|---|---|
| RS422 TTL | 1 | 5V DC ±5% | 40mA/90mA | ±20mA | min 2.5V | 200ns | NO |
| Push-pull (w/inv) | 5 | 5-30V DC | 80mA/150mA | ±30mA | min +V-3V | 1µs | YES |
| Push-pull (w/inv) | 3 | 10-30V DC | 80mA/150mA | ±30mA | min +V-3V | 1µs | NO(10-30V)/YES |
| Push-pull 7272 | A | 5-30V DC | 50mA/100mA | ±20mA | min +V-2V | 1µs | NO |
| Push-pull (no-inv) RS422-like | 2→ no inv | see c=5 | | | | | |
| SinCos 1Vpp | 8 | 5V DC ±5% | 65mA/110mA | — | 1Vpp ±20% | — | NO |
| SinCos 1Vpp | 9 | 10-30V DC | 65mA/110mA | — | 1Vpp ±20% | — | YES |
| RS422 5-30V | D (US) | 5-30V DC | 40mA/90mA | ±20mA | min 2.5V | 200ns | YES |

SinCos only ≥1024 PPR. Max speed 6000rpm (2500 at 60°C). IP65 only. Weight 0.8kg.
Connections R,K: without inverted signal, cannot combine with SinCos.
