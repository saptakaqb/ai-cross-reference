# AQB Encoder Cross-Reference API

REST API wrapping the encoder cross-reference matching engine.  
Returns top-N Kübler matches for any competitor encoder part number, with scoring breakdown and Claude AI explanation.

---

## Setup

```bash
# 1. Copy api.py and requirements_api.txt into the v15 project root (alongside matcher.py)
cp api.py requirements_api.txt /path/to/v15_build_duckdb/

# 2. Install dependencies
pip install -r requirements_api.txt

# 3. Ensure encoder_crossref.duckdb exists (or competitor_unified.csv for auto-build)
#    The DB loads automatically from data/encoder_crossref.duckdb on startup

# 4. Run
uvicorn api:app --host 0.0.0.0 --port 8000

# Or with auto-reload for development
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

---

## Authentication

All `/v1/*` endpoints require the `X-API-Key` header.

Default keys (change in production via `ENCODER_API_KEYS` env var):
- `aqb-dev-key-001`
- `aqb-prod-key-001`

**Production:** Set env var `ENCODER_API_KEYS=key1,key2,key3` to define your own keys.

---

## Endpoints

### `GET /health`
Liveness check. No auth required.

```bash
curl http://localhost:8000/health
```
```json
{
  "status": "ok",
  "db_connected": true,
  "db_rows": 732612,
  "model": "claude-sonnet-4-20250514"
}
```

---

### `GET /v1/manufacturers`
List all manufacturers in the DB with row counts.

```bash
curl http://localhost:8000/v1/manufacturers \
     -H "X-API-Key: aqb-dev-key-001"
```

---

### `POST /v1/match`
**Core endpoint.** Cross-reference a part number and return top N matches.

#### Request body

| Field | Type | Default | Description |
|---|---|---|---|
| `part_number` | string | required | Competitor part number to look up |
| `top_n` | int | 5 | Number of matches to return (1–20) |
| `source_manufacturer` | string | null | Narrow source lookup to one manufacturer |
| `target_manufacturer` | string | null | Filter results to one target manufacturer |
| `weights` | object | null | Custom scoring weights (see below) |
| `include_ai_explanation` | bool | true | Generate Claude AI narrative per match |

#### Minimal request

```bash
curl -X POST http://localhost:8000/v1/match \
     -H "X-API-Key: aqb-dev-key-001" \
     -H "Content-Type: application/json" \
     -d '{"part_number": "DFS60B-S4PA10000"}'
```

#### Full request with options

```bash
curl -X POST http://localhost:8000/v1/match \
     -H "X-API-Key: aqb-dev-key-001" \
     -H "Content-Type: application/json" \
     -d '{
       "part_number": "DFS60B-S4PA10000",
       "top_n": 3,
       "source_manufacturer": "Sick",
       "target_manufacturer": "Kubler",
       "include_ai_explanation": true,
       "weights": {
         "resolution_ppr": 0.30,
         "output_circuit_canonical": 0.14,
         "housing_diameter_mm": 0.14,
         "shaft_diameter_mm": 0.12,
         "ip_rating": 0.10,
         "supply_voltage_min_v": 0.04,
         "supply_voltage_max_v": 0.04,
         "operating_temp_min_c": 0.03,
         "operating_temp_max_c": 0.03,
         "max_speed_rpm_peak": 0.04,
         "max_speed_rpm_cont": 0.02,
         "num_output_channels": 0.02,
         "flange_type": 0.02,
         "connection_type": 0.02,
         "connector_pins": 0.02
       }
     }'
```

#### Response structure

```json
{
  "part_number_queried": "DFS60B-S4PA10000",
  "source_manufacturer": "Sick",
  "target_manufacturer": "Kubler",
  "status_tier": "strong",
  "status_message": "Strong match — direct replacement candidate.",
  "fallback_used": false,
  "top_score": 0.9412,
  "db_rows": 732612,
  "query_ms": 1847,
  "matches": [
    {
      "rank": 1,
      "part_number": "8.K58I.1234.1024",
      "manufacturer": "Kubler",
      "product_family": "K58I",
      "match_score": 0.9412,
      "match_score_pct": "94.12%",
      "tier": "strong",
      "hard_stop": null,
      "caps": [],
      "ppr_badge": null,
      "field_scores": [
        {
          "label": "Resolution (PPR)",
          "source_val": "10000",
          "candidate_val": "10000",
          "similarity": 1.0,
          "points": 25.0,
          "note": "Exact match",
          "weight": 0.25
        },
        {
          "label": "Output Circuit",
          "source_val": "TTL RS422",
          "candidate_val": "TTL RS422",
          "similarity": 1.0,
          "points": 14.0,
          "note": "Exact match",
          "weight": 0.14
        }
      ],
      "structured_summary": "Strong match (94%). Kübler K58I is a direct replacement candidate.",
      "ai_explanation": "The Kübler K58I (8.K58I.1234.1024) is an excellent replacement for the Sick DFS60B-S4PA10000. Both encoders share identical PPR (10,000), output type (TTL RS422), housing diameter (58mm), and shaft diameter (10mm). The Kübler offers IP66/67 sealing versus the Sick's IP65, which is a minor improvement. Operating temperature ranges are equivalent at -40°C to +85°C. This is a direct drop-in replacement requiring no mechanical modifications."
    }
  ]
}
```

#### Status tiers

| Tier | Score | Meaning |
|---|---|---|
| `strong` | ≥ 90% | Direct replacement candidate |
| `good` | 80–89% | Minor differences, review before ordering |
| `weak` | < 80% | No close equivalent, engineering review needed |

#### Hard stops (score = 0, excluded)

- Shaft type mismatch: solid vs hollow
- Hollow bore diameter difference > 1mm  
- Voltage class cross: TTL/Low ↔ HTL/High (push-pull = universal, no hard stop)

---

## Custom Weights

Default weights (must sum to 1.0):

```json
{
  "resolution_ppr": 0.25,
  "output_circuit_canonical": 0.14,
  "housing_diameter_mm": 0.14,
  "shaft_diameter_mm": 0.12,
  "supply_voltage_min_v": 0.04,
  "supply_voltage_max_v": 0.04,
  "ip_rating": 0.07,
  "operating_temp_min_c": 0.03,
  "operating_temp_max_c": 0.03,
  "max_speed_rpm_peak": 0.04,
  "max_speed_rpm_cont": 0.02,
  "num_output_channels": 0.02,
  "flange_type": 0.02,
  "connection_type": 0.02,
  "connector_pins": 0.02
}
```

---

## Error Codes

| Code | Meaning |
|---|---|
| 401 | Missing or invalid `X-API-Key` header |
| 404 | Part number not found in database |
| 422 | Invalid request body (validation error) |
| 500 | Internal server error |
| 503 | Database or AI client not ready |

---

## EC2 Deployment

```bash
# Install screen or run with nohup
pip install -r requirements_api.txt
nohup uvicorn api:app --host 0.0.0.0 --port 8000 > api.log 2>&1 &

# Or with systemd service (recommended for production)
# Create /etc/systemd/system/encoder-api.service

# Set production API keys
export ENCODER_API_KEYS="prod-key-here,backup-key-here"
```

The API loads the full 700k+ row DataFrame into memory on startup (~2–4 GB RAM). Use at least a **t3.medium** (4 GB) instance.

---

## Performance

- Startup / DB load: ~15–30s (one-time)
- Typical query (with AI): ~1–3 seconds
- Typical query (without AI): ~200–500ms
- DB size in memory: ~1.5 GB for 732k rows

The matching engine deduplicates ~700k rows to ~12k unique parameter combos before scoring, giving ~33× speedup over naive scoring.

---

## Interactive Docs

Swagger UI: `http://localhost:8000/docs`  
ReDoc: `http://localhost:8000/redoc`
