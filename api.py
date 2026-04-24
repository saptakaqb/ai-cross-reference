"""
api.py — Encoder Cross-Reference REST API
==========================================
FastAPI server wrapping the existing matcher + AI explanation engine.

Endpoints:
    GET  /health       → liveness + DB status
    POST /v1/match     → cross-reference a part number

Auth: X-API-Key header required on all /v1/* endpoints.

Run:
    uvicorn api:app --host 0.0.0.0 --port 8000
"""

import os
import sys
import time
import logging
from typing import Optional, List

import anthropic
import pandas as pd
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_load import load_all_as_df, test_connection
from matcher import find_matches_with_status, DEFAULT_WEIGHTS
try:
    from config_claude import CLAUDE_API_KEY, MODEL
except ImportError:
    CLAUDE_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
    MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("encoder_api")

_ENV_KEYS = os.environ.get("ENCODER_API_KEYS", "")
API_KEYS = set(k.strip() for k in _ENV_KEYS.split(",") if k.strip()) or {
    "aqb-dev-key-001",
    "aqb-prod-key-001",
}

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# ─────────────────────────────────────────────────────────────────────────────
# App + startup
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="AQB Encoder Cross-Reference API",
    description="Cross-reference any industrial encoder part number to the best Kübler equivalent.",
    version="1.0.0",
)

_df: Optional[pd.DataFrame] = None
_claude: Optional[anthropic.Anthropic] = None


@app.on_event("startup")
async def startup():
    global _df, _claude
    log.info("Loading encoder database...")
    t0 = time.time()
    _df = load_all_as_df()
    log.info(f"DB loaded: {len(_df):,} rows in {time.time() - t0:.1f}s")
    _claude = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
    log.info("Claude client ready")


def get_df() -> pd.DataFrame:
    if _df is None:
        raise HTTPException(503, "Database not ready")
    return _df


def get_claude() -> anthropic.Anthropic:
    if _claude is None:
        raise HTTPException(503, "AI client not ready")
    return _claude


# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────
async def verify_api_key(api_key: str = Security(API_KEY_HEADER)):
    if not api_key or api_key not in API_KEYS:
        raise HTTPException(401, "Invalid or missing API key. Pass X-API-Key header.")
    return api_key


# ─────────────────────────────────────────────────────────────────────────────
# Request / Response models
# ─────────────────────────────────────────────────────────────────────────────
class MatchRequest(BaseModel):
    part_number: str = Field(
        ...,
        description="Competitor encoder part number to cross-reference",
        examples=["DFS60B-S4PA10000"]
    )
    num_matches: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Number of top matches to return (1-20)"
    )


class MatchResult(BaseModel):
    rank: int
    part_number: str
    manufacturer: str
    product_family: Optional[str]
    match_score: float
    match_score_pct: str
    tier: str
    ai_explanation: Optional[str]


class MatchResponse(BaseModel):
    part_number_queried: str
    status: str
    status_message: str
    matches: List[MatchResult]


# ─────────────────────────────────────────────────────────────────────────────
# AI explanation
# ─────────────────────────────────────────────────────────────────────────────
def _get_ai_explanation(
    claude: anthropic.Anthropic,
    source: dict,
    candidate: dict,
    explanation: dict,
) -> Optional[str]:
    src_pn  = source.get("part_number", "source encoder")
    src_mfr = source.get("manufacturer", "")
    cnd_pn  = candidate.get("part_number", "candidate encoder")
    cnd_mfr = candidate.get("manufacturer", "")
    score   = explanation.get("score_pct", "")
    tier    = explanation.get("tier", "")
    hard    = explanation.get("hard_stop")

    field_lines = []
    for f in explanation.get("fields", []):
        if f.get("sim") is not None:
            symbol = "check" if (f.get("sim") or 0) >= 0.9 else ("partial" if (f.get("sim") or 0) >= 0.5 else "mismatch")
            field_lines.append(
                f"  [{symbol}] {f['label']}: source={f.get('source_val','?')} candidate={f.get('cand_val','?')} "
                f"(similarity={f['sim']:.2f}, note={f.get('note', '')})"
            )

    prompt = f"""You are a technical encoder sales engineer at AQB Solutions.
A customer asked to cross-reference {src_mfr} {src_pn} to a Kubler equivalent.

The matching engine found {cnd_mfr} {cnd_pn} as a match with score {score} ({tier} tier).

Parameter comparison:
{chr(10).join(field_lines) if field_lines else "  (no breakdown available)"}
{"Hard stop: " + hard if hard else ""}

Write a concise technical explanation (3-5 sentences) covering:
1. Whether this is a suitable replacement and why
2. Which parameters match well and which differ
3. Any caveats before ordering

Be specific with values. Use metric units. No bullet points. Be direct."""

    try:
        response = claude.messages.create(
            model=MODEL,
            max_tokens=350,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.warning(f"Claude API call failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["System"])
async def health():
    db_ok = test_connection()
    return {
        "status": "ok" if db_ok else "degraded",
        "db_rows": len(_df) if _df is not None else 0,
    }


@app.post("/v1/match", response_model=MatchResponse, tags=["Match"],
          dependencies=[Depends(verify_api_key)])
async def match_part_number(
    req: MatchRequest,
    df: pd.DataFrame = Depends(get_df),
    claude: anthropic.Anthropic = Depends(get_claude),
):
    """
    Cross-reference a competitor encoder part number.

    Send **part_number** and **num_matches**. Get back ranked Kubler matches
    with scores and AI explanations.
    """
    t0 = time.time()

    results_df, status, explanations = find_matches_with_status(
        source_pn=req.part_number,
        df=df,
        top_n=req.num_matches,
        weights=DEFAULT_WEIGHTS,
    )

    if status["tier"] == "not_found":
        raise HTTPException(404, f"Part number '{req.part_number}' not found in the database.")

    # Look up source record for AI prompts
    pn_col = df["part_number"].astype(str).str.upper()
    mask   = pn_col == req.part_number.strip().upper()
    source_record = df[mask].iloc[0].to_dict() if mask.any() else {}

    # Build results
    matches = []
    for rank, (_, res_row) in enumerate(results_df.iterrows(), start=1):
        expl = explanations[rank - 1] if rank <= len(explanations) else {}

        cand_mask = (
            (df["part_number"].astype(str) == str(res_row["part_number"])) &
            (df["manufacturer"].astype(str) == str(res_row["manufacturer"]))
        )
        cand_record = df[cand_mask].iloc[0].to_dict() if cand_mask.any() else {}

        ai_text = None
        if source_record and cand_record:
            ai_text = _get_ai_explanation(claude, source_record, cand_record, expl)

        score = float(res_row.get("match_score", 0))
        matches.append(MatchResult(
            rank=rank,
            part_number=str(res_row.get("part_number", "")),
            manufacturer=str(res_row.get("manufacturer", "")),
            product_family=str(res_row.get("product_family", "")) or None,
            match_score=round(score, 4),
            match_score_pct=f"{score * 100:.1f}%",
            tier=expl.get("tier", ""),
            ai_explanation=ai_text,
        ))

    log.info(
        f"MATCH pn={req.part_number!r} n={req.num_matches} "
        f"tier={status['tier']} score={status['top_score']:.3f} "
        f"{int((time.time()-t0)*1000)}ms"
    )

    return MatchResponse(
        part_number_queried=req.part_number,
        status=status["tier"],
        status_message=status["message"],
        matches=matches,
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request, exc):
    log.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})