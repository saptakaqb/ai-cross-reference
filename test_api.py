"""
test_api.py — Encoder Cross-Reference API Test Script
======================================================
Usage:
    python test_api.py
    python test_api.py --part-number "DFS60B-S4PA10000"
    python test_api.py --part-number "DFS60B-S4PA10000" --num-matches 5
"""

import argparse
import json
import sys
import requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
API_URL = "http://localhost:8000"
API_KEY = "aqb-dev-key-001"

HEADERS = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json",
}

TIER_COLOR = {
    "strong":   "\033[92m",
    "good":     "\033[93m",
    "moderate": "\033[33m",
    "weak":     "\033[91m",
}
RESET = "\033[0m"
BOLD  = "\033[1m"

def c(text, tier):
    return f"{TIER_COLOR.get(tier, '')}{text}{RESET}"

def line(char="─", w=65):
    print(char * w)


# ─────────────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────────────
def check_health():
    print(f"\nChecking API at {API_URL} ...")
    try:
        r = requests.get(f"{API_URL}/health", timeout=5)
        d = r.json()
        ok = d.get("status") == "ok"
        print(f"{'OK' if ok else 'DEGRADED'}  —  DB rows: {d.get('db_rows', 0):,}")
        return ok
    except requests.exceptions.ConnectionError:
        print("Cannot reach the API. Make sure uvicorn is running:")
        print("  uvicorn api:app --host 0.0.0.0 --port 8000")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Match
# ─────────────────────────────────────────────────────────────────────────────
def run_match(part_number: str, num_matches: int = 3):
    print(f"\n{'='*65}")
    print(f"{BOLD}Cross-referencing: {part_number}{RESET}")
    print(f"{'='*65}")

    payload = {"part_number": part_number, "num_matches": num_matches}

    try:
        r = requests.post(
            f"{API_URL}/v1/match",
            headers=HEADERS,
            json=payload,
            timeout=60,
        )
    except requests.exceptions.ConnectionError:
        print("Cannot reach the API.")
        sys.exit(1)

    if r.status_code == 401:
        print("Authentication failed. Check API key.")
        sys.exit(1)
    if r.status_code == 404:
        print(f"Not found: {r.json().get('detail')}")
        return
    if r.status_code != 200:
        print(f"Error {r.status_code}: {r.text}")
        sys.exit(1)

    data = r.json()

    # Overall status
    status  = data["status"]
    message = data["status_message"]
    matches = data.get("matches", [])

    print(f"\nStatus  : {c(status.upper(), status)}  —  {message}")
    print(f"Matches : {len(matches)} returned\n")

    # Each match
    for match in matches:
        line()
        rank      = match["rank"]
        pn        = match["part_number"]
        mfr       = match["manufacturer"]
        family    = match.get("product_family") or ""
        score_pct = match["match_score_pct"]
        tier      = match["tier"]
        ai_text   = match.get("ai_explanation")

        print(f"{BOLD}#{rank}  {mfr}  {pn}{RESET}  [{family}]")
        print(f"    Score : {c(score_pct, tier)}  ({tier.upper()})")

        if ai_text:
            print(f"\n    {BOLD}AI Explanation:{RESET}")
            # Word-wrap
            words = ai_text.split()
            current = "    "
            for word in words:
                if len(current) + len(word) + 1 > 70:
                    print(current)
                    current = "    " + word + " "
                else:
                    current += word + " "
            if current.strip():
                print(current)
        else:
            print("    AI Explanation: (not available)")

    line("=")
    # Save raw response
    with open("last_response.json", "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nFull JSON saved to last_response.json\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def main():
    global API_URL
    parser = argparse.ArgumentParser(description="Test the Encoder Cross-Reference API")
    parser.add_argument("--part-number", "-p", default=None,
                        help="Part number to cross-reference")
    parser.add_argument("--num-matches", "-n", type=int, default=3,
                        help="Number of matches to return (default: 3)")
    parser.add_argument("--url", default=API_URL,
                        help=f"API base URL (default: {API_URL})")
    args = parser.parse_args()

    API_URL = args.url.rstrip("/")

    if not check_health():
        sys.exit(1)

    if not args.part_number:
        # Demo mode — try a few part numbers
        print("\nNo part number given. Running demo queries...\n")
        for pn in ["DFS60B-S4PA10000", "WDGI58B-10-10-1024-ABN-K-K"]:
            run_match(pn, num_matches=2)
    else:
        run_match(args.part_number, num_matches=args.num_matches)


if __name__ == "__main__":
    main()
