import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from matcher import find_matches_with_status  # noqa: E402


BASE = Path(__file__).parent
CSV_PATH = BASE / "data" / "competitor_unified.csv"
OUT_PATH = BASE / "manufacturer_strong_weak_vs_kubler.xlsx"

RANDOM_SEED = 42
SAMPLE_PER_MFR = 45


def _evaluate_part(df: pd.DataFrame, pn: str):
    results, status, _ = find_matches_with_status(
        pn,
        df,
        target_manufacturer="Kubler",
        top_n=1,
    )
    if results is None or results.empty:
        return None

    top = results.iloc[0]
    return {
        "competitor_part_number": pn,
        "top_kubler_part_number": str(top.get("part_number", "")),
        "match_score": float(top.get("match_score", 0.0)),
        "match_score_pct": round(float(top.get("match_score", 0.0)) * 100, 2),
        "tier": status.get("tier", ""),
    }


def main():
    df = pd.read_csv(CSV_PATH, low_memory=False)
    df = df[df["part_number"].notna()].copy()
    df["part_number"] = df["part_number"].astype(str).str.strip()
    df = df[df["part_number"] != ""]

    manufacturers = sorted(m for m in df["manufacturer"].dropna().unique() if m != "Kubler")
    rng = np.random.default_rng(RANDOM_SEED)

    final_rows = []
    for mfr in manufacturers:
        mfr_df = df[df["manufacturer"] == mfr]
        unique_pns = pd.Series(mfr_df["part_number"].unique())
        if unique_pns.empty:
            continue

        if len(unique_pns) > SAMPLE_PER_MFR:
            sample_pns = unique_pns.iloc[rng.choice(len(unique_pns), size=SAMPLE_PER_MFR, replace=False)].tolist()
        else:
            sample_pns = unique_pns.tolist()

        evaluated = []
        for pn in sample_pns:
            row = _evaluate_part(df, pn)
            if row:
                evaluated.append(row)

        if not evaluated:
            continue

        eval_df = pd.DataFrame(evaluated).sort_values("match_score", ascending=False).reset_index(drop=True)

        # Strong pick: prefer tier=strong, fallback to highest score
        strong_df = eval_df[eval_df["tier"] == "strong"]
        strong_row = (strong_df.iloc[0] if not strong_df.empty else eval_df.iloc[0]).to_dict()
        strong_row["manufacturer"] = mfr
        strong_row["selection_type"] = "strong"
        strong_row["selection_note"] = (
            "tier=strong"
            if strong_row["tier"] == "strong"
            else "highest score available in sampled set"
        )

        # Weak pick: prefer tier=weak, fallback to lowest score
        weak_df = eval_df[eval_df["tier"] == "weak"]
        weak_row = (weak_df.sort_values("match_score", ascending=True).iloc[0] if not weak_df.empty else eval_df.iloc[-1]).to_dict()
        weak_row["manufacturer"] = mfr
        weak_row["selection_type"] = "weak"
        weak_row["selection_note"] = (
            "tier=weak"
            if weak_row["tier"] == "weak"
            else "lowest score available in sampled set"
        )

        final_rows.extend([strong_row, weak_row])
        print(f"{mfr:<12} strong={strong_row['match_score_pct']:>6.2f}% weak={weak_row['match_score_pct']:>6.2f}%")

    out_df = pd.DataFrame(final_rows)[
        [
            "manufacturer",
            "selection_type",
            "competitor_part_number",
            "top_kubler_part_number",
            "match_score",
            "match_score_pct",
            "tier",
            "selection_note",
        ]
    ].sort_values(["manufacturer", "selection_type"])

    out_df.to_excel(OUT_PATH, index=False)
    print(f"\nWrote {len(out_df)} rows to: {OUT_PATH}")


if __name__ == "__main__":
    main()
