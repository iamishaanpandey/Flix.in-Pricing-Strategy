"""
04_flag.py
──────────
Reads the similarity summary and applies the two-stage
flagging model:
  Stage 1 — IQR statistical flag
  Stage 2 — Quality-adjusted check

OUTPUT: outputs/04_flags.parquet
        outputs/04_flags.csv
"""

import pandas as pd
import numpy as np
import yaml
import os

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

print("Loading similarity summary ...")
df = pd.read_parquet(os.path.join(cfg["output_dir"], "03_similarity_summary.parquet"))
print(f"  {len(df):,} Flixbus rows to flag")

IQR_MULT = cfg["iqr_multiplier"]
QS_THRESH = cfg["quality_score_threshold"]

# ── STAGE 1 — IQR Statistical Flag ───────────────────────────
df["IQR"] = df["Peer_Q3_Price"] - df["Peer_Q1_Price"]
df["IQR_Lower_Bound"] = df["Peer_Median_Price"] - IQR_MULT * df["IQR"]
df["IQR_Upper_Bound"] = df["Peer_Median_Price"] + IQR_MULT * df["IQR"]

def stage1_flag(row):
    price  = row["Flix_Price"]
    lo     = row["IQR_Lower_Bound"]
    hi     = row["IQR_Upper_Bound"]
    median = row["Peer_Median_Price"]

    if pd.isna(price) or pd.isna(median):
        return "NO_DATA"
    if row["Peer_Count"] == 0:
        return "NO_PEERS"
    if price > hi:
        return "OVERPRICED"
    elif price < lo:
        return "UNDERPRICED"
    else:
        return "OK"

df["Stage1_Flag"] = df.apply(stage1_flag, axis=1)

# Price deviation from peer median
df["Price_Deviation_Abs"] = df["Flix_Price"] - df["Peer_Median_Price"]
df["Price_Deviation_Pct"] = np.where(
    df["Peer_Median_Price"] > 0,
    (df["Flix_Price"] - df["Peer_Median_Price"]) / df["Peer_Median_Price"] * 100,
    np.nan
)

print(f"\nStage 1 flag distribution:")
print(df["Stage1_Flag"].value_counts().to_string())

# ── STAGE 2 — Quality Adjustment ─────────────────────────────
def stage2_quality(row):
    flag  = row["Stage1_Flag"]
    f_score = row["Flix_Bus_Score"]
    p_score = row["Peer_Avg_Bus_Score"]

    if flag in ("NO_DATA", "NO_PEERS", "OK"):
        return "N/A"
    if pd.isna(f_score) or pd.isna(p_score):
        return "Score_Unavailable"

    score_diff = f_score - p_score  # positive = Flix is better quality

    if flag == "OVERPRICED" and score_diff > QS_THRESH:
        return "Quality_Justified"        # Flix is genuinely better — downgrade urgency
    elif flag == "UNDERPRICED" and score_diff < -QS_THRESH:
        return "Quality_Consistent"       # Flix is lower quality — flag consistent
    else:
        return "Not_Justified"            # Flag stands at full strength

df["Quality_Adjustment"] = df.apply(stage2_quality, axis=1)
df["Flix_vs_Peer_Score_Diff"] = df["Flix_Bus_Score"] - df["Peer_Avg_Bus_Score"]

print(f"\nQuality adjustment distribution (flagged rows only):")
flagged = df[df["Stage1_Flag"].isin(["OVERPRICED", "UNDERPRICED"])]
print(flagged["Quality_Adjustment"].value_counts().to_string())

# ── FINAL FLAG ───────────────────────────────────────────────
# Combine Stage 1 + Stage 2 into a single final flag
def final_flag(row):
    s1 = row["Stage1_Flag"]
    qa = row["Quality_Adjustment"]
    if s1 == "OVERPRICED" and qa == "Quality_Justified":
        return "OVERPRICED_JUSTIFIED"   # Price high but quality explains it
    return s1

df["Final_Flag"] = df.apply(final_flag, axis=1)

print(f"\nFinal flag distribution:")
print(df["Final_Flag"].value_counts().to_string())

# ── Save ─────────────────────────────────────────────────────
out_dir = cfg["output_dir"]
df.to_parquet(os.path.join(out_dir, "04_flags.parquet"), index=False)
df.to_csv(os.path.join(out_dir, "04_flags.csv"), index=False)

print(f"\nSaved → outputs/04_flags.parquet + .csv")
print("\n✅ Script 04 complete.")
