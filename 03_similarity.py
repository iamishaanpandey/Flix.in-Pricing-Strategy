"""
03_similarity.py
----------------
Finds peer groups for every Flixbus bus using vectorised
pandas operations.

KEY DESIGN DECISION:
  Date of Extraction is included in the merge key.
  This ensures we only compare Flixbus prices against
  competitor prices from the SAME snapshot date.
  Comparing across snapshots would mix prices from
  different points in time — invalid for pricing analysis.

STEPS:
  1. Merge on Route + DOJ + Date of Extraction + Day_Type + Bus_Category
  2. Vectorised departure window filter (±90 mins)
  3. Relax to ±150 mins for thin peer groups (< 3 peers)
  4. Soft scoring (rating band, review tier)
  5. Peer price statistics via groupby
  6. Build summary (one row per Flixbus bus)

OUTPUT: outputs/03_peer_groups.parquet + .csv
        outputs/03_similarity_summary.parquet + .csv
"""

import pandas as pd
import numpy as np
import yaml, os

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

WIN      = cfg["departure_window_mins"]
WIN_REL  = cfg["departure_window_relaxed_mins"]
MIN_PEER = cfg["min_peer_group"]
OUT      = cfg["output_dir"]

print("Loading featured data ...")
df = pd.read_parquet(os.path.join(OUT, "02_featured.parquet"))
print(f"  {len(df):,} rows loaded")
print(f"  Extraction dates: {sorted(df['Date of Extraction'].unique())}")

# ── Split ─────────────────────────────────────────────────────
flix = df[df["Is_Flixbus"]].copy()
comp = df[~df["Is_Flixbus"]].copy()
print(f"  Flixbus rows:    {len(flix):,}")
print(f"  Competitor rows: {len(comp):,}")

# ── Select columns needed ─────────────────────────────────────
FLIX_COLS = [c for c in [
    "Row_ID", "Route Number", "DOJ", "Date of Extraction",
    "Day_Type", "Bus_Category",
    "Departure Time", "Departure_Mins",
    "Weighted Average Price", "Bus Score", "Total Ratings",
    "Occupancy_Pct", "SRP_Rank_Num", "Rating_Band", "Review_Tier",
    "Available Seats"
] if c in flix.columns]

COMP_COLS = [c for c in [
    "Row_ID", "Route Number", "DOJ", "Date of Extraction",
    "Day_Type", "Bus_Category",
    "Departure Time", "Departure_Mins",
    "Weighted Average Price", "Bus Score", "Total Ratings",
    "Number of Reviews", "Occupancy_Pct",
    "Operator", "Operator_Size", "Rating_Band", "Review_Tier"
] if c in comp.columns]

flix = flix[FLIX_COLS].copy()
comp = comp[COMP_COLS].copy()

# ── STEP 1: Hard merge ────────────────────────────────────────
# Includes Date of Extraction — ensures same-snapshot comparison only.
# A Feb 17 Flixbus price is ONLY compared to Feb 17 competitor prices.
MERGE_KEYS = [
    "Route Number", "DOJ", "Date of Extraction",
    "Day_Type", "Bus_Category"
]
print(f"\nStep 1: Hard filter merge on {MERGE_KEYS} ...")

pairs = pd.merge(
    flix, comp,
    on=MERGE_KEYS,
    suffixes=("_flix", "_comp")
)
print(f"  Candidate pairs: {len(pairs):,}")

if len(pairs) == 0:
    print("  WARNING: No pairs found. Check that Flixbus and competitors share the same")
    print("  Route Number, DOJ, Date of Extraction, Day_Type and Bus_Category values.")
    import sys; sys.exit(1)

# ── STEP 2: Departure window ±90 mins ────────────────────────
print("Step 2: Applying ±90 min departure window ...")

pairs["Dep_Gap_Mins"] = (
    pairs["Departure_Mins_flix"] - pairs["Departure_Mins_comp"]
).abs()

within_90 = pairs[pairs["Dep_Gap_Mins"] <= WIN].copy()
within_150 = pairs[pairs["Dep_Gap_Mins"] <= WIN_REL].copy()

print(f"  Pairs within ±{WIN} min:  {len(within_90):,}")
print(f"  Pairs within ±{WIN_REL} min: {len(within_150):,}")

# ── STEP 3: Relax window for thin groups ─────────────────────
print(f"Step 3: Relaxing window for Flixbus rows with < {MIN_PEER} peers ...")

peer_counts_90 = (
    within_90.groupby("Row_ID_flix")
    .size()
    .reset_index(name="n90")
)

thin_ids = set(
    peer_counts_90[peer_counts_90["n90"] < MIN_PEER]["Row_ID_flix"]
)

# Also include Flixbus rows that got 0 pairs even in ±90
all_flix_ids  = set(flix["Row_ID"])
matched_ids   = set(peer_counts_90["Row_ID_flix"])
zero_peer_ids = all_flix_ids - matched_ids
thin_ids      = thin_ids | zero_peer_ids

print(f"  Flixbus rows needing relaxed window: {len(thin_ids):,}")
print(f"  Flixbus rows with enough peers:      {len(all_flix_ids) - len(thin_ids):,}")

thin_pairs   = within_150[within_150["Row_ID_flix"].isin(thin_ids)].copy()
normal_pairs = within_90[~within_90["Row_ID_flix"].isin(thin_ids)].copy()

thin_pairs["Window_Used"]   = WIN_REL
normal_pairs["Window_Used"] = WIN

all_pairs = pd.concat([normal_pairs, thin_pairs], ignore_index=True)
print(f"  Total pairs after window logic: {len(all_pairs):,}")

# ── STEP 4: Soft scoring ──────────────────────────────────────
print("Step 4: Soft scoring ...")

all_pairs["Same_Rating_Band"] = (
    all_pairs["Rating_Band_flix"] == all_pairs["Rating_Band_comp"]
)
all_pairs["Same_Review_Tier"] = (
    all_pairs["Review_Tier_flix"] == all_pairs["Review_Tier_comp"]
)
all_pairs["Soft_Score"] = (
    all_pairs["Same_Rating_Band"].astype(int) +
    all_pairs["Same_Review_Tier"].astype(int)
)

# ── STEP 5: Peer price statistics ────────────────────────────
print("Step 5: Computing peer statistics ...")

peer_stats = (
    all_pairs.groupby("Row_ID_flix")
    .agg(
        Peer_Count         = ("Row_ID_comp",                    "count"),
        Peer_Median_Price  = ("Weighted Average Price_comp",    "median"),
        Peer_Q1_Price      = ("Weighted Average Price_comp",    lambda x: x.quantile(0.25)),
        Peer_Q3_Price      = ("Weighted Average Price_comp",    lambda x: x.quantile(0.75)),
        Peer_Min_Price     = ("Weighted Average Price_comp",    "min"),
        Peer_Max_Price     = ("Weighted Average Price_comp",    "max"),
        Peer_Avg_Bus_Score = ("Bus Score_comp",                 "mean"),
        Window_Used_Mins   = ("Window_Used",                    "first"),
    )
    .reset_index()
    .rename(columns={"Row_ID_flix": "Row_ID"})
)

def confidence(n):
    if n >= 5:          return "High"
    elif n >= MIN_PEER: return "Medium"
    else:               return "Low"

peer_stats["Confidence"] = peer_stats["Peer_Count"].apply(confidence)

# ── STEP 6: Similarity summary ────────────────────────────────
print("Step 6: Building similarity summary ...")

flix_details = df[df["Is_Flixbus"]][[
    "Row_ID", "Route Number", "DOJ", "Date of Extraction",
    "Day_Type", "Bus_Category", "Departure Time",
    "Weighted Average Price", "Bus Score", "Total Ratings",
    "Occupancy_Pct", "SRP_Rank_Num",
]].copy().rename(columns={
    "Row_ID":                   "Row_ID",
    "Route Number":             "Route_Number",
    "Date of Extraction":       "Extraction_Date",
    "Departure Time":           "Departure_Time",
    "Weighted Average Price":   "Flix_Price",
    "Bus Score":                "Flix_Bus_Score",
    "Total Ratings":            "Flix_Rating",
    "Occupancy_Pct":            "Flix_Occupancy_Pct",
    "SRP_Rank_Num":             "Flix_SRP_Rank",
})

summary = flix_details.merge(peer_stats, on="Row_ID", how="left")
summary["Peer_Count"]  = summary["Peer_Count"].fillna(0).astype(int)
summary["Confidence"]  = summary["Peer_Count"].apply(confidence)

# ── Summary stats ─────────────────────────────────────────────
print(f"\nResults:")
print(f"  Flixbus rows analysed: {len(summary):,}")
print(f"  Peer group records:    {len(all_pairs):,}")
print(f"\n  Confidence distribution:")
print(summary["Confidence"].value_counts().to_string())
print(f"\n  Peer count stats:")
print(summary["Peer_Count"].describe().round(1).to_string())
print(f"\n  Extraction date breakdown:")
print(summary.groupby("Extraction_Date")["Row_ID"].count().to_string())

# ── Build peer groups table ───────────────────────────────────
# Use rename dict instead of list assignment — safe when some columns
# don't exist in all_pairs and get filtered out by the if-check.
keep_cols = [c for c in [
    "Row_ID_flix", "Row_ID_comp",
    "Route Number", "DOJ", "Date of Extraction",
    "Day_Type", "Bus_Category",
    "Departure Time_flix", "Departure Time_comp",
    "Dep_Gap_Mins", "Window_Used",
    "Weighted Average Price_comp",
    "Bus Score_comp", "Total Ratings_comp",
    "Operator", "Operator_Size",
    "Same_Rating_Band", "Same_Review_Tier", "Soft_Score",
    "Peer_Count", "Confidence",
] if c in all_pairs.columns]

peer_groups = all_pairs[keep_cols].copy()

# Rename using a dict — only renames columns that actually exist
peer_groups = peer_groups.rename(columns={
    "Row_ID_flix":                      "Flixbus_Row_ID",
    "Row_ID_comp":                      "Competitor_Row_ID",
    "Route Number":                     "Route_Number",
    "Date of Extraction":               "Extraction_Date",
    "Departure Time_flix":              "Flix_Departure",
    "Departure Time_comp":              "Competitor_Departure",
    "Dep_Gap_Mins":                     "Departure_Gap_Mins",
    "Window_Used":                      "Window_Used_Mins",
    "Weighted Average Price_comp":      "Competitor_Price",
    "Bus Score_comp":                   "Competitor_Bus_Score",
    "Total Ratings_comp":               "Competitor_Rating",
    "Operator":                         "Competitor_Operator",
    "Peer_Count":                       "Peer_Group_Size",
})

# ── Save ──────────────────────────────────────────────────────
print("\nSaving ...")
peer_groups.to_parquet(os.path.join(OUT, "03_peer_groups.parquet"), index=False)
peer_groups.to_csv(os.path.join(OUT, "03_peer_groups.csv"), index=False)
summary.to_parquet(os.path.join(OUT, "03_similarity_summary.parquet"), index=False)
summary.to_csv(os.path.join(OUT, "03_similarity_summary.csv"), index=False)

print(f"  Saved 03_peer_groups    ({len(peer_groups):,} rows)")
print(f"  Saved 03_similarity_summary ({len(summary):,} rows)")
print("\n✅ Script 03 complete.")