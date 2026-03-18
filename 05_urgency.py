"""
05_urgency.py
─────────────
Cross-references flags with occupancy to assign urgency.
Produces the final clean output file that feeds Power BI
and the Excel deliverable.

OUTPUT: outputs/05_final_output.parquet
        outputs/05_final_output.csv
        outputs/05_executive_summary.csv
"""

import pandas as pd
import numpy as np
import yaml
import os

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

print("Loading flags ...")
df = pd.read_parquet(os.path.join(cfg["output_dir"], "04_flags.parquet"))
print(f"  {len(df):,} flagged rows loaded")

OCC_LOW  = cfg["occupancy_low"]
OCC_HIGH = cfg["occupancy_high"]
OCC_RAISE = cfg["occupancy_consider_raise"]

# ── URGENCY SCORING ──────────────────────────────────────────
def urgency(row):
    flag = row["Final_Flag"]
    occ  = row["Flix_Occupancy_Pct"]

    if flag == "NO_DATA" or flag == "NO_PEERS":
        return ("SKIP",   0, "Insufficient data")

    if flag == "OVERPRICED":
        if pd.isna(occ):
            return ("HIGH",    3, "Overpriced — occupancy unknown")
        elif occ < OCC_LOW:
            return ("URGENT",  4, "Overpriced + low occupancy — reduce price")
        elif occ > OCC_HIGH:
            return ("MONITOR", 2, "Overpriced but high demand — watch trend")
        else:
            return ("HIGH",    3, "Overpriced — moderate occupancy")

    elif flag == "OVERPRICED_JUSTIFIED":
        return ("MONITOR", 2, "Price premium explained by quality — monitor load")

    elif flag == "UNDERPRICED":
        dtd = row.get("Days_To_Departure", None)
        if pd.isna(occ):
            return ("REVIEW",      2, "Underpriced — occupancy unknown")
        elif occ > OCC_HIGH:
            return ("OPPORTUNITY", 4, "Underpriced + high demand — consider price raise")
        elif occ < OCC_LOW:
            # Split INVESTIGATE by days to departure
            # Far out = too early to judge; Close in = genuine signal
            if pd.notna(dtd) and dtd > 30:
                return ("INVESTIGATE_EARLY", 1, "Underpriced + low load but journey is 30+ days away — monitor")
            else:
                return ("INVESTIGATE",       1, "Underpriced + low load — pricing not the issue, check rank/quality/timing")
        else:
            return ("REVIEW",      2, "Underpriced — moderate occupancy")

    elif flag == "OK":
        if pd.notna(occ) and occ > OCC_RAISE:
            return ("CONSIDER_RAISE", 1, "Well-priced but near-full — test a price increase")
        return ("OPTIMAL", 0, "Price in range — no action needed")

    return ("UNKNOWN", 0, "")

urgency_results = df.apply(urgency, axis=1)
df["Urgency_Label"]   = urgency_results.apply(lambda x: x[0])
df["Urgency_Score"]   = urgency_results.apply(lambda x: x[1])
df["Urgency_Action"]  = urgency_results.apply(lambda x: x[2])

print(f"\nUrgency distribution:")
print(df["Urgency_Label"].value_counts().to_string())

# ── REVENUE IMPACT ESTIMATE ──────────────────────────────────
# Rough estimate: how much revenue is at risk / opportunity
df["Revenue_Impact_Est"] = np.where(
    df["Final_Flag"].isin(["OVERPRICED", "UNDERPRICED", "OVERPRICED_JUSTIFIED"]),
    (df["Price_Deviation_Abs"].abs() * df["Available Seats"].fillna(0)).round(0) if "Available Seats" in df.columns else df["Price_Deviation_Abs"].abs().round(0),
    0
)

# ── CLEAN FINAL OUTPUT ───────────────────────────────────────
final_cols = [
    # Identity
    "Flixbus_Row_ID", "Route_Number", "DOJ", "Day_Type",
    "Bus_Category", "Departure_Time",

    # Our price
    "Flix_Price", "Flix_Bus_Score", "Flix_Rating", "Flix_Occupancy_Pct", "Flix_SRP_Rank",

    # Peer benchmark
    "Peer_Count", "Peer_Median_Price", "Peer_Q1_Price", "Peer_Q3_Price",
    "Peer_Min_Price", "Peer_Max_Price", "Peer_Avg_Bus_Score",
    "IQR_Lower_Bound", "IQR_Upper_Bound", "Window_Used_Mins",

    # Deviation
    "Price_Deviation_Abs", "Price_Deviation_Pct",

    # Flags
    "Stage1_Flag", "Quality_Adjustment", "Flix_vs_Peer_Score_Diff", "Final_Flag",

    # Urgency
    "Urgency_Label", "Urgency_Score", "Urgency_Action",

    # Context
    "Confidence", "Revenue_Impact_Est",
    "Days_To_Departure", "DTD_Bucket",
]

# Keep only columns that exist
final_cols = [c for c in final_cols if c in df.columns]

final_df = df[final_cols].copy()

# Sort: highest urgency first, then by revenue impact
final_df = final_df.sort_values(
    ["Urgency_Score", "Revenue_Impact_Est"],
    ascending=[False, False]
).reset_index(drop=True)

# ── EXECUTIVE SUMMARY ────────────────────────────────────────
actionable = final_df[final_df["Final_Flag"].isin(["OVERPRICED","UNDERPRICED","OVERPRICED_JUSTIFIED"])]

summary = {
    "Total_Flixbus_Buses_Analyzed":    len(final_df),
    "Urgent_Flags":                    (final_df["Urgency_Label"] == "URGENT").sum(),
    "Investigate_Genuine":             (final_df["Urgency_Label"] == "INVESTIGATE").sum(),
    "Investigate_Early_Booking":       (final_df["Urgency_Label"] == "INVESTIGATE_EARLY").sum(),
    "Opportunity_Flags":               (final_df["Urgency_Label"] == "OPPORTUNITY").sum(),
    "High_Priority_Flags":             (final_df["Urgency_Label"] == "HIGH").sum(),
    "Monitor_Flags":                   (final_df["Urgency_Label"] == "MONITOR").sum(),
    "Optimal_OK":                      (final_df["Urgency_Label"] == "OPTIMAL").sum(),
    "Overpriced_Count":                (final_df["Final_Flag"] == "OVERPRICED").sum(),
    "Underpriced_Count":               (final_df["Final_Flag"] == "UNDERPRICED").sum(),
    "Quality_Justified_Count":         (final_df["Final_Flag"] == "OVERPRICED_JUSTIFIED").sum(),
    "Avg_Price_Deviation_Pct":         round(final_df["Price_Deviation_Pct"].mean(), 2),
    "Total_Revenue_Impact_Est":        round(final_df["Revenue_Impact_Est"].sum(), 0),
    "High_Confidence_Flags":           (final_df["Confidence"] == "High").sum(),
    "Low_Confidence_Flags":            (final_df["Confidence"] == "Low").sum(),
}

summary_df = pd.DataFrame([summary])

print(f"\n{'='*50}")
print("EXECUTIVE SUMMARY")
print(f"{'='*50}")
for k, v in summary.items():
    print(f"  {k}: {v}")

# ── Save ─────────────────────────────────────────────────────
out_dir = cfg["output_dir"]
final_df.to_parquet(os.path.join(out_dir, "05_final_output.parquet"), index=False)
final_df.to_csv(os.path.join(out_dir, "05_final_output.csv"), index=False)
summary_df.to_csv(os.path.join(out_dir, "05_executive_summary.csv"), index=False)

print(f"\nSaved → outputs/05_final_output.parquet + .csv")
print(f"Saved → outputs/05_executive_summary.csv")
print("\n✅ Script 05 complete. Pipeline done.")