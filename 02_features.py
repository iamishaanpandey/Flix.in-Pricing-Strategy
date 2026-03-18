"""
02_features.py
--------------
Reads validated data, engineers all derived features.

OUTPUT: outputs/02_featured.parquet
"""

import pandas as pd
import numpy as np
import yaml, os

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

print("Loading validated data ...")
df = pd.read_parquet(os.path.join(cfg["output_dir"], "01_validated.parquet"))
print(f"  {len(df):,} rows loaded")
print(f"  Columns: {df.columns.tolist()}")

# ── 1. IS FLIXBUS ─────────────────────────────────────────────
df["Is_Flixbus"] = df["Operator"] == cfg["our_operator"]
print(f"\n  Flixbus rows:    {df['Is_Flixbus'].sum():,}")
print(f"  Competitor rows: {(~df['Is_Flixbus']).sum():,}")

# ── 2. DAY TYPE ───────────────────────────────────────────────
weekend_days = cfg["weekend_days"]
df["Day_Type"] = df["DOJ"].dt.weekday.apply(
    lambda x: "Weekend" if x in weekend_days else "Weekday"
)
print(f"\n  Weekday: {(df['Day_Type']=='Weekday').sum():,}  Weekend: {(df['Day_Type']=='Weekend').sum():,}")

# ── 3. DEPARTURE MINUTES FROM MIDNIGHT ───────────────────────
# Excel can store times as:
#   "22:50"       (HH:MM string)
#   "22:50:00"    (HH:MM:SS string - when Excel time object is cast to str)
#   datetime.time (actual time object)
# All three are handled below.

def time_to_mins(x):
    try:
        # If it's already a datetime.time object
        import datetime
        if isinstance(x, datetime.time):
            return x.hour * 60 + x.minute

        s = str(x).strip()
        if not s or s == "":
            return np.nan

        parts = s.split(":")
        # Takes HH and MM, ignores seconds if present
        h = int(parts[0])
        m = int(parts[1])
        return h * 60 + m
    except:
        return np.nan

df["Departure_Mins"] = df["Departure Time"].apply(time_to_mins)
bad = df["Departure_Mins"].isna().sum()
print(f"\n  Departure_Mins: {df['Departure_Mins'].notna().sum():,} parsed  |  {bad:,} failed")
if bad > 0:
    # Show a sample of what failed so we can debug if needed
    sample = df[df["Departure_Mins"].isna()]["Departure Time"].value_counts().head(5)
    print(f"  Sample of failed values: {sample.to_dict()}")

# ── 4. BUS CATEGORY ──────────────────────────────────────────
# Check which boolean columns survived zero-variance drop in script 01
has_ac      = "Is AC"      in df.columns
has_seater  = "Is Seater"  in df.columns
has_sleeper = "Is Sleeper" in df.columns

print(f"\n  Boolean cols present — Is AC: {has_ac}  Is Seater: {has_seater}  Is Sleeper: {has_sleeper}")

def bus_category(row):
    # If a column was dropped (zero-variance = all True), treat it as True
    ac      = bool(row["Is AC"])      if has_ac      else True
    seater  = bool(row["Is Seater"])  if has_seater  else True
    sleeper = bool(row["Is Sleeper"]) if has_sleeper else True

    if ac and seater and sleeper:  return "AC_Semi_Sleeper"
    elif ac and sleeper:           return "AC_Sleeper"
    elif ac and seater:            return "AC_Seater"
    elif seater:                   return "Non_AC_Seater"
    elif sleeper:                  return "Non_AC_Sleeper"
    else:                          return "AC_Seater"

df["Bus_Category"] = df.apply(bus_category, axis=1)
print(f"\n  Bus categories:")
print(df["Bus_Category"].value_counts().to_string())

# ── 5. OCCUPANCY % ────────────────────────────────────────────
df["Occupancy_Pct"] = np.where(
    df["Total Seats"] > 0,
    (df["Total Seats"] - df["Available Seats"]) / df["Total Seats"],
    np.nan
)
df["Occupancy_Pct"] = df["Occupancy_Pct"].clip(0, 1)

# ── 6. SRP RANK ───────────────────────────────────────────────
df["SRP_Rank_Num"] = df["SRP Rank"].str.split("/").str[0].apply(pd.to_numeric, errors="coerce")
df["SRP_Total_Listings"] = df["SRP Rank"].str.split("/").str[1].apply(pd.to_numeric, errors="coerce")

# ── 7. FARE MIN PRICE ─────────────────────────────────────────
def fare_min(fare_str):
    try:
        if not fare_str or fare_str == "": return np.nan
        vals = [float(p) for p in str(fare_str).split("-") if p.strip()]
        return min(vals) if vals else np.nan
    except:
        return np.nan

df["Fare_Min_Price"] = df["Fare List"].apply(fare_min)

# ── 8. DISCOUNTED MIN PRICE ───────────────────────────────────
# NA in source = no discount exists -> 0 (not NaN)
# 0 means "no discount", NaN means "we don't know" — these are different
def disc_min(disc_str):
    try:
        if not disc_str or disc_str == "": return 0
        vals = [float(p) for p in str(disc_str).split("-") if p.strip()]
        return min(vals) if vals else 0
    except:
        return 0

df["Discounted_Min_Price"] = df["Discounted Prices"].apply(disc_min)

# Rupee discount per ticket (0 = no discount)
df["Discount_Amount"] = np.where(
    (df["Fare_Min_Price"].notna()) & (df["Discounted_Min_Price"] > 0),
    df["Fare_Min_Price"] - df["Discounted_Min_Price"],
    0
)

# ── 9. RATING BAND ────────────────────────────────────────────
def rating_band(r):
    try:
        r = float(r)
        if r >= 4.5: return "A"
        elif r >= 4.0: return "B"
        elif r >= 3.5: return "C"
        else: return "D"
    except:
        return "Unknown"

df["Rating_Band"] = df["Total Ratings"].apply(rating_band) if "Total Ratings" in df.columns else "Unknown"

# ── 10. REVIEW VOLUME TIER ────────────────────────────────────
t1 = cfg["review_tiers"]["tier1_min"]
t2 = cfg["review_tiers"]["tier2_min"]

def review_tier(n):
    try:
        n = int(n)
        if n >= t1: return "Tier1"
        elif n >= t2: return "Tier2"
        else: return "Tier3"
    except:
        return "Tier3"

df["Review_Tier"] = df["Number of Reviews"].apply(review_tier) if "Number of Reviews" in df.columns else "Tier3"

# ── 11. OPERATOR SIZE ─────────────────────────────────────────
op_counts = df.groupby("Operator")["Route Number"].count().reset_index()
op_counts.columns = ["Operator", "Op_Listing_Count"]
large_thresh = op_counts["Op_Listing_Count"].quantile(cfg["operator_size_large_pct"])
small_thresh = op_counts["Op_Listing_Count"].quantile(cfg["operator_size_small_pct"])

def op_size(cnt):
    if cnt >= large_thresh: return "Large"
    elif cnt <= small_thresh: return "Small"
    return "Medium"

op_counts["Operator_Size"] = op_counts["Op_Listing_Count"].apply(op_size)
df = df.merge(op_counts[["Operator", "Op_Listing_Count", "Operator_Size"]], on="Operator", how="left")

# ── 12. DAYS TO DEPARTURE ────────────────────────────────────
# How many days between when data was scraped and the journey date.
# Critical context for INVESTIGATE cases:
#   - Far out (>30 days): low occupancy is normal, too early to judge
#   - Close in (<7 days): low occupancy is a genuine signal
#   - Negative: journey already passed (data quality issue)
df["Days_To_Departure"] = (
    df["DOJ"].dt.normalize() - df["Date of Extraction"].dt.normalize()
).dt.days

neg = (df["Days_To_Departure"] < 0).sum()
if neg > 0:
    print(f"  WARNING: {neg:,} rows have negative Days_To_Departure (journey before extraction date)")

print("\n  Days_To_Departure stats:")
print(df["Days_To_Departure"].describe().round(1).to_string())
print("\n  Distribution buckets:")
bins = [-999, 0, 7, 14, 30, 60, 999]
labels = ["Past", "0-7 days", "8-14 days", "15-30 days", "31-60 days", "60+ days"]
df["DTD_Bucket"] = pd.cut(df["Days_To_Departure"], bins=bins, labels=labels)
print(df["DTD_Bucket"].value_counts().sort_index().to_string())

# ── 13. ROW ID ────────────────────────────────────────────────
df = df.reset_index(drop=True)
df["Row_ID"] = df.index

# ── Summary ───────────────────────────────────────────────────
print(f"\nColumn summary:")
new_cols = ["Is_Flixbus", "Day_Type", "Departure_Mins", "Bus_Category",
            "Occupancy_Pct", "SRP_Rank_Num", "Fare_Min_Price",
            "Discounted_Min_Price", "Discount_Amount",
            "Rating_Band", "Review_Tier", "Operator_Size", "Row_ID"]
for c in new_cols:
    if c in df.columns:
        nulls = df[c].isna().sum()
        print(f"  {c}: {df[c].dtype}  nulls={nulls:,}")

# ── Save ──────────────────────────────────────────────────────
out_path = os.path.join(cfg["output_dir"], "02_featured.parquet")
df.to_parquet(out_path, index=False)
print(f"\nSaved -> {out_path}")
print("\n✅ Script 02 complete.")