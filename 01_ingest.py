"""
01_ingest.py
------------
Loads data.xlsx, validates schema, enforces types with
context-aware NA handling, drops zero-variance columns,
audits nulls and duplicates.

ORDER IS IMPORTANT:
  1. Fill booleans with True/False FIRST
  2. THEN check for zero-variance (so True+NA cols are correctly evaluated)

NA handling:
  - Boolean columns           -> False
  - Seat count columns        -> 0
  - Discounted Prices         -> "" (no discount)
  - Bus Score / Total Ratings -> keep NaN
  - Number of Reviews         -> 0
  - Weighted Average Price    -> drop row (critical)

OUTPUT: outputs/01_validated.parquet
        outputs/01_ingestion_log.csv
"""

import pandas as pd
import numpy as np
import yaml, os, sys
from datetime import datetime

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

os.makedirs(cfg["output_dir"], exist_ok=True)
LOG = []

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")
    LOG.append({"timestamp": ts, "level": level, "message": msg})

REQUIRED_COLS = [
    "Route Number", "Date of Extraction", "Departure Date",
    "SRP Rank", "Operator", "Bus Type", "Is AC", "Is Seater", "Is Sleeper",
    "Departure Time", "Arrival Time", "Journey Duration (Min)", "DOJ",
    "Fare List", "Discounted Prices", "Weighted Average Price", "Seat Prices",
    "Available Seats", "Total Seats", "Available Window Seats",
    "Available Single Seats", "Available Aisle Seats",
    "Available Upper Seats", "Available Lower Seats",
    "BP Count", "DP Count", "Total Ratings", "Number of Reviews",
    "Bus Score", "Is Seat Layout Available", "Is Live Tracking Available",
    "Is M-Ticket Enabled"
]

NA_STRINGS = {"NA", "na", "N/A", "n/a", "n.a.", "N.A.", "none", "None", "null", "NULL", ""}

# ── Load ─────────────────────────────────────────────────────
log(f"Loading {cfg['input_file']} ...")
try:
    df = pd.read_excel(cfg["input_file"], engine="openpyxl")
    log(f"Loaded {len(df):,} rows x {len(df.columns)} columns")
except Exception as e:
    log(f"FAILED to load file: {e}", "ERROR")
    sys.exit(1)

# ── Schema check ─────────────────────────────────────────────
missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
if missing_cols:
    log(f"MISSING columns: {missing_cols}", "ERROR")
    sys.exit(1)
else:
    log("Schema check passed - all required columns present")

extra_cols = [c for c in df.columns if c not in REQUIRED_COLS]
if extra_cols:
    log(f"Extra columns (will be kept): {extra_cols}", "WARN")

# ── Step 1: Date columns ─────────────────────────────────────
log("Parsing date columns ...")
df["DOJ"] = pd.to_datetime(df["DOJ"], dayfirst=True, errors="coerce")
bad_doj = df["DOJ"].isna().sum()
if bad_doj > 0:
    log(f"  {bad_doj:,} rows have unparseable DOJ - dropping", "WARN")
    df = df.dropna(subset=["DOJ"])

df["Date of Extraction"] = pd.to_datetime(df["Date of Extraction"], errors="coerce")

# ── Step 2: Boolean columns (MUST happen before zero-variance check) ──
# Your data: True = feature present, NA = feature absent -> False
# We fill NA with False FIRST so that zero-variance check sees
# the real distribution (True + False), not just True.
log("Processing boolean columns ...")

bool_cols = [c for c in [
    "Is AC", "Is Seater", "Is Sleeper",
    "Is Seat Layout Available", "Is Live Tracking Available", "Is M-Ticket Enabled"
] if c in df.columns]

def to_bool(x):
    if pd.isna(x):              return False
    if isinstance(x, bool):     return x
    if isinstance(x, (int, float)): return bool(x)
    s = str(x).strip()
    if s in NA_STRINGS:         return False
    if s.lower() == "true":     return True
    if s == "1":                return True
    return False

for col in bool_cols:
    df[col] = df[col].apply(to_bool)
    log(f"  {col}: True={df[col].sum():,}  False={(~df[col]).sum():,}")

# ── Step 3: Zero-variance check (AFTER boolean fill) ─────────
# Now True+NA cols correctly show True+False distribution.
# Only drop if genuinely all same value after filling.
log("Checking for zero-variance columns ...")
zero_var_cols = []
for col in df.columns:
    # Use all values including filled booleans
    unique_vals = df[col].dropna().unique()
    if len(unique_vals) <= 1:
        zero_var_cols.append(col)

if zero_var_cols:
    log(f"  Zero-variance columns detected (dropping): {zero_var_cols}", "WARN")
    for col in zero_var_cols:
        uv = df[col].dropna().unique()
        log(f"    '{col}' — only value: {uv}", "WARN")
    df = df.drop(columns=zero_var_cols)
    log(f"  Dropped {len(zero_var_cols)} column(s). Remaining: {len(df.columns)}")
else:
    log("  No zero-variance columns found")

# ── Step 4: Numeric columns ───────────────────────────────────
log("Processing numeric columns ...")

df["Weighted Average Price"] = pd.to_numeric(df["Weighted Average Price"], errors="coerce")

# Seat counts: NA = seat type doesn't exist on this bus -> 0
seat_count_cols = [c for c in [
    "Available Seats", "Total Seats",
    "Available Window Seats", "Available Single Seats", "Available Aisle Seats",
    "Available Upper Seats", "Available Lower Seats"
] if c in df.columns]
for col in seat_count_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

# Number of Reviews: NA = no reviews -> 0
if "Number of Reviews" in df.columns:
    df["Number of Reviews"] = pd.to_numeric(df["Number of Reviews"], errors="coerce").fillna(0).astype(int)

# Keep NaN for genuinely unknown quality metrics
for col in ["Journey Duration (Min)", "BP Count", "DP Count", "Total Ratings", "Bus Score"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ── Step 5: String columns ────────────────────────────────────
# Forces uniform str type — fixes parquet crash from mixed int/str in Fare List
# (single-fare buses store fare as integer, multi-fare as string)
# Discounted Prices NA -> "" meaning no discount, not missing data
log("Processing string columns ...")

def to_clean_str(x):
    if pd.isna(x): return ""
    s = str(x).strip()
    return "" if s in NA_STRINGS else s

string_cols = [c for c in [
    "Fare List", "Discounted Prices", "Seat Prices", "SRP Rank",
    "Departure Time", "Arrival Time", "Route Number",
    "Operator", "Bus Type", "Departure Date"
] if c in df.columns]

for col in string_cols:
    df[col] = df[col].apply(to_clean_str)

log("Type enforcement complete")

# ── Step 6: Null audit ────────────────────────────────────────
log("Null audit (after type enforcement):")
null_counts = df.isnull().sum()
null_counts = null_counts[null_counts > 0].sort_values(ascending=False)
if len(null_counts) == 0:
    log("  No nulls remaining")
else:
    for col, cnt in null_counts.items():
        pct = cnt / len(df) * 100
        log(f"  {col}: {cnt:,} nulls ({pct:.1f}%)", "WARN" if pct > 10 else "INFO")

critical = [c for c in ["Route Number", "DOJ", "Operator",
                         "Weighted Average Price", "Departure Time"] if c in df.columns]
if df[critical].isnull().sum().any():
    before = len(df)
    df = df.dropna(subset=critical)
    log(f"Dropped {before - len(df):,} rows with critical nulls. Remaining: {len(df):,}", "WARN")

# ── Step 7: Duplicate check ───────────────────────────────────
dup_keys = [c for c in ["Route Number", "DOJ", "Date of Extraction",
                         "Operator", "Departure Time", "Bus Type"] if c in df.columns]
dupes = df.duplicated(subset=dup_keys, keep="first").sum()
if dupes > 0:
    log(f"Found {dupes:,} duplicate rows - keeping first", "WARN")
    df = df.drop_duplicates(subset=dup_keys, keep="first")
else:
    log("No duplicates found")

# ── Step 8: Summary ───────────────────────────────────────────
our_op = cfg["our_operator"]
our_count = (df["Operator"] == our_op).sum()
log(f"Unique operators: {df['Operator'].nunique()}")
log(f"  '{our_op}' rows: {our_count:,}")
if our_count == 0:
    log(f"WARNING: '{our_op}' not found in data!", "WARN")
    log(f"  Top 5 operators: {df['Operator'].value_counts().head(5).to_dict()}", "WARN")

log(f"Routes: {df['Route Number'].nunique()}")
log(f"Journey dates (DOJ): {df['DOJ'].nunique()}")
log(f"Extraction dates: {df['Date of Extraction'].nunique()}")
log(f"Final row count: {len(df):,}")
log(f"Final columns ({len(df.columns)}): {df.columns.tolist()}")

# ── Save ──────────────────────────────────────────────────────
out_path = os.path.join(cfg["output_dir"], "01_validated.parquet")
df.to_parquet(out_path, index=False)
log(f"Saved -> {out_path}")

log_df = pd.DataFrame(LOG)
log_df.to_csv(os.path.join(cfg["output_dir"], "01_ingestion_log.csv"), index=False)
log("Saved ingestion log")

print("\n✅ Script 01 complete.")