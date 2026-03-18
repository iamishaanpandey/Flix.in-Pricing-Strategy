# Flix.in Pricing Strategy — MVP Pipeline

> Automated end-to-end competitive pricing pipeline for FlixBus routes.
> Drops a file → runs Python → refreshes Power BI → emails the pricing team. Zero manual steps.

---

## Overview

This pipeline ingests competitor pricing data, engineers features, groups peer routes, flags mispriced FlixBus fares, assigns urgency scores, and delivers a daily-refreshed Power BI report to the pricing team — fully automated via Microsoft Power Automate.

---

## Pipeline Architecture

```
CSV / Excel Drop (SharePoint)
        ↓  [file-created trigger]
Power Automate
        ↓  [HTTP call]
Python Pipeline
  01_ingest.py       → validate schema, enforce types, fill NAs
  02_features.py     → engineer derived columns
  03_similarity.py   → peer grouping & soft scoring
  04_flag.py         → IQR statistical flagging
  05_urgency.py      → urgency labels & revenue impact
        ↓  [05_final_output.parquet → SharePoint]
Power BI Service
  Dataflow (Power Query / M)
  DAX Measures
  Published Report (5 pages, daily auto-refresh)
        ↓
Email Notification → Pricing Team
```

---

## Folder Structure

```
Flix.in-Pricing-Strategy/
│
├── pipeline/
│   ├── 01_ingest.py
│   ├── 02_features.py
│   ├── 03_similarity.py
│   ├── 04_flag.py
│   └── 05_urgency.py
│
├── config/
│   └── config.yaml          # all thresholds — no code changes needed
│
├── data/
│   ├── raw/                 # input files dropped here (not committed)
│   └── processed/           # output parquet files (not committed)
│
├── notebooks/               # exploratory analysis (optional)
│
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Pipeline Steps

### Step 1 — Data Ingestion (`01_ingest.py`)
- Loads CSV file, validates schema, enforces data types
- Fills NAs with business-logic defaults
- Drops zero-variance columns and true duplicates
- **Output:** `01_validated.parquet` (821,245 rows)

---

## Script Documentation

### Script 2 — Feature Engineering (`02_features.py`)

#### Overview
Transforms raw validated data into machine-learning-ready features. This step derives 14 new columns through parsing, classification, bucketing, and time-based calculations. The output feeds directly into peer grouping and flagging algorithms.

#### Why This Step Exists
Raw data contains nested information (e.g., "720-780-880" fare range, "17/165" rank strings, "HH:MM:SS" departure times) that cannot be used in grouping or comparison logic. Feature engineering decodes this information into structured, typed columns. Additionally, business rules (e.g., Friday = weekend for demand patterns) must be captured as flags to enable accurate peer matching.

#### Inputs
- **File:** `01_validated.parquet`
- **Rows:** 821,245
- **Columns:** 30 (mixed types: strings, floats, booleans, datetimes)

#### Operations Performed

| # | Feature | Logic | Notes |
|---|---------|-------|-------|
| 1 | `Is_Flixbus` | Boolean flag where `Operator == "Flixbus"` | Used to split dataset later |
| 2 | `Day_Type` | Mon–Thu = "Weekday", Fri–Sun = "Weekend" | Derived from DOJ weekday number. Friday in weekend due to demand patterns |
| 3 | `Departure_Mins` | Convert "HH:MM" or "HH:MM:SS" to integer minutes from midnight | Handles both string formats and datetime.time objects. Range: 0–1440 |
| 4 | `Bus_Category` | 5-way label from boolean combination: AC + Seater + Sleeper | AC_Sleeper, AC_Seater, AC_Semi_Sleeper, Non_AC_Seater, Non_AC_Sleeper. Zero-variance columns treated as all-True |
| 5 | `Occupancy_Pct` | (Total_Seats - Available_Seats) / Total_Seats | Clipped to [0, 1]. Empty capacity → 0, Full → 1 |
| 6 | `SRP_Rank_Num` | Parse "17/165" string → extract 17 as integer | SRP_Total_Listings extracts 165 from same string |
| 7 | `Fare_Min_Price` | Split "720-780-880" by dash, return min as float | No discount case: just single price parsed as float |
| 8 | `Discounted_Min_Price` | Same dash-split logic | Empty string (no discount) returns 0, not NaN. Distinction: 0 = no discount exists |
| 9 | `Discount_Amount` | Fare_Min_Price − Discounted_Min_Price | Returns 0 if no discount. Difference in absolute terms |
| 10 | `Rating_Band` | Bucket Total_Ratings: A (≥4.5), B (4.0–4.4), C (3.5–3.9), D (<3.5), Unknown if null | Categorical for soft scoring |
| 11 | `Review_Tier` | Number_of_Reviews: Tier1 (≥500), Tier2 (51–499), Tier3 (≤50) | Proxy for operator credibility |
| 12 | `Operator_Size` | Classify operators by listing count percentile: Large (top 20%), Small (bottom 20%), Medium (rest) | Controls for operator scale bias |
| 13 | `Days_To_Departure` | Integer days: DOJ − Date_of_Extraction | Always non-negative. DTD_Bucket assigns label: Past, 0-7 days, 8-14 days, 15-30 days, 31-60 days, 60+ days |
| 14 | `Row_ID` | Sequential integer index → unique per row | Primary key for downstream joins in peer grouping |

#### Output Columns Produced
**14 new features** + all original 30 columns = **44 total columns**

#### Key Design Decisions

- **Friday in weekend:** Demand data showed Friday behaves like weekend (higher fares, different passenger mix). Classification rule explicitly put Friday in weekend bucket.
- **Occupancy clipping:** Prevents invalid percentages from division errors; clips to [0, 1] range.
- **Discounted_Min_Price = 0 vs NaN:** Deliberate. NaN would be treated as missing in downstream stats. A 0 explicitly signals "no discount offered", which is actionable information.
- **Row_ID as sequential integer:** Enables fast merges on integer join keys instead of string (Route + DOJ + ...). Reduces memory footprint.
- **Bus_Category from boolean combo:** 5 categories cover ~95% of real fleet. Remaining combos treated as "None" and dropped.

#### Example Calculation

**Sample Row Data:**
```
Operator: "Flixbus"
DOJ: 2026-03-20 (Friday)
Date_of_Extraction: 2026-03-17
Departure_Time: "14:30:00"
Is_AC: 1, Is_Seater: 1, Is_Sleeper: 0
Total_Seats: 50, Available_Seats: 12
SRP_Rank: "8/124"
Fare_String: "650-750-850"
Discounted_Fare_String: "600-700"
Total_Ratings: 4.2
Number_Reviews: 287
Operator_Listings: 4,500 (82nd percentile)
```

**Calculated Features:**
```
Is_Flixbus = TRUE (Operator == "Flixbus")
Day_Type = "Weekend" (DOJ is Friday)
Departure_Mins = 14*60 + 30 = 870 mins
Bus_Category = "AC_Seater" (Is_AC=1, Is_Seater=1, Is_Sleeper=0)
Occupancy_Pct = (50-12)/50 = 0.76
SRP_Rank_Num = 8
SRP_Total_Listings = 124
Fare_Min_Price = min(650, 750, 850) = 650.0
Discounted_Min_Price = min(600, 700) = 600.0
Discount_Amount = 650.0 - 600.0 = 50.0
Rating_Band = "B" (4.0 ≤ 4.2 < 4.5)
Review_Tier = "Tier2" (51 ≤ 287 < 500)
Operator_Size = "Large" (82nd percentile > 80th)
Days_To_Departure = (20th - 17th) = 3 days
Row_ID = 45829 (example sequential ID)
```

---

### Script 3 — Peer Grouping (`03_similarity.py`)

#### Overview
Groups FlixBus routes with competitor routes that are operationally comparable. Uses vectorised pandas merging (not loops) to identify 27.5 peers per FlixBus route on average. Assigns confidence levels and soft quality scores. Produces aggregated peer statistics (min, median, Q1, Q3, max fares) used in flagging.

#### Why This Step Exists
A FlixBus price cannot be evaluated in isolation. Peer grouping defines the "market basket" — comparable competitor prices — against which each FlixBus fare is benchmarked. Without accurate peers, pricing flags are meaningless. The algorithm balances:
- **Hard constraints** (same route, same day) to ensure apples-to-apples comparison
- **Soft relaxation** (expanded time window) to handle routes with few peers
- **Quality signals** (rating, review count) to weight credible operators

#### Inputs
- **File:** `02_featured.parquet`
- **Rows:** 821,245
- **Flixbus rows:** 30,888 | **Competitor rows:** 790,357

#### Why Vectorised Merge Over Row-by-Row Loops?

**Performance Comparison:**

| Approach | Time Complexity | Memory | Runtime (821K rows) | Feasibility |
|----------|-----------------|--------|---------------------|-------------|
| **Python loop** | O(n × m) where m = avg peers | Low | ~2 hours | ❌ Impractical |
| **Vectorised pd.merge** | O(n log n) | High (worth it) | ~60 seconds | ✅ Production |

**Why the difference?**
- Loop approach: For each of 30,888 FlixBus rows, iterate through 790,357 competitors, check n conditions per pair → 24.4 billion comparisons
- Merge approach: Single `pd.merge()` on hard keys (route, DOJ, etc.), then vectorised comparisons on remaining columns → ~2.4M candidate pairs checked once

**Decision:** Vectorised merge is 120× faster. Chosen for production pipeline.

#### Operations Performed

**1. Dataset Split**
```python
flixbus = data[data['Is_Flixbus'] == True]  # 30,888 rows
competitors = data[data['Is_Flixbus'] == False]  # 790,357 rows
```

**2. Hard Merge on Candidate Generation**

Hard-merge keys (exact match required):
- `Route_Number`
- `DOJ` (Date of Journey)
- `Date_of_Extraction` (snapshot consistency)
- `Day_Type`
- `Bus_Category`

**Output:** 2,410,081 candidate (Flixbus, competitor) pairs

*Rationale for `Date_of_Extraction`:* Ensures a FlixBus price scraped on Feb 17 is only compared to competitor prices from Feb 17. Cross-date comparison would confound time trends with pricing differences.

**3. Departure Window Filter (Vectorised)**

```python
abs_gap = abs(flixbus['Departure_Mins'] - competitors['Departure_Mins'])
candidates = candidates[abs_gap <= 90]  # ±90 minute window
```

**Result:** 845,026 pairs remain (35% filtered)

**4. Window Relaxation**

For FlixBus rows with <3 peers in ±90 minute window, expand to ±150 minutes:
```python
relaxed_mask = (candidates['Peer_Count'] < 3) & (abs_gap <= 150)
candidates[relaxed_mask]['Window_Used_Mins'] = 150
```

**Result:** 2,109 FlixBus rows relaxed to broader window

**5. Confidence Assignment**

| Peer Count | Confidence | Count | % |
|------------|-----------|-------|-----|
| ≥5 | High | 26,550 | 86% |
| 3–4 | Medium | 3,085 | 10% |
| 1–2 | Low | 1,253 | 4% |

**6. Soft Scoring (Quality Signals)**

Per candidate pair, award points (no exclusion, only weighting):
- `Same_Rating_Band`: +1 if both operators rated A, B, C, or D (not Unknown)
- `Same_Review_Tier`: +1 if both in same Tier1/Tier2/Tier3

**Soft_Score range:** 0–2
- 2 = matching on both rating & review count (high credibility)
- 0 = mismatched on both (outlier, but included)

**7. Peer Statistics via GroupBy**

Per FlixBus `Row_ID`, aggregate all matched competitors:

| Statistic | Description |
|-----------|-------------|
| `Peer_Count` | Number of competitor matches |
| `Peer_Median_Price` | Median fare across all peers |
| `Peer_Q1_Price` | 25th percentile fare |
| `Peer_Q3_Price` | 75th percentile fare |
| `Peer_Min_Price` | Minimum competitor fare |
| `Peer_Max_Price` | Maximum competitor fare |
| `Peer_Avg_Bus_Score` | Mean bus quality score (ratings/reviews summary) |
| `Window_Used_Mins` | ±90 or ±150 depending on relaxation |

**8. Output Files**

| File | Rows | Granularity | Use |
|------|------|-------------|-----|
| `03_peer_groups.parquet` | 848,032 | One row per (Flixbus, competitor) pair | Detailed peer comparisons, debugging |
| `03_similarity_summary.parquet` | 30,888 | One row per Flixbus route | Input to flagging stage |

#### Key Design Decisions

- **Hard merge on Date_of_Extraction:** Prevents stale comparisons. A Feb 17 snapshot is never compared to Feb 16 prices.
- **Two-stage window (±90 then ±150):** Balances specificity (tight window for most routes) with coverage (relaxed window for sparse routes).
- **Soft scoring, no filtering:** A low-credibility operator (Soft_Score=0) is still included as a peer. Downweighting happens in downstream analysis (e.g., Power BI uses soft score for filtering). This preserves data and enables exploratory analysis.
- **Vectorised merge:** Single vectorised operation on 821K rows beats Python loops by 120×.

#### Example Calculation

**FlixBus Route:**
```
Row_ID: 12945
Route_Number: "Delhi_Mumbai_Express"
DOJ: 2026-03-20 (Friday)
Date_of_Extraction: 2026-03-17
Day_Type: "Weekend"
Bus_Category: "AC_Seater"
Departure_Mins: 870 (14:30)
Flix_Price: 720
Bus_Score: 4.2
```

**Matched Competitors (after hard merge):**
```
Competitor A: 
  Departure_Mins: 860 (gap=10 < 90 ✓)
  Price: 680, Rating_Band: B, Review_Tier: Tier2
  Soft_Score: 2 (match on rating & review)

Competitor B:
  Departure_Mins: 930 (gap=60 < 90 ✓)
  Price: 710, Rating_Band: A, Review_Tier: Tier1
  Soft_Score: 1 (rating match, different review tier)

Competitor C:
  Departure_Mins: 905 (gap=35 < 90 ✓)
  Price: 750, Rating_Band: C, Review_Tier: Tier3
  Soft_Score: 0 (no matches)
```

**Aggregated Peer Statistics:**
```
Peer_Count: 3
Peer_Median_Price: 710
Peer_Q1_Price: 695 (25th: between 680 and 710)
Peer_Q3_Price: 730 (75th: between 710 and 750)
Peer_Min_Price: 680
Peer_Max_Price: 750
Peer_Avg_Bus_Score: (4.1+4.2+3.8)/3 = 4.03
Window_Used_Mins: 90 (no relaxation, Peer_Count≥3)
Confidence: "High"
```

---

### Script 4 — Pricing Flag Engine (`04_flag.py`)

#### Overview
Applies two-stage statistical flagging to identify over/under-priced FlixBus routes. Stage 1 uses Interquartile Range (IQR) to detect outliers. Stage 2 adjusts for quality differences (e.g., premium operators justify higher prices). Final flag combines both stages.

#### Why This Step Exists
Raw peer prices vary widely. A simple "Flix price > median" rule creates false positives (valid premium pricing for superior quality) and false negatives (undercutting despite good quality). IQR-based flagging identifies true statistical outliers. Quality adjustment layers in business context: if a FlixBus bus is demonstrably better, the "overpricing" is justified and urgency is downgraded.

#### Inputs
- **File:** `03_similarity_summary.parquet`
- **Rows:** 30,888 FlixBus routes

#### The IQR Formula

**Interquartile Range (IQR):**
$$\text{IQR} = Q_3 - Q_1$$

**Bounds (outlier detection — standard box-plot method):**
$$\text{Lower Bound} = Q_1 - 1.5 \times \text{IQR}$$
$$\text{Upper Bound} = Q_3 + 1.5 \times \text{IQR}$$

**Decision rule:**
- If price > Upper Bound → **OVERPRICED**
- If price < Lower Bound → **UNDERPRICED**
- Otherwise → **OK**

**Multiplier 1.5:** Standard statistical convention. Configurable via `config.yaml: iqr_multiplier`

#### Operations Performed

**1. IQR Calculation (Vectorised)**

Per FlixBus row:
```python
IQR = Peer_Q3_Price - Peer_Q1_Price
```

**2. Bound Computation**

```python
IQR_Lower_Bound = Peer_Median_Price - (1.5 * IQR)
IQR_Upper_Bound = Peer_Median_Price + (1.5 * IQR)
```

**3. Stage 1 Flag (Raw Pricing Status)**

| Condition | Flag |
|-----------|------|
| Flix_Price > IQR_Upper_Bound | OVERPRICED |
| Flix_Price < IQR_Lower_Bound | UNDERPRICED |
| IQR_Lower_Bound ≤ Flix_Price ≤ IQR_Upper_Bound | OK |
| Flix_Price or Peer_Median is null | NO_DATA |
| Peer_Count == 0 | NO_PEERS |

**4. Price Deviation (Quantifies Distance from Market)**

```python
Price_Deviation_Abs = Flix_Price - Peer_Median_Price
Price_Deviation_Pct = (Price_Deviation_Abs / Peer_Median_Price) × 100
```

**Example:** Flix=750, Median=700 → Deviation_Abs=50, Deviation_Pct=7.14%

**5. Stage 2 Quality Adjustment (Context Layer)**

Checks if pricing premium/discount is explained by superior/inferior quality:

| Condition | Decision | Reasoning |
|-----------|----------|-----------|
| OVERPRICED AND Flix_Bus_Score > Peer_Avg_Bus_Score by >0.3 | **Quality_Justified** | Premium explained by better quality; downgrade urgency |
| UNDERPRICED AND Flix_Bus_Score < Peer_Avg_Bus_Score by >0.3 | **Quality_Consistent** | Discount explained by lower quality; expected |
| Neither condition met | **Not_Justified** | No quality explanation; flag stands at full strength |
| Bus Score is null for Flixbus or peers | **Score_Unavailable** | Cannot assess quality; flag remains unchanged |

**6. Final Flag (Combined)**

Combines Stage 1 and Stage 2:

```python
Final_Flag = Stage1_Flag + "_" + Stage2_Adjustment (if applicable)
```

**Examples:**
- OVERPRICED + Quality_Justified = **OVERPRICED_JUSTIFIED**
- UNDERPRICED + Quality_Consistent = **UNDERPRICED_CONSISTENT**
- OK + any adjustment = **OK** (no further action)

#### Output Columns Produced

| Column | Type | Description |
|--------|------|-------------|
| `IQR` | float | Interquartile range of peer prices |
| `IQR_Lower_Bound` | float | Q1 − 1.5×IQR |
| `IQR_Upper_Bound` | float | Q3 + 1.5×IQR |
| `Price_Deviation_Abs` | float | Flix_Price − Peer_Median |
| `Price_Deviation_Pct` | float | (deviation / median) × 100 |
| `Stage1_Flag` | string | OVERPRICED / UNDERPRICED / OK / NO_DATA / NO_PEERS |
| `Quality_Adjustment` | string | Justified / Consistent / Not_Justified / Score_Unavailable |
| `Final_Flag` | string | Combined stage 1 + 2 result |

#### Output Distribution (30,888 rows)

| Final Flag | Count | % | Action |
|-----------|-------|-----|--------|
| OVERPRICED | 1,284 | 4.2% | Critical review; consider discounting |
| OVERPRICED_JUSTIFIED | 1,018 | 3.3% | Monitor; acceptable premium |
| UNDERPRICED | 4,453 | 14.4% | High priority; raise fares |
| UNDERPRICED_CONSISTENT | - | - | (example; not in sample) |
| OK | 23,470 | 76.0% | No action; competitive pricing |
| NO_DATA | 663 | 2.1% | Insufficient peer data |
| **Total** | **30,888** | **100%** | |

#### Key Design Decisions

- **IQR multiplier = 1.5:** Box-plot standard. Catches ~99.3% of normal distribution data as outliers at ±1.5×IQR. Tunable in `config.yaml`.
- **Quality threshold > 0.3:** A small score difference is noise. Only differences ≥0.3 (e.g., 4.5 vs 4.2) trigger quality justification. Conservative to avoid false excuses for pricing.
- **Score_Unavailable as distinct category:** Preserves flag even if quality data missing (don't suppress the signal). Downstream reports can filter or escalate.
- **Two-stage design:** Separates statistical detection (Stage 1) from business context (Stage 2). Enables independent tuning and debugging.

#### Example Calculation

**Sample Row Data:**
```
Flix_Price: 750
Peer_Q1_Price: 680
Peer_Q3_Price: 720
Peer_Median_Price: 700
Peer_Min_Price: 650
Peer_Max_Price: 800
Flix_Bus_Score: 4.4
Peer_Avg_Bus_Score: 4.0
```

**Stage 1 Calculations:**

```
Step 1: Calculate IQR
IQR = 720 - 680 = 40

Step 2: Calculate bounds
IQR_Lower_Bound = 700 - (1.5 × 40) = 700 - 60 = 640
IQR_Upper_Bound = 700 + (1.5 × 40) = 700 + 60 = 760

Step 3: Compare Flix_Price to bounds
Flix_Price (750) < IQR_Upper_Bound (760) ✓
Flix_Price (750) > IQR_Lower_Bound (640) ✓
→ Within bounds: Flag = "OK"
```

**Stage 2 Calculations:**

```
Price_Deviation_Abs = 750 - 700 = +50
Price_Deviation_Pct = (50 / 700) × 100 = 7.14%

Stage1_Flag = "OK"
Quality_Adjustment: N/A (OK flag, no adjustment applied)

Final_Flag = "OK"
```

**Alternative Example: OVERPRICED**

```
If Flix_Price = 830 (instead of 750):

IQR_Lower_Bound = 640
IQR_Upper_Bound = 760

830 > 760 → OVERPRICED (Stage 1)

Quality check:
Flix_Bus_Score (4.4) - Peer_Avg_Bus_Score (4.0) = 0.4 > 0.3 ✓
→ Quality_Justified (Stage 2)

Final_Flag = "OVERPRICED_JUSTIFIED"
```

---

### Script 5 — Urgency Scoring (`05_urgency.py`)

- Cross-references flags with occupancy data
- Assigns urgency label (Critical / High / Medium / Low) and numeric score
- Estimates revenue impact per flagged route
- **Output:** `05_final_output.parquet`

---

### Step 6 — Power BI

- Dataflows read `05_final_output.parquet` from SharePoint
- Power Query handles light transforms (data types, buckets, colour codes)
- DAX measures compute KPIs
- Published report auto-refreshes on dataset trigger

---

## Configuration

All thresholds are stored in `config/config.yaml`. No code changes are needed to tune the pipeline.

```yaml
iqr_multiplier: 1.5          # IQR fence multiplier for flagging
peer_group_min: 3             # minimum peers required for a valid group
departure_window_days: 3      # ± days for departure time matching
occupancy_threshold_high: 0.80
occupancy_threshold_low: 0.40
urgency_score_critical: 90
urgency_score_high: 70
urgency_score_medium: 50
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Backend | Python 3.10+, pandas, numpy, pyarrow, openpyxl, pyyaml |
| Orchestration | Microsoft Power Automate |
| Storage | SharePoint / OneDrive |
| BI | Power BI Service (Dataflows, DAX, Published Report) |

---

## Setup & Installation

```bash
# 1. Clone the repo
git clone https://github.com/iamishaanpandey/Flix.in-Pricing-Strategy.git
cd Flix.in-Pricing-Strategy

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure thresholds
# Edit config/config.yaml as needed

# 5. Run the pipeline manually (optional, normally triggered by Power Automate)
python 01_ingest.py
python 02_features.py
python 03_similarity.py
python 04_flag.py
python 05_urgency.py
```

---

## Requirements

See `requirements.txt` for all dependencies and their pinned versions.

---

## Repeatability

A new `pricing_YYYY-MM-DD.csv` dropped into the SharePoint folder automatically triggers the entire pipeline. All competitor API data is consolidated into one Excel file, uploaded to SharePoint, processed through Python, and the pricing team receives an email when the Power BI report is ready. **No manual steps required after initial setup.**

---

## Author

**Ishaan Pandey**
GitHub: [@iamishaanpandey](https://github.com/iamishaanpandey)
