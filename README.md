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

### Step 1 — Data Drop
- New `pricing_YYYY-MM-DD.csv` placed in SharePoint `/FlixBus/Data/Raw/`
- Power Automate watches this folder via file-created trigger

### Step 2 — Power Automate
- Sends file path to Python via HTTP call
- Archives raw file to `/Processed/` after completion
- Triggers Power BI dataset refresh via REST API
- Sends email notification to pricing team

### Step 3 — `01_ingest.py`
- Loads file, validates schema, enforces data types
- Fills NAs with business-logic defaults
- Drops zero-variance columns and true duplicates
- Output: `01_validated.parquet`

### Step 4 — `02_features.py`
Derives the following columns:
| Column | Description |
|---|---|
| `Day_Type` | Weekday vs weekend classification |
| `Bus_Category` | Operator tier bucket |
| `Occupancy_Pct` | Seats filled as percentage |
| `Departure_Mins` | Departure time in minutes from midnight |
| `Fare_Min_Price` | Minimum fare in the peer group |
| `Rating_Band` | Rating bucketed into Low / Mid / High |
| `Review_Tier` | Review count tier |
| `Operator_Size` | Operator scale classification |
| `Days_To_Departure` | Days between scrape date and departure |

Output: `02_featured.parquet`

### Step 5 — `03_similarity.py`
- Vectorised peer grouping via `pd.merge` on hard filters
- Departure window filter
- Soft scoring for borderline peers
- Peer statistics computed via `groupby`
- Output: `03_peer_groups.parquet`, `03_similarity_summary.parquet`

### Step 6 — `04_flag.py`
- Stage 1: IQR statistical flag
- Stage 2: Quality adjustment
- Outputs flag direction (over/under-priced) and magnitude per FlixBus row
- Output: `04_flags.parquet`

### Step 7 — `05_urgency.py`
- Cross-references flags with occupancy data
- Assigns urgency label (Critical / High / Medium / Low) and numeric score
- Estimates revenue impact per flagged route
- Output: `05_final_output.parquet` — feeds Power BI directly

### Step 8 — Power BI
- Dataflows read `final_output.parquet` from SharePoint
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
