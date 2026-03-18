"""
run_pipeline.py
───────────────
Runs all 5 scripts in sequence.
Usage: python run_pipeline.py
"""

import subprocess
import sys
import time
from datetime import datetime

scripts = [
    ("01_ingest.py",     "Ingest & Validate"),
    ("02_features.py",   "Feature Engineering"),
    ("03_similarity.py", "Similarity Engine"),
    ("04_flag.py",       "Pricing Flag Engine"),
    ("05_urgency.py",    "Urgency Scoring"),
]

print("=" * 60)
print("  FlixBus Pricing Intelligence Pipeline")
print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

total_start = time.time()

for script, name in scripts:
    print(f"\n▶ Running {script} — {name} ...")
    start = time.time()
    result = subprocess.run([sys.executable, script], capture_output=False)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n❌ {script} FAILED (exit code {result.returncode})")
        print("Pipeline stopped.")
        sys.exit(1)

    print(f"  ✓ {script} completed in {elapsed:.1f}s")

total = time.time() - total_start
print(f"\n{'=' * 60}")
print(f"  ✅ All scripts completed in {total:.1f}s")
print(f"  Outputs in: outputs/")
print(f"  Key file:   outputs/05_final_output.csv")
print(f"{'=' * 60}")
