"""Summarise a capture run: feeds seen, trains, and what the stream job made of it (reports/capture_summary.json)."""
import gzip
import json
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

cap, out = Path(sys.argv[1]), Path(sys.argv[2])
events = [json.loads(line) for f in sorted(cap.glob("poll-*.json")) for line in f.read_text().splitlines() if line]
last = {}                                            # each feed has its own timestamp: use its newest poll
for e in events:
    last[e["feed"]] = max(last.get(e["feed"], 0), e["feed_ts"])
summary = {"polls": len(list(cap.glob("poll-*.json"))), "train_updates": len(events),
           "trains_by_route": dict(Counter(e["route_id"] for e in events if e["feed_ts"] == last[e["feed"]])),
           "stops_per_update": round(sum(len(e["stops"]) for e in events) / max(len(events), 1), 1)}
hw_dir = out / "headways"
if hw_dir.exists():
    hw = pd.read_parquet(hw_dir)
    summary["headways"] = len(hw)
    summary["status_counts"] = hw["status"].value_counts().to_dict()
    cols = ["route_id", "stop_id", "headway_min", "sched_min", "status"]
    summary["headway_examples"] = hw.sort_values("arrival_ts").head(15)[cols].to_dict("records")
Path("reports").mkdir(exist_ok=True)
Path("reports/capture_summary.json").write_text(json.dumps(summary, indent=1, default=str))
Path("tests/fixtures").mkdir(parents=True, exist_ok=True)
with gzip.open("tests/fixtures/capture.jsonl.gz", "wt") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")
print(json.dumps({k: v for k, v in summary.items() if k != "headway_examples"}, indent=1))
