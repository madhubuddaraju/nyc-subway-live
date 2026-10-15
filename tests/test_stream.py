"""End-to-end run of the Spark job in file mode on a simulated line (no Kafka needed). Takes about a minute."""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
STOPS = ["X1N", "X2N", "X3N"]
TRAVEL = 120              # seconds between stops


def simulate(start: int, departures_min: list[int], minutes: int) -> list[list[dict]]:
    """Polls every 30 s: each train predicts its remaining stops; a stop drops off once the train has passed it."""
    polls = []
    for k in range(minutes * 2):
        now = start + k * 30
        events = []
        for i, d in enumerate(departures_min):
            t0 = start + d * 60
            ahead = [{"stop_id": s, "t": t0 + j * TRAVEL} for j, s in enumerate(STOPS) if t0 + j * TRAVEL > now - 10]
            if t0 - 600 <= now and ahead:
                events.append({"feed": "TEST", "feed_ts": now, "trip_uid": f"d_{i}", "route_id": "X", "direction": "N", "stops": ahead})
        polls.append(events)
    return polls


@pytest.mark.skipif(shutil.which("java") is None, reason="needs Java for Spark")
def test_stream_job_produces_headways_with_statuses(tmp_path):
    start = int(time.time()) - 60 * 60
    deps = [0, 6, 12, 13, 19, 40, 46]          # 13 is bunched behind 12; 19 -> 40 is a gap
    inp = tmp_path / "in"
    inp.mkdir()
    for n, events in enumerate(simulate(start, deps, 55)):
        f = inp / f"poll-{n:04d}.json"
        f.write_text("".join(json.dumps(e) + "\n" for e in events))
        os.utime(f, (start + n * 30, start + n * 30))   # Spark's file source orders files by modification time
    static = tmp_path / "static"
    static.mkdir()
    pd.DataFrame([{"stop_id": s, "route_id": "X", "day_type": d, "hour": h, "sched_headway_min": 6.0, "trains_per_hour": 10}
                  for s in STOPS for d in ("weekday", "saturday", "sunday") for h in range(24)]
                 ).to_parquet(static / "scheduled_headways.parquet")
    env = {**os.environ, "SUBWAY_OUT": str(tmp_path / "out"), "SUBWAY_STATIC": str(static), "PYTHONPATH": str(ROOT / "src")}
    subprocess.run([sys.executable, "-m", "subway.stream", "--source", "files", "--input", str(inp), "--minutes", "2.5"],
                   env=env, check=True, timeout=600)
    hw = pd.read_parquet(tmp_path / "out" / "headways")
    x1 = hw[hw.stop_id == "X1N"].sort_values("arrival_ts")
    assert list(x1.headway_min.round()) == [6, 6, 1, 6, 21, 6]
    assert list(x1.status) == ["ok", "ok", "bunched", "ok", "gap", "ok"]
    # the terminal is only recorded when the trip times out (5 minutes after the feed stops reporting it)
    assert set(hw.stop_id) >= {"X1N", "X2N"}
    x2 = hw[hw.stop_id == "X2N"].sort_values("arrival_ts")
    assert list(x2.status) == list(x1.status)
