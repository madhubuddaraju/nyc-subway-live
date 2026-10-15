"""Turn the stream job's output into the live board (live.json) and the running reliability history (history.json).

python -m subway.live --out data/live [--history-in previous/history.json]

Line status rules. Wait assessment (MTA): share of intervals within 25% of the scheduled interval, last 30 minutes.
A gap incident is a direction of a line with any platform overdue; one late train opens a gap at every station it
hasn't reached yet, so platforms are grouped by direction rather than counted one by one.
    good     wait assessment >= 75% and no gap open
    delays   wait assessment 60-75%, or a gap open in one direction
    severe   wait assessment < 60%, gaps open in both directions, or a wait of 3x the scheduled interval
"""

import argparse
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from .logic import NYC, day_type

ROOT = Path(__file__).resolve().parents[2]
log = logging.getLogger("live")
RECENT_MIN = 30


def _read(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path) if path.exists() and any(path.rglob("*.parquet")) else pd.DataFrame()
    except Exception as e:  # a file being written right now: try again next cycle
        log.warning("could not read %s: %s", path, e)
        return pd.DataFrame()


def status(wa: float | None, gap_directions: int, worst_ratio: float = 0.0) -> str:
    if wa is None and not gap_directions:
        return "no data"
    if (wa is not None and wa < 0.60) or gap_directions >= 2 or worst_ratio >= 3:
        return "severe"
    if (wa is not None and wa < 0.75) or gap_directions >= 1:
        return "delays"
    return "good"


def board(stream_dir: Path, static_dir: Path, now: int | None = None) -> dict:
    now = now or int(time.time())
    hw = _read(stream_dir / "headways")
    alerts = _read(stream_dir / "alerts")
    rel = _read(stream_dir / "reliability")
    sp = static_dir / "stops.parquet"
    stops = pd.read_parquet(sp).set_index("stop_id") if sp.exists() else pd.DataFrame()
    name = lambda s: stops["name"].get(s, s) if len(stops) else s  # noqa: E731

    open_gaps = pd.DataFrame()
    if len(alerts):
        alerts = alerts.sort_values("arrival_ts")
        latest = alerts.groupby(["route_id", "stop_id"]).tail(1)            # an alert stays open until its gap_closed
        open_gaps = latest[(latest.status == "open_gap") & (latest.arrival_ts >= now - 10 * 60)]

    recent = hw[hw.arrival_ts >= now - RECENT_MIN * 60] if len(hw) else hw
    lines = []
    for route in sorted(set(hw.route_id) if len(hw) else set()):
        r = recent[recent.route_id == route]
        scored = r[r.status.isin(["ok", "late", "gap", "bunched"])]
        wa = float((scored.status == "ok").mean()) if len(scored) >= 5 else None
        og = open_gaps[open_gaps.route_id == route] if len(open_gaps) else open_gaps
        directions = og.stop_id.str[-1].nunique() if len(og) else 0
        worst = float((og.headway_min / og.sched_min).max()) if len(og) and og.sched_min.notna().any() else 0.0
        lines.append({"route": route, "status": status(wa, directions, worst), "wait_assessment": None if wa is None else round(wa, 3),
                      "headways": int(len(scored)), "gaps": int((scored.status == "gap").sum()),
                      "bunched": int((scored.status == "bunched").sum()), "open_gaps": int(len(og)), "gap_directions": int(directions),
                      "median_headway": None if r.empty else round(float(r.headway_min.median()), 1)})
    gaps = []
    for g in open_gaps.itertuples() if len(open_gaps) else []:
        gaps.append({"route": g.route_id, "stop_id": g.stop_id, "station": name(g.stop_id),
                     "direction": "northbound" if g.stop_id.endswith("N") else "southbound",
                     "waited_min": round(float(g.headway_min), 1), "scheduled_min": g.sched_min,
                     "severity": round(float(g.headway_min) / float(g.sched_min), 1) if g.sched_min else None})
    gaps.sort(key=lambda g: -(g["severity"] or 0))
    worst = []
    if len(hw):
        bad = hw[hw.status == "gap"].groupby(["route_id", "stop_id"]).size().sort_values(ascending=False).head(10)
        worst = [{"route": r, "station": name(s), "direction": "northbound" if s.endswith("N") else "southbound", "gaps": int(n)}
                 for (r, s), n in bad.items()]
    windows = []
    if len(rel):
        rel = rel[rel.headways >= 3].sort_values("window_start")
        windows = [{"t": pd.Timestamp(w.window_start).isoformat(), "route": w.route_id, "wa": round(float(w.wait_assessment), 3),
                    "headways": int(w.headways)} for w in rel.itertuples()]
    session_start = int(hw.arrival_ts.min()) if len(hw) else now
    return {"generated_at": datetime.fromtimestamp(now, UTC).isoformat(timespec="seconds"), "session_start": session_start,
            "lines": lines, "open_gaps": gaps, "worst_platforms": worst, "windows": windows,
            "totals": {"headways": int(len(hw)), "gaps": int((hw.status == "gap").sum()) if len(hw) else 0,
                       "bunched": int((hw.status == "bunched").sum()) if len(hw) else 0}}


def merge_history(previous: dict | None, stream_dir: Path, session_id: str) -> dict:
    """Add this session's headways to running totals per line x day type x hour (New York time) and per day."""
    hist = previous or {"sessions": [], "by_hour": {}, "by_day": {}}
    if session_id in hist["sessions"]:
        return hist
    hw = _read(stream_dir / "headways")
    if hw.empty:
        return hist
    hw = hw[hw.status.isin(["ok", "late", "gap", "bunched"])].copy()
    local = pd.to_datetime(hw.arrival_ts, unit="s", utc=True).dt.tz_convert(NYC)
    hw["hour"], hw["date"] = local.dt.hour, local.dt.strftime("%Y-%m-%d")
    hw["day_type"] = [day_type(int(t)) for t in hw.arrival_ts]
    for (route, dt, hour), g in hw.groupby(["route_id", "day_type", "hour"]):
        cell = hist["by_hour"].setdefault(f"{route}|{dt}|{hour}", [0, 0, 0, 0])
        cell[0] += len(g)
        cell[1] += int((g.status == "ok").sum())
        cell[2] += int((g.status == "gap").sum())
        cell[3] += int((g.status == "bunched").sum())
    for (route, d), g in hw.groupby(["route_id", "date"]):
        cell = hist["by_day"].setdefault(f"{route}|{d}", [0, 0, 0])
        cell[0] += len(g)
        cell[1] += int((g.status == "ok").sum())
        cell[2] += int((g.status == "gap").sum())
    hist["sessions"] = (hist["sessions"] + [session_id])[-2000:]
    return hist


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default=str(ROOT / "data" / "stream"))
    ap.add_argument("--static", default=str(ROOT / "data" / "static"))
    ap.add_argument("--out", default=str(ROOT / "data" / "live"))
    ap.add_argument("--history-in", help="previous history.json to extend (end of a session)")
    ap.add_argument("--session", help="session id; when given, this session is added to history.json")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    b = board(Path(a.stream), Path(a.static))
    (out / "live.json").write_text(json.dumps(b, separators=(",", ":")))
    log.info("live board: %d lines, %d open gaps, %d headways", len(b["lines"]), len(b["open_gaps"]), b["totals"]["headways"])
    if a.session:
        prev = json.loads(Path(a.history_in).read_text()) if a.history_in and Path(a.history_in).exists() else None
        h = merge_history(prev, Path(a.stream), a.session)
        (out / "history.json").write_text(json.dumps(h, separators=(",", ":")))
        log.info("history: %d sessions", len(h["sessions"]))


if __name__ == "__main__":
    main()
