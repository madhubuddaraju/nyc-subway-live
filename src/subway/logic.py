"""Stateful rules for the stream job, as plain functions so they can be tested without Spark.

Two kinds of state:
- per train trip: the stops it is still predicted to reach. When a stop drops off the list, the train has
  arrived there, at its last predicted time. This is how arrivals are inferred from GTFS-realtime.
- per platform and line: the last arrival. Each new arrival gives a headway (minutes since the previous
  train), compared with the timetable. If no train comes for too long, an open-gap alert is raised before
  the next train shows up, which is what a rider on the platform needs to know.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

NYC = ZoneInfo("America/New_York")
ARRIVED_GRACE = 90          # a stop that disappears is counted as served only if the train was due within 90 s
TRIP_TIMEOUT = 5 * 60       # no update for 5 minutes: the trip has finished (it reached its terminal)


# ----------------------------------------------------------------------------- trips -> arrivals

def track_trip(state: dict | None, events: list[dict]) -> tuple[dict, list[dict]]:
    """Update one trip's state with new feed events (any order) and return the arrivals they reveal."""
    state = state or {"stops": {}, "last_ts": 0, "route_id": None, "direction": None}
    arrivals = []
    for e in sorted(events, key=lambda x: x["feed_ts"]):
        if e["feed_ts"] <= state["last_ts"]:
            continue                                   # stale or duplicate poll
        current = {s["stop_id"]: int(s["t"]) for s in e["stops"]}
        for stop, t in state["stops"].items():
            if stop not in current and t <= e["feed_ts"] + ARRIVED_GRACE:
                arrivals.append(arrival(e, stop, min(t, e["feed_ts"])))
        state = {"stops": current, "last_ts": e["feed_ts"], "route_id": e["route_id"], "direction": e["direction"]}
    return state, arrivals


def finish_trip(state: dict, trip_uid: str) -> list[dict]:
    """On timeout: the train reached the stops it was about to reach when the feed stopped reporting it (the terminal)."""
    e = {"trip_uid": trip_uid, "route_id": state["route_id"], "direction": state["direction"], "feed_ts": state["last_ts"]}
    return [arrival(e, s, min(t, state["last_ts"] + ARRIVED_GRACE)) for s, t in state["stops"].items()
            if t <= state["last_ts"] + ARRIVED_GRACE]


def arrival(e: dict, stop_id: str, ts: int) -> dict:
    return {"trip_uid": e["trip_uid"], "route_id": e["route_id"], "direction": e["direction"], "stop_id": stop_id,
            "arrival_ts": int(ts)}


# ----------------------------------------------------------------------------- arrivals -> headways and alerts

def day_type(ts: int) -> str:
    d = datetime.fromtimestamp(ts, NYC).weekday()
    return "weekday" if d < 5 else ("saturday" if d == 5 else "sunday")


def hour_of(ts: int) -> int:
    return datetime.fromtimestamp(ts, NYC).hour


def gap_threshold(sched: float) -> float:
    """Minutes without a train that count as a service gap: twice the scheduled headway, and at least 8 minutes more."""
    return max(2 * sched, sched + 8)


def classify(headway: float, sched: float | None) -> str:
    """MTA-style wait assessment: a headway within 25% of the scheduled one meets the standard."""
    if sched is None:
        return "unscheduled"
    if headway >= gap_threshold(sched):
        return "gap"
    if headway <= 2 and sched >= 4:
        return "bunched"
    return "ok" if headway <= sched * 1.25 else "late"


def track_platform(state: dict | None, arrivals: list[dict], sched_lookup) -> tuple[dict, list[dict]]:
    """Headway rows and gap-closed alerts for one (line, platform). sched_lookup(stop, route, day_type, hour) -> minutes or None."""
    state = state or {"last_ts": 0, "last_trip": "", "gap_open_since": 0}
    out = []
    for a in sorted(arrivals, key=lambda x: x["arrival_ts"]):
        if a["arrival_ts"] <= state["last_ts"] or a["trip_uid"] == state["last_trip"]:
            continue
        if state["last_ts"]:
            headway = (a["arrival_ts"] - state["last_ts"]) / 60
            if headway <= 120:                       # longer means service had ended; not a headway
                sched = sched_lookup(a["stop_id"], a["route_id"], day_type(a["arrival_ts"]), hour_of(a["arrival_ts"]))
                out.append({"kind": "headway", **a, "headway_min": round(headway, 2),
                            "sched_min": sched, "status": classify(headway, sched)})
        if state["gap_open_since"]:
            out.append({"kind": "gap_closed", **a, "headway_min": round((a["arrival_ts"] - state["last_ts"]) / 60, 2),
                        "sched_min": None, "status": "gap_closed"})
        state = {"last_ts": a["arrival_ts"], "last_trip": a["trip_uid"], "gap_open_since": 0}
    return state, out


def check_open_gap(state: dict, key: dict, now: int, sched_lookup) -> tuple[dict, list[dict]]:
    """Called when a platform has been quiet: raise (or refresh) an open-gap alert if the wait is far beyond schedule."""
    if not state.get("last_ts"):
        return state, []
    sched = sched_lookup(key["stop_id"], key["route_id"], day_type(now), hour_of(now))
    waited = (now - state["last_ts"]) / 60
    if sched is None or waited < gap_threshold(sched) or waited > 180:
        return state, []
    state = {**state, "gap_open_since": state.get("gap_open_since") or now}
    return state, [{"kind": "open_gap", "trip_uid": "", "route_id": key["route_id"], "direction": key["stop_id"][-1],
                    "stop_id": key["stop_id"], "arrival_ts": now, "headway_min": round(waited, 2), "sched_min": sched,
                    "status": "open_gap"}]


def next_check_minutes(state: dict, key: dict, now: int, sched_lookup) -> float:
    """How long to wait before checking a quiet platform again."""
    sched = sched_lookup(key["stop_id"], key["route_id"], day_type(now), hour_of(now)) or 10
    if state.get("gap_open_since"):
        return 2.0                                   # keep an open alert fresh
    return max(1.0, gap_threshold(sched) - (now - state.get("last_ts", now)) / 60)
