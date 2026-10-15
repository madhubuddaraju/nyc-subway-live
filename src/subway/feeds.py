"""MTA subway GTFS-realtime feeds: download and flatten into one JSON event per train per poll.

Each event carries the train's remaining predicted stops. Arrivals are not in the feed directly; the stream
job infers them when a stop drops off a train's prediction list (see stream.py).
"""

import time

import requests
from google.transit import gtfs_realtime_pb2

BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
FEEDS = {
    "1234567S": BASE,             # 1 2 3 4 5 6 7 and 42 St shuttle
    "ACEH": BASE + "-ace",        # A C E and Rockaway shuttle
    "BDFM": BASE + "-bdfm",
    "G": BASE + "-g",
    "JZ": BASE + "-jz",
    "NQRW": BASE + "-nqrw",
    "L": BASE + "-l",
    "SIR": BASE + "-si",
}


def fetch(url: str, session: requests.Session | None = None, timeout: int = 20) -> bytes:
    r = (session or requests).get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def parse(blob: bytes, feed: str) -> list[dict]:
    """One event per trip: route, direction and the remaining (stop_id, predicted arrival) list."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(blob)
    feed_ts = int(msg.header.timestamp) or int(time.time())
    out = []
    for ent in msg.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        trip = tu.trip
        stops = []
        for stu in tu.stop_time_update:
            t = stu.arrival.time if stu.HasField("arrival") and stu.arrival.time else (
                stu.departure.time if stu.HasField("departure") else 0)
            if stu.stop_id and t:
                stops.append({"stop_id": stu.stop_id, "t": int(t)})
        if not stops or not trip.route_id:
            continue
        direction = stops[0]["stop_id"][-1] if stops[0]["stop_id"][-1] in "NS" else "?"
        out.append({
            "feed": feed,
            "feed_ts": feed_ts,
            "trip_uid": f"{trip.start_date}_{trip.trip_id}",
            "route_id": route_name(trip.route_id),
            "direction": direction,
            "stops": stops,
        })
    return out


def route_name(route_id: str) -> str:
    """Feed route ids as published, except the Staten Island Railway ('SI'), shown as 'SIR'."""
    return "SIR" if route_id in ("SI", "SS") else route_id
