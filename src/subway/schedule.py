"""Scheduled service from the MTA's static timetable (GTFS): stations, and how often each line is due at each platform.

python -m subway.schedule        # downloads the timetable, writes data/static/stops.parquet and scheduled_headways.parquet

A platform is a stop id with its direction ('A27N' = 42 St-Port Authority, northbound). The scheduled headway is the
typical minutes between trains of one line at one platform, by day type and hour. The stream job compares every
observed headway against it.
"""

import io
import logging
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
STATIC = ROOT / "data" / "static"
URLS = ["https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip",
        "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"]
DAY_TYPES = {"weekday": ["monday", "tuesday", "wednesday", "thursday", "friday"], "saturday": ["saturday"],
             "sunday": ["sunday"]}
log = logging.getLogger("schedule")


def download() -> zipfile.ZipFile:
    for url in URLS:
        try:
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            log.info("timetable from %s (%.1f MB)", url, len(r.content) / 1e6)
            return zipfile.ZipFile(io.BytesIO(r.content))
        except requests.RequestException as e:
            log.warning("%s failed: %s", url, e)
    raise SystemExit("could not download the static timetable")


def read(z: zipfile.ZipFile, name: str, **kw) -> pd.DataFrame:
    return pd.read_csv(z.open(name), dtype=str, **kw)


def day_type_of_services(calendar: pd.DataFrame) -> dict[str, str]:
    """service_id -> weekday/saturday/sunday (a service running Monday-Friday is a weekday service, and so on)."""
    out = {}
    for _, r in calendar.iterrows():
        for dt, days in DAY_TYPES.items():
            if all(r[d] == "1" for d in days):
                out[r["service_id"]] = dt
                break
    return out


def seconds(hms: pd.Series) -> pd.Series:
    """'25:10:00' style GTFS times (can pass 24:00) -> seconds after midnight of the service day."""
    p = hms.str.split(":", expand=True).astype(int)
    return p[0] * 3600 + p[1] * 60 + p[2]


def scheduled_headways(stop_times: pd.DataFrame, trips: pd.DataFrame, services: dict[str, str]) -> pd.DataFrame:
    st = stop_times.merge(trips[["trip_id", "route_id", "service_id"]], on="trip_id")
    st["day_type"] = st["service_id"].map(services)
    st = st.dropna(subset=["day_type"])
    st["sec"] = seconds(st["arrival_time"].fillna(st["departure_time"])) % 86400
    st = st.sort_values(["stop_id", "route_id", "day_type", "sec"]).drop_duplicates(["stop_id", "route_id", "day_type", "sec"])
    st["gap"] = st.groupby(["stop_id", "route_id", "day_type"])["sec"].diff() / 60
    st["hour"] = st["sec"] // 3600
    g = st.dropna(subset=["gap"]).groupby(["stop_id", "route_id", "day_type", "hour"])
    out = g["gap"].median().rename("sched_headway_min").reset_index()
    out["trains_per_hour"] = g.size().values
    out["hour"] = out["hour"].astype(int)
    out["sched_headway_min"] = out["sched_headway_min"].round(1)
    return out


def build(z: zipfile.ZipFile | None = None) -> None:
    z = z or download()
    STATIC.mkdir(parents=True, exist_ok=True)
    stops = read(z, "stops.txt")
    stops = stops.assign(lat=stops["stop_lat"].astype(float), lon=stops["stop_lon"].astype(float))
    platform = stops[stops["stop_id"].str[-1].isin(["N", "S"])]
    stations = stops.set_index("stop_id")
    platform = platform.assign(station_id=platform["stop_id"].str[:-1], direction=platform["stop_id"].str[-1])
    platform["name"] = platform["station_id"].map(stations["stop_name"]).fillna(platform["stop_name"])
    platform[["stop_id", "station_id", "direction", "name", "lat", "lon"]].to_parquet(STATIC / "stops.parquet", index=False)

    services = day_type_of_services(read(z, "calendar.txt"))
    trips = read(z, "trips.txt")
    stop_times = read(z, "stop_times.txt", usecols=["trip_id", "stop_id", "arrival_time", "departure_time"])
    sh = scheduled_headways(stop_times, trips, services)
    sh.to_parquet(STATIC / "scheduled_headways.parquet", index=False)
    routes = read(z, "routes.txt")
    cols = ["route_id", "route_short_name", "route_long_name", "route_color", "route_text_color"]
    routes[[c for c in cols if c in routes.columns]].to_parquet(STATIC / "routes.parquet", index=False)
    log.info("%d platforms, %d scheduled headway rows, %d routes", len(platform), len(sh), len(routes))
    peak = sh[(sh.day_type == "weekday") & sh.hour.between(7, 9)]
    log.info("median weekday peak headway across platforms: %.1f min", float(np.median(peak["sched_headway_min"])))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    build()
