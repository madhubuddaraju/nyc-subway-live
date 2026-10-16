"""Poll the MTA feeds and publish train updates to Kafka (or to JSON-lines files for local runs).

python -m subway.producer --minutes 50                         # Kafka at localhost:9092, topic trip_updates
python -m subway.producer --minutes 10 --out tests/fixtures/capture   # write files instead (no Kafka needed)
"""

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from .feeds import FEEDS, fetch, parse

log = logging.getLogger("producer")
POLL_SECONDS = 30


class KafkaSink:
    def __init__(self, servers: str, topic: str):
        from confluent_kafka import Producer
        self.p = Producer({"bootstrap.servers": servers, "linger.ms": 50, "compression.type": "zstd",
                           "enable.idempotence": True})
        self.topic = topic

    def send(self, events: list[dict]) -> None:
        for e in events:
            self.p.produce(self.topic, key=e["trip_uid"].encode(), value=json.dumps(e).encode())
            self.p.poll(0)
        self.p.flush(10)


class FileSink:
    """One JSON-lines file per poll, written atomically so a Spark file source never reads half a file."""

    def __init__(self, out: str):
        self.dir = Path(out)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.n = 0

    def send(self, events: list[dict]) -> None:
        self.n += 1
        tmp = self.dir / f".poll-{self.n:06d}.tmp"
        tmp.write_text("".join(json.dumps(e) + "\n" for e in events))
        tmp.rename(self.dir / f"poll-{int(time.time())}-{self.n:06d}.json")


def poll_once(session: requests.Session) -> tuple[list[dict], dict]:
    def one(item):
        name, url = item
        try:
            return name, parse(fetch(url, session), name), None
        except Exception as e:  # one failing feed must not stop the others
            return name, [], str(e)

    events, health = [], {}
    with ThreadPoolExecutor(len(FEEDS)) as ex:
        for name, evs, err in ex.map(one, FEEDS.items()):
            events += evs
            health[name] = {"trains": len(evs), "error": err}
    return events, health


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=50)
    ap.add_argument("--out", help="write JSON-lines files here instead of Kafka")
    ap.add_argument("--servers", default=os.environ.get("KAFKA_SERVERS", "localhost:9092"))
    ap.add_argument("--topic", default="trip_updates")
    a = ap.parse_args()
    sink = FileSink(a.out) if a.out else KafkaSink(a.servers, a.topic)
    session = requests.Session()
    session.headers["User-Agent"] = "nyc-subway-live (github.com/subbarajubuddaraju/nyc-subway-live)"
    end = time.time() + a.minutes * 60
    while time.time() < end:
        t0 = time.time()
        events, health = poll_once(session)
        sink.send(events)
        bad = {k: v["error"] for k, v in health.items() if v["error"]}
        log.info("published %d train updates%s", len(events), f"; failed feeds: {bad}" if bad else "")
        time.sleep(max(0.0, POLL_SECONDS - (time.time() - t0)))


if __name__ == "__main__":
    main()
