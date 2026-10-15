"""Spark Structured Streaming job: train updates -> arrivals -> headways and gap alerts -> 15-minute reliability.

python -m subway.stream --source kafka --minutes 50          # Kafka topics trip_updates -> arrivals -> alerts
python -m subway.stream --source files --input tests/fixtures/capture --minutes 3   # no Kafka needed

Three queries, joined by topics (or folders in file mode) so each stage can be replayed and scaled on its own:
  Q1  trip_updates  --dedupe, stateful per trip-->            arrivals
  Q2  arrivals      --stateful per line+platform, event-time timeouts--> headways (parquet) + alerts (parquet, Kafka)
  Q3  headways      --15-minute event-time windows-->         reliability (parquet)
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming.state import GroupStateTimeout

from . import logic

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(os.environ.get("SUBWAY_OUT", ROOT / "data" / "stream"))
STATIC = Path(os.environ.get("SUBWAY_STATIC", ROOT / "data" / "static"))
log = logging.getLogger("stream")
TRIGGER = os.environ.get("SUBWAY_TRIGGER", "15 seconds")                 # micro-batch interval
MAX_FILES = int(os.environ.get("SUBWAY_MAX_FILES", "20"))               # file mode: polls per micro-batch

EVENT = T.StructType([
    T.StructField("feed", T.StringType()), T.StructField("feed_ts", T.LongType()),
    T.StructField("trip_uid", T.StringType()), T.StructField("route_id", T.StringType()),
    T.StructField("direction", T.StringType()),
    T.StructField("stops", T.ArrayType(T.StructType([T.StructField("stop_id", T.StringType()), T.StructField("t", T.LongType())]))),
])
ARRIVAL = T.StructType([
    T.StructField("trip_uid", T.StringType()), T.StructField("route_id", T.StringType()),
    T.StructField("direction", T.StringType()), T.StructField("stop_id", T.StringType()),
    T.StructField("arrival_ts", T.LongType()),
])
PLATFORM_OUT = T.StructType(ARRIVAL.fields + [
    T.StructField("kind", T.StringType()), T.StructField("headway_min", T.DoubleType()),
    T.StructField("sched_min", T.DoubleType()), T.StructField("status", T.StringType()),
])
STATE = T.StructType([T.StructField("json", T.StringType())])


# ----------------------------------------------------------------------------- timetable lookup

_SCHED: dict | None = None


def sched_lookup(stop_id: str, route_id: str, day_type: str, hour: int) -> float | None:
    global _SCHED
    if _SCHED is None:
        p = STATIC / "scheduled_headways.parquet"
        df = pd.read_parquet(p) if p.exists() else pd.DataFrame(columns=["stop_id", "route_id", "day_type", "hour", "sched_headway_min"])
        _SCHED = {(r.stop_id, r.route_id, r.day_type, int(r.hour)): float(r.sched_headway_min) for r in df.itertuples()}
    return _SCHED.get((stop_id, route_id, day_type, hour))


# ----------------------------------------------------------------------------- stateful functions

def _empty(schema: T.StructType) -> pd.DataFrame:
    return pd.DataFrame({f.name: pd.Series(dtype="object") for f in schema.fields})


def _frame(rows: list[dict], schema: T.StructType) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=[f.name for f in schema.fields]) if rows else _empty(schema)


def trip_state_fn(key, pdfs, state):
    (trip_uid,) = key
    if state.hasTimedOut:
        st = json.loads(state.get[0])
        state.remove()
        yield _frame(logic.finish_trip(st, trip_uid), ARRIVAL)
        return
    events = []
    for pdf in pdfs:
        for r in pdf.itertuples(index=False):
            events.append({"trip_uid": r.trip_uid, "route_id": r.route_id, "direction": r.direction, "feed_ts": int(r.feed_ts),
                           "stops": [{"stop_id": s["stop_id"], "t": int(s["t"])} for s in r.stops]})
    st = json.loads(state.get[0]) if state.exists else None
    st, arrivals = logic.track_trip(st, events)
    state.update((json.dumps(st),))
    state.setTimeoutDuration(logic.TRIP_TIMEOUT * 1000)
    yield _frame(arrivals, ARRIVAL)


# how late an arrival may reach the platform query; gap alerts trail real time by about this much
ARRIVAL_WATERMARK = os.environ.get("SUBWAY_ARRIVAL_WATERMARK", "2 minutes")


def platform_state_fn(key, pdfs, state):
    """Event-time state per line and platform. "Now" is Spark's watermark (the newest arrival time processed, minus
    the allowed lateness), not the wall clock, so a backlog upstream delays gap alerts instead of inventing them."""
    route_id, stop_id = key
    k = {"route_id": route_id, "stop_id": stop_id}
    wm = state.getCurrentWatermarkMs()
    st = json.loads(state.get[0]) if state.exists else None
    if state.hasTimedOut:
        now = wm // 1000
        st, rows = logic.check_open_gap(st, k, now, sched_lookup)
    else:
        arrivals = []
        for pdf in pdfs:
            for r in pdf.itertuples(index=False):
                a = {f: getattr(r, f) for f in ("trip_uid", "route_id", "direction", "stop_id")}
                a["arrival_ts"] = int(r.arrival_ts)
                arrivals.append(a)
        st, rows = logic.track_platform(st, arrivals, sched_lookup)
        now = max(wm // 1000, st["last_ts"])
    state.update((json.dumps(st),))
    if st.get("last_ts"):
        due_ms = int((now + logic.next_check_minutes(st, k, now, sched_lookup) * 60) * 1000)
        state.setTimeoutTimestamp(max(due_ms, wm + 1000))
    yield _frame(rows, PLATFORM_OUT)


# ----------------------------------------------------------------------------- topics: Kafka or folders

class Bus:
    def __init__(self, spark: SparkSession, source: str, servers: str, input_dir: str | None):
        self.spark, self.source, self.servers, self.input_dir = spark, source, servers, input_dir

    def read(self, topic: str, schema: T.StructType) -> DataFrame:
        if self.source == "kafka":
            raw = (self.spark.readStream.format("kafka").option("kafka.bootstrap.servers", self.servers)
                   .option("subscribe", topic).option("startingOffsets", "earliest").option("failOnDataLoss", "false").load())
            return raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v")).select("v.*")
        path = self.input_dir if topic == "trip_updates" and self.input_dir else str(OUT / "topics" / topic)
        Path(path).mkdir(parents=True, exist_ok=True)
        return self.spark.readStream.schema(schema).option("maxFilesPerTrigger", MAX_FILES).json(path)

    def write(self, df: DataFrame, topic: str, key: str, name: str):
        ck = str(OUT / "checkpoints" / name)
        if self.source == "kafka":
            out = df.select(F.col(key).alias("key"), F.to_json(F.struct(*df.columns)).alias("value"))
            return (out.writeStream.format("kafka").option("kafka.bootstrap.servers", self.servers).option("topic", topic)
                    .option("checkpointLocation", ck).queryName(name).trigger(processingTime=TRIGGER).start())
        return (df.writeStream.format("json").option("path", str(OUT / "topics" / topic)).option("checkpointLocation", ck)
                .queryName(name).trigger(processingTime=TRIGGER).start())


# ----------------------------------------------------------------------------- queries

def arrivals_query(bus: Bus):
    updates = bus.read("trip_updates", EVENT).withColumn("event_time", F.timestamp_seconds("feed_ts"))
    arrivals = (updates.withWatermark("event_time", "3 minutes")
                .dropDuplicatesWithinWatermark(["trip_uid", "feed_ts"])
                .groupBy("trip_uid")
                .applyInPandasWithState(trip_state_fn, ARRIVAL, STATE, "append", GroupStateTimeout.ProcessingTimeTimeout))
    return bus.write(arrivals, "arrivals", "trip_uid", "q1_arrivals")


def platform_query(bus: Bus):
    arrivals = (bus.read("arrivals", ARRIVAL).withColumn("arrival_time", F.timestamp_seconds("arrival_ts"))
                .withWatermark("arrival_time", ARRIVAL_WATERMARK))
    out = (arrivals.groupBy("route_id", "stop_id")
           .applyInPandasWithState(platform_state_fn, PLATFORM_OUT, STATE, "append", GroupStateTimeout.EventTimeTimeout))

    def sink(batch: DataFrame, batch_id: int) -> None:
        batch = batch.withColumn("arrival_time", F.timestamp_seconds("arrival_ts")).cache()
        batch.filter("kind = 'headway'").write.mode("append").parquet(str(OUT / "headways"))
        alerts = batch.filter("kind != 'headway'")
        alerts.write.mode("append").parquet(str(OUT / "alerts"))
        if bus.source == "kafka" and not alerts.isEmpty():
            (alerts.select(F.col("stop_id").alias("key"), F.to_json(F.struct(*alerts.columns)).alias("value"))
             .write.format("kafka").option("kafka.bootstrap.servers", bus.servers).option("topic", "alerts").save())
        batch.unpersist()

    return (out.writeStream.foreachBatch(sink).option("checkpointLocation", str(OUT / "checkpoints" / "q2_platforms"))
            .queryName("q2_platforms").trigger(processingTime=TRIGGER).start())


def reliability_query(spark: SparkSession):
    schema = T.StructType(PLATFORM_OUT.fields + [T.StructField("arrival_time", T.TimestampType())])
    (OUT / "headways").mkdir(parents=True, exist_ok=True)
    hw = spark.readStream.schema(schema).parquet(str(OUT / "headways"))
    agg = (hw.withWatermark("arrival_time", "5 minutes")
           .groupBy(F.window("arrival_time", "15 minutes"), "route_id")
           .agg(F.count("*").alias("headways"),
                F.avg((F.col("status") == "ok").cast("double")).alias("wait_assessment"),
                F.sum((F.col("status") == "gap").cast("int")).alias("gaps"),
                F.sum((F.col("status") == "bunched").cast("int")).alias("bunched"),
                F.avg("headway_min").alias("avg_headway_min"),
                F.avg("sched_min").alias("avg_sched_min"))
           .select(F.col("window.start").alias("window_start"), "route_id", "headways", "wait_assessment", "gaps", "bunched",
                   "avg_headway_min", "avg_sched_min"))
    return (agg.writeStream.format("parquet").outputMode("append").option("path", str(OUT / "reliability"))
            .option("checkpointLocation", str(OUT / "checkpoints" / "q3_reliability")).queryName("q3_reliability")
            .trigger(processingTime="30 seconds").start())


def spark_session(kafka: bool) -> SparkSession:
    b = (SparkSession.builder.appName("nyc-subway-live").master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.streaming.stateStore.providerClass",
                 "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false"))
    if kafka:
        b = b.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3")
    return b.getOrCreate()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--input", help="folder of producer JSON-lines files (file mode)")
    ap.add_argument("--servers", default=os.environ.get("KAFKA_SERVERS", "localhost:9092"))
    ap.add_argument("--minutes", type=float, default=50)
    a = ap.parse_args()
    spark = spark_session(a.source == "kafka")
    spark.sparkContext.setLogLevel("WARN")
    bus = Bus(spark, a.source, a.servers, a.input)
    queries = [arrivals_query(bus), platform_query(bus), reliability_query(spark)]
    end = time.time() + a.minutes * 60
    while time.time() < end:
        time.sleep(30)
        for q in queries:
            if q.exception():
                raise q.exception()
            p = q.lastProgress
            if p:
                log.info("%-15s %5d rows in, %.1f rows/s", q.name, p.get("numInputRows", 0), p.get("processedRowsPerSecond") or 0)
    # stop() lets the current micro-batch finish; processAllAvailable() would never return here, because
    # state timeouts keep scheduling new batches
    for q in queries:
        q.stop()
    spark.stop()


if __name__ == "__main__":
    main()
