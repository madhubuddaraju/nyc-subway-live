"""Create the Kafka topics with explicit partitions (trip updates are keyed by trip, so one trip stays in order)."""
import os
import time

from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": os.environ.get("KAFKA_SERVERS", "localhost:9092")})
for _ in range(30):                                  # wait for the broker to come up
    try:
        admin.list_topics(timeout=5)
        break
    except Exception:
        time.sleep(2)
topics = [NewTopic("trip_updates", 4, 1), NewTopic("arrivals", 4, 1), NewTopic("alerts", 1, 1)]
for t, f in admin.create_topics(topics).items():
    try:
        f.result()
        print("created", t)
    except Exception as e:
        print(t, e)
