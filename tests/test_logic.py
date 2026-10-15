from subway import logic

T0 = 1_760_536_800      # 2025-10-15 10:00 New York (a Wednesday)


def ev(ts, stops, trip="t1", route="A"):
    return {"trip_uid": trip, "route_id": route, "direction": "N", "feed_ts": ts,
            "stops": [{"stop_id": s, "t": t} for s, t in stops]}


def sched(minutes):
    return lambda stop, route, day, hour: minutes


def test_stop_dropping_off_the_list_is_an_arrival_at_its_last_predicted_time():
    st, arr = logic.track_trip(None, [ev(T0, [("A1N", T0 + 20), ("A2N", T0 + 200)])])
    assert arr == []
    st, arr = logic.track_trip(st, [ev(T0 + 30, [("A2N", T0 + 190)])])
    assert arr == [{"trip_uid": "t1", "route_id": "A", "direction": "N", "stop_id": "A1N", "arrival_ts": T0 + 20}]


def test_stops_dropped_far_ahead_of_time_are_not_arrivals():
    """A reroute removes stops the train was not about to reach; those must not count as served."""
    st, _ = logic.track_trip(None, [ev(T0, [("A1N", T0 + 20), ("A9N", T0 + 900)])])
    _, arr = logic.track_trip(st, [ev(T0 + 30, [("A1N", T0 + 40)])])
    assert arr == []


def test_duplicate_and_out_of_order_polls_are_ignored():
    st, _ = logic.track_trip(None, [ev(T0 + 30, [("A2N", T0 + 190)])])
    st2, arr = logic.track_trip(st, [ev(T0, [("A1N", T0 + 20), ("A2N", T0 + 200)]), ev(T0 + 30, [("A2N", T0 + 190)])])
    assert arr == [] and st2 == st


def test_finished_trip_arrives_at_its_terminal():
    st, _ = logic.track_trip(None, [ev(T0, [("A65N", T0 + 40)])])
    assert [a["stop_id"] for a in logic.finish_trip(st, "t1")] == ["A65N"]


def test_headways_are_classified_against_the_timetable():
    arrivals = [{"trip_uid": f"t{i}", "route_id": "A", "direction": "N", "stop_id": "A1N", "arrival_ts": T0 + m * 60}
                for i, m in enumerate([0, 6, 7, 13, 30])]
    _, rows = logic.track_platform(None, arrivals, sched(6))
    assert [(r["headway_min"], r["status"]) for r in rows] == [(6, "ok"), (1, "bunched"), (6, "ok"), (17, "gap")]


def test_a_long_quiet_platform_raises_an_open_gap_before_the_next_train():
    st, _ = logic.track_platform(None, [{"trip_uid": "t0", "route_id": "A", "direction": "N", "stop_id": "A1N",
                                         "arrival_ts": T0}], sched(6))
    key = {"route_id": "A", "stop_id": "A1N"}
    st, alerts = logic.check_open_gap(st, key, T0 + 10 * 60, sched(6))
    assert alerts == []                                  # 10 min: late but not a gap (threshold 14)
    st, alerts = logic.check_open_gap(st, key, T0 + 15 * 60, sched(6))
    assert alerts[0]["status"] == "open_gap" and alerts[0]["headway_min"] == 15
    _, rows = logic.track_platform(st, [{"trip_uid": "t1", "route_id": "A", "direction": "N", "stop_id": "A1N",
                                         "arrival_ts": T0 + 16 * 60}], sched(6))
    assert [r["kind"] for r in rows] == ["headway", "gap_closed"]


def test_no_alert_when_the_line_is_not_scheduled():
    st = {"last_ts": T0, "last_trip": "t0", "gap_open_since": 0}
    _, alerts = logic.check_open_gap(st, {"route_id": "B", "stop_id": "D1N"}, T0 + 3600, sched(None))
    assert alerts == []


def test_gap_threshold():
    assert logic.gap_threshold(4) == 12 and logic.gap_threshold(10) == 20
