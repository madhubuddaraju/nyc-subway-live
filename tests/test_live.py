from subway import live


def test_line_status_thresholds():
    assert live.status(None, 0) == "no data"
    assert live.status(0.9, 0) == "good"
    assert live.status(0.9, 1) == "delays"            # one late train, felt along one direction
    assert live.status(0.7, 0) == "delays"
    assert live.status(0.5, 0) == "severe"
    assert live.status(0.95, 2) == "severe"           # gaps in both directions
    assert live.status(0.95, 1, worst_ratio=3.2) == "severe"
    assert live.status(None, 1) == "delays"
