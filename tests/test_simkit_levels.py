"""Unit tests for the Container-based bandwidth/level primitive."""

from __future__ import annotations

import simpy
import pytest

from agent_db_sim.simkit.errors import BandwidthError
from agent_db_sim.simkit.levels import BandwidthPool


def test_transfer_service_time_and_level_restored() -> None:
    env = simpy.Environment()
    pool = BandwidthPool(env, "link", capacity=10.0, bandwidth=2.0)

    def _xfer():
        return (yield pool.transfer(volume=4.0, label="read"))

    proc = env.process(_xfer())
    env.run(until=10.0)
    rec = proc.value

    assert rec.queue_time == 0.0
    assert rec.service_time == pytest.approx(2.0)  # 4 / 2
    assert rec.end == pytest.approx(2.0)
    assert pool.level == pytest.approx(10.0)  # volume returned to the level


def test_shared_level_blocks_concurrent_transfers() -> None:
    env = simpy.Environment()
    pool = BandwidthPool(env, "net", capacity=10.0, bandwidth=2.0)

    def _xfer():
        return (yield pool.transfer(volume=8.0))

    p1 = env.process(_xfer())
    p2 = env.process(_xfer())
    env.run(until=100.0)

    r1, r2 = p1.value, p2.value
    assert r1.queue_time == 0.0
    assert r1.service_time == pytest.approx(4.0)  # 8 / 2

    # second waits until the first returns its volume (t=4), then serves 4.0
    assert r2.queue_time == pytest.approx(4.0)
    assert r2.service_time == pytest.approx(4.0)
    assert r2.end == pytest.approx(8.0)


def test_invalid_configuration_fails_fast() -> None:
    env = simpy.Environment()
    with pytest.raises(BandwidthError, match="capacity"):
        BandwidthPool(env, "b", capacity=0.0, bandwidth=1.0)
    with pytest.raises(BandwidthError, match="bandwidth"):
        BandwidthPool(env, "b", capacity=1.0, bandwidth=0.0)
    with pytest.raises(BandwidthError, match="init"):
        BandwidthPool(env, "b", capacity=1.0, bandwidth=1.0, init=2.0)


def test_invalid_volume_fails_at_transfer() -> None:
    env = simpy.Environment()
    pool = BandwidthPool(env, "b", capacity=1.0, bandwidth=1.0)

    def _bad():
        yield pool.transfer(volume=0.0)

    env.process(_bad())
    with pytest.raises(BandwidthError, match="volume must be > 0"):
        env.run(until=1.0)
