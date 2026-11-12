import asyncio
import os
import stat

import pytest

from attendance_ops_mcp import tunnel as tunnel_module
from attendance_ops_mcp.config import Target
from attendance_ops_mcp.tunnel import Tunnel

IDLE = 0.2
TARGET = Target(env="dev", context="rancher-desktop", namespace="attendance")


@pytest.fixture(autouse=True)
def fake_kubectl(tmp_path, monkeypatch):
    script = tmp_path / "kubectl"
    script.write_text("#!/bin/sh\necho 'Forwarding from 127.0.0.1:15432 -> 5432'\nexec sleep 30\n")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setattr(tunnel_module, "IDLE_CLOSE_SECONDS", IDLE)


def run(scenario):
    asyncio.run(scenario())


def test_closes_after_idle_period():
    async def scenario():
        tunnel = Tunnel(TARGET, "svc/postgres", 5432)
        async with tunnel.lease():
            assert await tunnel.open() == 15432
        assert tunnel.is_alive
        await asyncio.sleep(IDLE * 3)
        assert tunnel.is_alive is False

    run(scenario)


def test_stays_open_while_leased():
    async def scenario():
        tunnel = Tunnel(TARGET, "svc/postgres", 5432)
        async with tunnel.lease():
            await tunnel.open()
            await asyncio.sleep(IDLE * 3)
            assert tunnel.is_alive
        await tunnel.close()

    run(scenario)


def test_new_lease_cancels_pending_close():
    async def scenario():
        tunnel = Tunnel(TARGET, "svc/postgres", 5432)
        async with tunnel.lease():
            await tunnel.open()
        first = tunnel.process
        await asyncio.sleep(IDLE / 2)
        async with tunnel.lease():
            await tunnel.open()
            await asyncio.sleep(IDLE)
            assert tunnel.process is first
            assert tunnel.is_alive
        await asyncio.sleep(IDLE * 3)
        assert tunnel.is_alive is False

    run(scenario)


def test_reopens_after_idle_close():
    async def scenario():
        tunnel = Tunnel(TARGET, "svc/postgres", 5432)
        async with tunnel.lease():
            await tunnel.open()
        await asyncio.sleep(IDLE * 3)
        async with tunnel.lease():
            assert await tunnel.open() == 15432
            assert tunnel.is_alive
        await tunnel.close()

    run(scenario)
