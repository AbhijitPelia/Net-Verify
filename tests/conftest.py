import socket

import pytest

from netverify.server import Server


@pytest.fixture
def server():
    srv = Server()
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture
def raw_sock():
    """A plain UDP socket for sending hand-built packets to the server."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(1.0)
    yield sock
    sock.close()


@pytest.fixture
def blackhole():
    """Address of a bound UDP socket that never replies (an unreachable server)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    yield sock.getsockname()
    sock.close()
