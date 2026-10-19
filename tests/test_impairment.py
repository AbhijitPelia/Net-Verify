"""Checks on the impairment proxy itself (test equipment, not the system under test)."""

import socket

from netverify.impairment import Impairment, ImpairmentProxy


def _survivors(seed, packets):
    """Send `packets` through a 50%-loss proxy and return the set that came out the other side."""
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sink.bind(("127.0.0.1", 0))
    sink.settimeout(0.3)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    received = set()
    with ImpairmentProxy(sink.getsockname(), c2s=Impairment(loss=0.5), seed=seed) as proxy:
        for p in packets:
            sender.sendto(p, proxy.address)
        try:
            while True:
                received.add(sink.recvfrom(2048)[0])
        except socket.timeout:
            pass
    sender.close()
    sink.close()
    return received


def test_seeded_drops_do_not_depend_on_packet_order():
    packets = [f"pkt-{i}".encode() for i in range(100)]
    forward = _survivors(seed=1, packets=packets)
    backward = _survivors(seed=1, packets=packets[::-1])
    assert forward == backward
    assert 0 < len(forward) < len(packets)  # the loss really was applied
    assert _survivors(seed=2, packets=packets) != forward  # and the seed matters
