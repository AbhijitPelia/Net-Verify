"""Verification tests on an unimpaired link. Each test names the requirement it verifies."""

import socket
import time

import pytest

from netverify import protocol
from netverify.client import Client


@pytest.mark.req("REQ-001")
def test_server_acks_valid_message_with_same_sequence_number(server, raw_sock):
    raw_sock.sendto(protocol.encode(protocol.TYPE_MSG, 42, b"hello"), server.address)
    data, _ = raw_sock.recvfrom(2048)
    type_, seq, _ = protocol.decode(data)
    assert type_ == protocol.TYPE_ACK
    assert seq == 42


@pytest.mark.req("REQ-002")
def test_p95_latency_on_clean_link(server):
    client = Client(server.address)
    results = [client.send(b"x" * 64) for _ in range(200)]
    assert all(r.delivered for r in results)
    latencies = sorted(r.latency_s for r in results)
    p95 = latencies[int(len(latencies) * 0.95) - 1]
    assert p95 <= 0.050


@pytest.mark.req("REQ-003")
def test_corrupted_packet_is_rejected_and_not_acked(server, raw_sock):
    packet = bytearray(protocol.encode(protocol.TYPE_MSG, 1, b"payload"))
    packet[-1] ^= 0xFF  # corrupt the last payload byte
    raw_sock.settimeout(0.3)
    raw_sock.sendto(bytes(packet), server.address)
    with pytest.raises(socket.timeout):
        raw_sock.recvfrom(2048)
    assert server.stats["rejected"] == 1
    assert server.messages == []


@pytest.mark.req("REQ-004")
def test_duplicates_delivered_once_but_each_acked(server, raw_sock):
    packet = protocol.encode(protocol.TYPE_MSG, 7, b"once")
    for _ in range(3):
        raw_sock.sendto(packet, server.address)
        data, _ = raw_sock.recvfrom(2048)
        assert protocol.decode(data)[:2] == (protocol.TYPE_ACK, 7)
    assert server.messages == [b"once"]
    assert server.stats["duplicates"] == 2


@pytest.mark.req("REQ-005")
def test_client_retransmits_up_to_max_retries(blackhole):
    client = Client(blackhole, timeout_s=0.05, max_retries=3)
    result = client.send(b"lost")
    assert result.delivered is False
    assert result.attempts == 4  # initial send + 3 retransmissions


@pytest.mark.req("REQ-008")
def test_client_gives_up_within_bounded_time(blackhole):
    timeout_s, retries = 0.05, 3
    client = Client(blackhole, timeout_s=timeout_s, max_retries=retries)
    start = time.monotonic()
    result = client.send(b"lost")
    elapsed = time.monotonic() - start
    assert result.delivered is False
    assert elapsed <= (retries + 1) * timeout_s + 0.100


@pytest.mark.req("REQ-009")
def test_messages_delivered_in_send_order(server):
    client = Client(server.address)
    payloads = [f"msg-{i}".encode() for i in range(20)]
    for payload in payloads:
        assert client.send(payload).delivered
    assert server.messages == payloads


@pytest.mark.req("REQ-010")
def test_max_payload_accepted_and_oversize_rejected(server):
    client = Client(server.address)
    assert client.send(b"a" * 1024).delivered
    assert server.messages == [b"a" * 1024]
    with pytest.raises(protocol.ProtocolError):
        client.send(b"a" * 1025)
