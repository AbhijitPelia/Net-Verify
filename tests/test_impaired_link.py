"""Verification tests through the impairment proxy. Each test names the requirement it verifies.

Measured values are attached with `record_property`, so they appear in the
traceability matrix and in JUnit XML output instead of being hard-coded anywhere.
"""

import statistics

import pytest

from netverify import protocol
from netverify.client import Client
from netverify.impairment import Impairment, ImpairmentProxy, decide, packet_rng


def predicted_attempts(seed, imp, payloads, max_attempts):
    """Replay the proxy's seeded drop decisions offline, assuming no ACK arrives late.

    Returns, per message, the attempt on which it should be delivered (None = never).
    The client numbers messages 1, 2, 3, ... and the server ACKs every copy it receives.
    """
    ack_copies = {}
    predicted = []
    for seq, payload in enumerate(payloads, start=1):
        msg = protocol.encode(protocol.TYPE_MSG, seq, payload)
        ack = protocol.encode(protocol.TYPE_ACK, seq)
        result = None
        for attempt in range(1, max_attempts + 1):
            if decide(imp, packet_rng(seed, "c2s", msg, attempt)) is None:
                continue
            ack_copies[seq] = ack_copies.get(seq, 0) + 1
            if decide(imp, packet_rng(seed, "s2c", ack, ack_copies[seq])) is not None:
                result = attempt
                break
        predicted.append(result)
    return predicted


@pytest.mark.req("REQ-006")
def test_delivery_ratio_with_20_percent_loss_each_way(server, record_property):
    # An attempt succeeds only if the message AND its ACK survive: 0.8 * 0.8 = 0.64.
    # With 11 attempts, P(message fails) = 0.36 ** 11, about 1.3e-5, so the 99%
    # threshold tests the retransmission logic rather than luck.
    n_messages = 500
    seed, max_retries = 6, 10
    loss = Impairment(loss=0.20)
    payloads = [f"msg-{i}".encode() for i in range(n_messages)]
    with ImpairmentProxy(server.address, c2s=loss, s2c=loss, seed=seed) as proxy:
        client = Client(proxy.address, timeout_s=0.05, max_retries=max_retries)
        results = [client.send(p) for p in payloads]
        client.close()
        stats = proxy.stats

    # Determinism check, recorded rather than asserted: a late ACK on a loaded machine
    # can add a retransmission without breaking the requirement.
    predicted = predicted_attempts(seed, loss, payloads, max_retries + 1)
    matches = sum(r.attempts == p for r, p in zip(results, predicted))
    record_property("attempts_match_seeded_prediction", f"{matches}/{n_messages}")
    record_property("max_attempts", max(r.attempts for r in results))

    # Check the test environment itself: the impairment must actually have been applied.
    assert stats["c2s"]["dropped"] > 0 and stats["s2c"]["dropped"] > 0

    delivered = sum(r.delivered for r in results)
    ratio = delivered / n_messages
    record_property("delivery_ratio", f"{ratio:.4f} ({delivered}/{n_messages})")
    record_property("mean_attempts", f"{statistics.mean(r.attempts for r in results):.2f}")
    record_property("observed_loss_c2s", f"{stats['c2s']['dropped'] / stats['c2s']['received']:.3f}")
    record_property("observed_loss_s2c", f"{stats['s2c']['dropped'] / stats['s2c']['received']:.3f}")
    assert ratio >= 0.99


@pytest.mark.req("REQ-007")
def test_latency_with_100ms_delay_and_20ms_jitter(server, record_property):
    # Round trip is 2 x (100 +/- 20) ms = 160..240 ms, inside the 500 ms timeout,
    # so no retransmissions are expected.
    n_messages = 50
    link = Impairment(delay_s=0.100, jitter_s=0.020)
    with ImpairmentProxy(server.address, c2s=link, s2c=link, seed=7) as proxy:
        client = Client(proxy.address, timeout_s=0.500, max_retries=5)
        results = [client.send(f"msg-{i}".encode()) for i in range(n_messages)]
        client.close()

    latencies_ms = sorted(r.latency_s * 1000 for r in results)
    median_ms = statistics.median(latencies_ms)
    record_property("median_latency_ms", f"{median_ms:.1f}")
    record_property("max_latency_ms", f"{latencies_ms[-1]:.1f}")
    record_property("max_attempts", max(r.attempts for r in results))
    assert all(r.delivered for r in results)
    assert median_ms <= 350
