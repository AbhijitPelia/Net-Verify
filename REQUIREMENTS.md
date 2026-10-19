# Requirements

Each requirement has a unique ID. Tests are tagged with `@pytest.mark.req("REQ-xxx")`
so a traceability matrix can be generated from the test suite:

    pytest --trace-matrix traceability.md

The Status column records whether a verifying test exists. Pass/fail results come
from a test run and are recorded in `traceability.md`, not here.

The system under test is a UDP client-server messaging service with
stop-and-wait delivery: the client sends one message, waits for an ACK, and
retransmits on timeout.

| ID | Requirement | Verification | Status |
|----|-------------|--------------|--------|
| REQ-001 | The server shall acknowledge every valid message with an ACK carrying the same sequence number. | Test (raw socket) | Test implemented (Stage 1) |
| REQ-002 | On an unimpaired link, the 95th-percentile send-to-ACK latency shall be 50 ms or less. | Test (200 messages) | Test implemented (Stage 1) |
| REQ-003 | The server shall neither acknowledge nor deliver a packet whose checksum fails. | Test (corrupted packet) | Test implemented (Stage 1) |
| REQ-004 | The server shall deliver each message to the application exactly once, and shall acknowledge every retransmission of it. | Test (duplicate packets) | Test implemented (Stage 1) |
| REQ-005 | After an ACK timeout, the client shall retransmit the message up to `max_retries` times, then report failure. | Test (silent server) | Test implemented (Stage 1) |
| REQ-006 | With 20% packet loss in each direction, at least 99% of messages shall be delivered. | Test (impairment proxy, 500 messages) | Test implemented (Stage 2) |
| REQ-007 | With 100 ms one-way delay and 20 ms jitter, all messages shall be delivered and median send latency shall be 350 ms or less (client timeout 500 ms). | Test (impairment proxy, 50 messages) | Test implemented (Stage 2) |
| REQ-008 | When the server is unreachable, the client shall report failure within `(max_retries + 1) x timeout + 100 ms` instead of blocking. | Test (silent server) | Test implemented (Stage 1) |
| REQ-009 | Messages sent sequentially by one client shall be delivered to the application in send order. | Test (20 messages) | Test implemented (Stage 1) |
| REQ-010 | Payloads up to 1024 bytes shall be supported, and the client shall reject larger payloads before sending. | Test (boundary values) | Test implemented (Stage 1) |
