"""UDP messaging client using stop-and-wait with retransmission.

The client sends one message at a time. If no matching ACK arrives within
`timeout_s`, it retransmits, up to `max_retries` retransmissions
(so at most max_retries + 1 total attempts), then reports failure.
"""

import socket
import time
from dataclasses import dataclass

from . import protocol


@dataclass
class SendResult:
    delivered: bool
    attempts: int
    latency_s: float


class Client:
    def __init__(self, server_addr, timeout_s: float = 0.2, max_retries: int = 5):
        self.server_addr = server_addr
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._seq = 0

    def close(self) -> None:
        self._sock.close()

    def send(self, payload: bytes) -> SendResult:
        # Raises ProtocolError for oversize payloads (before any packet is sent).
        self._seq += 1
        seq = self._seq
        packet = protocol.encode(protocol.TYPE_MSG, seq, payload)

        start = time.monotonic()
        attempts = 0
        for _ in range(self.max_retries + 1):
            attempts += 1
            self._sock.sendto(packet, self.server_addr)
            deadline = time.monotonic() + self.timeout_s
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._sock.settimeout(remaining)
                try:
                    data, _ = self._sock.recvfrom(2048)
                except socket.timeout:
                    break
                try:
                    type_, ack_seq, _ = protocol.decode(data)
                except protocol.ProtocolError:
                    continue
                # Ignore stale ACKs left over from earlier messages.
                if type_ == protocol.TYPE_ACK and ack_seq == seq:
                    return SendResult(True, attempts, time.monotonic() - start)
        return SendResult(False, attempts, time.monotonic() - start)
