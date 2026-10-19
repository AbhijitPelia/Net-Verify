"""UDP messaging server.

Behaviour (see REQUIREMENTS.md):
  * Valid messages are delivered to the application once and always acknowledged.
  * Duplicates (client retransmissions) are acknowledged again but not re-delivered.
  * Packets that fail validation are counted and dropped without an acknowledgement.
"""

import socket
import threading

from . import protocol


class Server:
    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((host, port))
        self._sock.settimeout(0.1)
        self.address = self._sock.getsockname()

        self.messages = []  # payloads delivered to the application, in arrival order
        self.stats = {
            "received": 0,
            "delivered": 0,
            "duplicates": 0,
            "rejected": 0,
            "acks_sent": 0,
        }
        self._seen = set()  # (client address, sequence number)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=2)
        self._sock.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()

    def _bump(self, key: str) -> None:
        with self._lock:
            self.stats[key] += 1

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                data, addr = self._sock.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                break

            self._bump("received")
            try:
                type_, seq, payload = protocol.decode(data)
            except protocol.ProtocolError:
                self._bump("rejected")
                continue
            if type_ != protocol.TYPE_MSG:
                self._bump("rejected")
                continue

            key = (addr, seq)
            with self._lock:
                if key in self._seen:
                    self.stats["duplicates"] += 1
                else:
                    self._seen.add(key)
                    self.messages.append(payload)
                    self.stats["delivered"] += 1

            # Always acknowledge, even duplicates: the earlier ACK may have been lost.
            self._sock.sendto(protocol.encode(protocol.TYPE_ACK, seq), addr)
            self._bump("acks_sent")
