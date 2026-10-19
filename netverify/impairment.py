"""UDP impairment proxy: a man-in-the-middle that degrades the link on purpose.

    client  <-->  [listen socket | ImpairmentProxy | upstream socket]  <-->  server

The client sends to `proxy.address` instead of the server. Each datagram is
independently dropped with probability `loss`, otherwise held for
`delay_s + uniform(-jitter_s, +jitter_s)` seconds (never negative) and then
forwarded. Client-to-server and server-to-client each have their own settings,
their own random number generator, and their own counters.

Because each packet's delay is drawn independently, jitter can reorder packets,
as on a real network.

Reproducibility: with a `seed`, the same sequence of packets in a direction gets
the same drop/delay decisions. The end-to-end run is not bit-for-bit identical,
because thread scheduling can change how many retransmissions the client makes.

The proxy supports one client at a time: replies from the server go to the
client address the proxy heard from most recently.
"""

import heapq
import itertools
import random
import socket
import threading
import time
from dataclasses import dataclass


@dataclass
class Impairment:
    loss: float = 0.0     # probability in [0, 1] that a packet is dropped
    delay_s: float = 0.0  # base one-way delay added to every forwarded packet
    jitter_s: float = 0.0  # delay varies uniformly within +/- jitter_s


class ImpairmentProxy:
    def __init__(self, upstream_addr, c2s=None, s2c=None, seed=None,
                 host: str = "127.0.0.1", port: int = 0):
        self.upstream_addr = upstream_addr
        self.impairments = {"c2s": c2s or Impairment(), "s2c": s2c or Impairment()}
        # One RNG per direction, so the decisions in one direction do not depend on
        # how the two receive threads interleave. String seeds are deterministic.
        self._rngs = {
            d: random.Random(None if seed is None else f"{seed}:{d}") for d in ("c2s", "s2c")
        }

        self._listen_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._listen_sock.bind((host, port))
        self._listen_sock.settimeout(0.1)
        self.address = self._listen_sock.getsockname()

        self._upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._upstream_sock.bind((host, 0))
        self._upstream_sock.settimeout(0.1)

        self._client_addr = None
        self.stats = {d: {"received": 0, "dropped": 0, "forwarded": 0} for d in ("c2s", "s2c")}
        self._lock = threading.Lock()

        # Scheduled sends: a min-heap of (due_time, tiebreak, direction, data).
        self._queue = []
        self._tiebreak = itertools.count()
        self._queue_cv = threading.Condition()

        self._stop_event = threading.Event()
        self._threads = [
            threading.Thread(target=self._receive_loop, args=(self._listen_sock, "c2s"), daemon=True),
            threading.Thread(target=self._receive_loop, args=(self._upstream_sock, "s2c"), daemon=True),
            threading.Thread(target=self._send_loop, daemon=True),
        ]

    def start(self) -> None:
        for t in self._threads:
            t.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._queue_cv:
            self._queue_cv.notify()
        for t in self._threads:
            t.join(timeout=2)
        self._listen_sock.close()
        self._upstream_sock.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()

    def _bump(self, direction: str, key: str) -> None:
        with self._lock:
            self.stats[direction][key] += 1

    def _receive_loop(self, sock, direction: str) -> None:
        imp = self.impairments[direction]
        rng = self._rngs[direction]
        while not self._stop_event.is_set():
            try:
                data, addr = sock.recvfrom(2048)
            except socket.timeout:
                continue
            except ConnectionResetError:
                # Windows (WinError 10054): an earlier forward hit a closed port. Not fatal.
                # Must come before OSError, which it subclasses.
                continue
            except OSError:
                break

            if direction == "c2s":
                self._client_addr = addr
            self._bump(direction, "received")

            if rng.random() < imp.loss:
                self._bump(direction, "dropped")
                continue
            delay = max(0.0, imp.delay_s + rng.uniform(-imp.jitter_s, imp.jitter_s))
            with self._queue_cv:
                heapq.heappush(self._queue, (time.monotonic() + delay, next(self._tiebreak), direction, data))
                self._queue_cv.notify()

    def _send_loop(self) -> None:
        while not self._stop_event.is_set():
            with self._queue_cv:
                if not self._queue:
                    self._queue_cv.wait()
                    continue
                wait = self._queue[0][0] - time.monotonic()
                if wait > 0:
                    # Woken early if a packet with an earlier due time is queued.
                    self._queue_cv.wait(timeout=wait)
                    continue
                _, _, direction, data = heapq.heappop(self._queue)

            if direction == "c2s":
                sock, dest = self._upstream_sock, self.upstream_addr
            else:
                sock, dest = self._listen_sock, self._client_addr
            try:
                sock.sendto(data, dest)
            except OSError:
                continue  # socket closed during stop()
            self._bump(direction, "forwarded")
