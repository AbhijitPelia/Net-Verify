"""Robustness regression tests that are not tied to a numbered requirement."""

import socket

from netverify import protocol


def test_server_survives_ack_to_closed_client_port(server, raw_sock):
    # On Windows, an ACK sent to a closed port triggers an ICMP "port unreachable",
    # and the server's next recvfrom() raises ConnectionResetError (WinError 10054).
    gone = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    gone.sendto(protocol.encode(protocol.TYPE_MSG, 1, b"bye"), server.address)
    gone.close()

    # The server must still be serving other clients afterwards.
    raw_sock.sendto(protocol.encode(protocol.TYPE_MSG, 2, b"still there?"), server.address)
    data, _ = raw_sock.recvfrom(2048)
    assert protocol.decode(data)[:2] == (protocol.TYPE_ACK, 2)
