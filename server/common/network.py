import socket
import logging


def read_exact(sock: socket.socket, n: int) -> bytes:
    """
    Read exactly n bytes from sock or raise OSError if the socket is closed
    before n bytes are received.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise OSError("socket closed while reading")
        buf.extend(chunk)
    return bytes(buf)


def read_client_message(sock: socket.socket) -> bytes:
    """
    Read a message prefixed with a 2-byte big-endian uint16 SIZE and return
    the payload bytes.
    """
    size_b = read_exact(sock, 2)
    size = int.from_bytes(size_b, byteorder="big")
    if size == 0:
        return b""
    payload = read_exact(sock, size)
    return payload


def send_ack(sock: socket.socket) -> None:
    """
    Send a single byte ACK (0x06) to the server.
    """
    try:
        sock.sendall(b"\x01")
    except OSError as e:
        logging.error(f"action: send_ack | result: fail | error: {e}")