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


def read_frame(sock):
    """
    Read a full frame: 1 byte type + 2 bytes length + payload.
    Returns (frame_type:int, payload:bytes) or raises OSError on I/O error/EOF.
    """

    hdr = read_exact(sock, 3)
    frame_type = hdr[0]
    total_len = int.from_bytes(hdr[1:3], 'big')
    payload = read_exact(sock, total_len) if total_len > 0 else b''
    return frame_type, payload


def send_ack(sock: socket.socket) -> None:
    """
    Send a single byte ACK (0x01) to the server.
    """
    try:
        sock.sendall(b"\x01")
    except OSError as e:
        logging.error(f"action: send_ack | result: fail | error: {e}")


def send_nack(sock):
    """
    Send a single-byte NACK (0x02). Propaga OSError si falla.
    """
    sock.sendall(b'\x02')


def read_ack(sock):
    """
    Read a single byte from the socket and return True if it's an ACK (0x01),
    False if it's a NACK (0x02), or raise OSError on I/O error/EOF.
    """
    resp = read_exact(sock, 1)
    if resp == b'\x01':
        return True
    elif resp == b'\x02':
        return False
    else:
        raise ValueError(f"unexpected response byte: {resp.hex()}")