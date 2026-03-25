from typing import Tuple


class Bet:
    def __init__(self, agency: int, first_name: str, last_name: str, document: str, birthdate: str, number: int):
        self.agency = agency
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = birthdate
        self.number = number

    def __repr__(self) -> str:
        return (f"Bet(agency={self.agency!r}, first_name={self.first_name!r}, last_name={self.last_name!r}, "
                f"document={self.document!r}, birthdate={self.birthdate!r}, number={self.number!r})")


def _read_u8(buf: memoryview, offset: int) -> Tuple[int, int]:
    if offset + 1 > len(buf):
        raise ValueError("unexpected EOF while reading uint8")
    return int(buf[offset]), offset + 1


def _read_u16(buf: memoryview, offset: int) -> Tuple[int, int]:
    if offset + 2 > len(buf):
        raise ValueError("unexpected EOF while reading uint16")
    val = int.from_bytes(buf[offset : offset + 2], byteorder="big")
    return val, offset + 2


def _read_bytes(buf: memoryview, offset: int, length: int) -> Tuple[bytes, int]:
    if length < 0 or offset + length > len(buf):
        raise ValueError("invalid length while reading bytes")
    return bytes(buf[offset : offset + length]), offset + length


def _read_str(buf: memoryview, offset: int) -> Tuple[str, int]:
    ln, offset = _read_u8(buf, offset)
    if ln == 0:
        return "", offset
    b, offset = _read_bytes(buf, offset, ln)
    return b.decode("utf-8", errors="strict"), offset


def deserialize_bet(payload: bytes) -> Bet:
    """
    Deserialize payload into Bet.
    Expected payload layout:
      ID uint8
      LEN NAME uint8, NAME bytes
      LEN SURNAME uint8, SURNAME bytes
      LEN DNI uint8, DNI bytes
      LEN BIRTHDATE uint8, BIRTHDATE bytes
      BETNUMBER uint16 (big-endian)
    """
    if payload is None:
        raise ValueError("payload is None")
    if len(payload) == 0:
        raise ValueError("empty payload")

    mv = memoryview(payload)
    offset = 0

    # ID
    id_val, offset = _read_u8(mv, offset)

    # variable strings
    first_name, offset = _read_str(mv, offset)
    last_name, offset = _read_str(mv, offset)
    document, offset = _read_str(mv, offset)
    birthdate, offset = _read_str(mv, offset)

    # bet number
    bet_number, offset = _read_u16(mv, offset)

    if offset != len(mv):
        raise ValueError(f"payload not fully consumed (consumed={offset} total={len(mv)})")

    return Bet(
        agency=id_val,
        first_name=first_name,
        last_name=last_name,
        document=document,
        birthdate=birthdate,
        number=bet_number,
    )


def deserialize_batch(payload: bytes):
    """
    Parse batch payload composed of N len-payloads.
    Returns (agency_id, list_of_bets, error) — error is non-none on framing corruption.
    Individual bet deserialization errors skip that bet (logged by caller if wanted).
    """
    bets = []
    off = 0
    total = len(payload)
    agency_id = None
    while off < total:
        if off + 2 > total:
            return agency_id, bets, ValueError("short per-bet length header")
        per_len = int.from_bytes(payload[off:off+2], 'big')
        off += 2
        if off + per_len > total:
            return agency_id, bets, ValueError("short per-bet payload")
        bet_bytes = payload[off:off+per_len]
        off += per_len
        try:
            bet = deserialize_bet(bet_bytes)
            bets.append(bet)
            if not agency_id:
                agency_id = bet.agency
        except ValueError:
            # skip invalid bet
            continue
    
    return agency_id, bets, None


def serialize_winners(winners: list[Bet]) -> bytes:
    """
    Serialize winners list into a payload with the following layout:
    1-byte message type [0x04].
    2-byte len(payload).
    For each winner:
      2-byte big-endian length of bet.document payload
      Bet.document payload
    """
    if len(winners) > 255:
        raise ValueError("too many winners to serialize")
    
    payload = bytearray()
    for bet in winners:
        doc_bytes = bet.document.encode("utf-8")
        if len(doc_bytes) > 0xFFFF:
            raise ValueError("bet document too long to serialize")
        payload.extend(len(doc_bytes).to_bytes(2, 'big'))
        payload.extend(doc_bytes)

    if len(payload) > 0xFFFF:
        raise ValueError("payload too long to serialize")
    
    frame = bytearray()
    frame.append(0x04)  # message type
    frame.extend(len(payload).to_bytes(2, 'big'))
    frame.extend(payload)

    return bytes(frame)