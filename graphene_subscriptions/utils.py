"""Utils."""

import binascii


def consistent_hash(value, range_size: int):
    """Maps the value to a node value between 0 and 4095 using CRC, then down to one of the ring nodes."""
    if range_size == 1:
        # Avoid the overhead of hashing and modulo when it is unnecessary.
        return 0
    if isinstance(value, str):
        value = value.encode('utf-8')
    big_val = binascii.crc32(value) & 0xFFF
    ring_divisor = 4096 / float(range_size)
    return int(big_val / ring_divisor)
