"""LZF decompression, for string values Redis chose to compress in an RDB file.

Only decompression is implemented. Compression is optional in the format - the
length prefix says whether a value is compressed - so this server always writes
uncompressed values and still produces files Redis can read. Being able to
*read* compressed values is not optional if the goal is to load a file that
real Redis wrote.

The format is the classic liblzf byte stream: a control byte either introduces
a run of literals or a back-reference into the output produced so far.
"""

from __future__ import annotations

from pyredis.errors import RdbError

__all__ = ["lzf_decompress"]

#: Control bytes below this value introduce a literal run.
_LITERAL_MAX = 32
#: A length field of 7 means "the real length follows in the next byte".
_LONG_MATCH = 7


def lzf_decompress(data: bytes, expected_length: int) -> bytes:
    """Decompress ``data``, which must expand to exactly ``expected_length`` bytes.

    Raises:
        RdbError: if the stream is truncated, refers outside the output, or
            does not produce the expected length.
    """
    out = bytearray()
    i = 0
    size = len(data)

    while i < size:
        control = data[i]
        i += 1

        if control < _LITERAL_MAX:
            run = control + 1
            if i + run > size:
                raise RdbError("truncated LZF literal run")
            out += data[i : i + run]
            i += run
            continue

        length = control >> 5
        if length == _LONG_MATCH:
            if i >= size:
                raise RdbError("truncated LZF match length")
            length += data[i]
            i += 1

        if i >= size:
            raise RdbError("truncated LZF back-reference")
        reference = len(out) - ((control & 0x1F) << 8) - data[i] - 1
        i += 1
        if reference < 0:
            raise RdbError("LZF back-reference points before the start of the output")

        # Copies may overlap (that is how runs are encoded), so this has to be
        # byte by byte rather than a slice copy.
        for _ in range(length + 2):
            out.append(out[reference])
            reference += 1

    if len(out) != expected_length:
        raise RdbError(f"LZF output length {len(out)} does not match expected {expected_length}")
    return bytes(out)
