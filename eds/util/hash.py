"""PARITY: internal/util/hash.go — xxhash of Go ``%+v``, and FNV-1a-32 modulo."""

from __future__ import annotations

import xxhash

_FNV32_OFFSET = 0x811C9DC5  # 2166136261
_FNV32_PRIME = 0x01000193  # 16777619
_UINT32_MASK = 0xFFFFFFFF


def hash(*vals: object) -> str:
    """PARITY: util.Hash — ``h := xxhash.New(); for v: h.Write(S2B(fmt.Sprintf("%+v", v)))``;
    return ``fmt.Sprintf("%x", h.Sum(nil))`` (16 lowercase hex chars of the 8-byte big-endian digest)."""
    h = xxhash.xxh64()  # seed 0, matching xxhash.New()
    for v in vals:
        h.update(_go_plus_v(v).encode("utf-8"))  # gstr.S2B(s) = the string's UTF-8 bytes
    return h.hexdigest()


def modulo(value: str, num: int) -> int:
    """PARITY: util.Modulo — ``int(fnv32a(value)) % num``, then abs."""
    partition = _fnv1a_32(value.encode("utf-8")) % num
    if partition < 0:
        # PARITY: dead branch on 64-bit Go, where int(uint32) is always >= 0; kept to mirror the source.
        partition = -partition
    return partition


def _fnv1a_32(data: bytes) -> int:
    """PARITY: hash/fnv.New32a — 32-bit FNV-1a (Python ints are unbounded, so mask to uint32)."""
    h = _FNV32_OFFSET
    for b in data:
        h ^= b
        h = (h * _FNV32_PRIME) & _UINT32_MASK
    return h


def _go_plus_v(v: object) -> str:
    """Reproduce ``fmt.Sprintf("%+v", v)`` for the types EDS actually hashes.

    Grounded: the Stage-1 production Hash call sites (consumer/kafka/snowflake/importer) hash only
    ``int`` and ``string``; the Go test vectors add ``bool`` and ``nil``. Floats and composite types are
    not hashed on any production path, so their ``%+v`` (esp. Go float ``'g'`` formatting) is deferred to
    GoFloat and added only when a real call site needs it.
    """
    if v is None:
        return "<nil>"  # PARITY: Go %+v of nil
    if isinstance(v, bool):
        # PARITY: Go bool prints "true"/"false". MUST precede the int check — bool is an int subclass
        # in Python, so True would otherwise format as "1".
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)  # PARITY: Go %+v of int/int64/uint64 is decimal
    if isinstance(v, str):
        return v
    raise NotImplementedError(
        f"Go %+v not implemented for {type(v).__name__} — no Stage-1 production Hash call site hashes it"
    )
