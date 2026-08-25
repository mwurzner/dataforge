"""Pure-Python keccak-256.

Windows Application Control began blocking newly-installed compiled extensions on this machine,
so eth-hash has no usable backend and `eth_utils.keccak` cannot run. Function selectors and event
topics are deterministic constants of the ABI, so computing them in pure Python removes the
dependency permanently -- better than hardcoding a lookup table, which would silently break the
next time a new signature is needed.

Verified against the standard empty-string vector and against selectors previously computed with
eth-utils (see test at the bottom of this file).
"""
from __future__ import annotations

_RC = [0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
       0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
       0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
       0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
       0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
       0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008]
_R = [[0, 36, 3, 41, 18], [1, 44, 10, 45, 2], [62, 6, 43, 15, 61],
      [28, 55, 25, 21, 56], [27, 20, 39, 8, 14]]
_M = (1 << 64) - 1


def _rotl(v: int, n: int) -> int:
    return ((v << n) | (v >> (64 - n))) & _M if n else v


def _f(A):
    for rnd in range(24):
        C = [A[x][0] ^ A[x][1] ^ A[x][2] ^ A[x][3] ^ A[x][4] for x in range(5)]
        D = [C[(x - 1) % 5] ^ _rotl(C[(x + 1) % 5], 1) for x in range(5)]
        for x in range(5):
            for y in range(5):
                A[x][y] ^= D[x]
        B = [[0] * 5 for _ in range(5)]
        for x in range(5):
            for y in range(5):
                B[y][(2 * x + 3 * y) % 5] = _rotl(A[x][y], _R[x][y])
        for x in range(5):
            for y in range(5):
                A[x][y] = B[x][y] ^ ((~B[(x + 1) % 5][y]) & _M & B[(x + 2) % 5][y])
        A[0][0] ^= _RC[rnd]
    return A


def keccak256(data: bytes) -> bytes:
    rate = 136
    p = bytearray(data)
    p.append(0x01)                      # keccak (not SHA3) padding
    while len(p) % rate:
        p.append(0x00)
    p[-1] ^= 0x80
    A = [[0] * 5 for _ in range(5)]
    for off in range(0, len(p), rate):
        blk = p[off:off + rate]
        for i in range(rate // 8):
            A[i % 5][i // 5] ^= int.from_bytes(blk[i * 8:(i + 1) * 8], "little")
        A = _f(A)
    return b"".join(A[i % 5][i // 5].to_bytes(8, "little") for i in range(4))[:32]


def selector(sig: str) -> str:
    return "0x" + keccak256(sig.encode())[:4].hex()


if __name__ == "__main__":
    assert keccak256(b"").hex() == (
        "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"), "empty vector"
    # selectors previously computed with eth-utils, on a machine where it still worked
    for sig, want in (("market(bytes32)", "0x5c60e39a"),
                      ("description()", "0x7284e416"),
                      ("latestRoundData()", "0xfeaf968c"),
                      ("BASE_FEED_1()", "0xf50a4718"),
                      ("convertToAssets(uint256)", "0x07a2d13a")):
        got = selector(sig)
        assert got == want, f"{sig}: {got} != {want}"
    assert "0x" + keccak256(
        b"CreateMarket(bytes32,(address,address,address,address,uint256))").hex() == (
        "0xac4b2400f169220b0c0afdde7a0b32e775ba727ea1cb30b35f935cdaab8683ac"), "topic"
    print("keccak-256 verified: empty vector, 5 selectors, 1 event topic")
