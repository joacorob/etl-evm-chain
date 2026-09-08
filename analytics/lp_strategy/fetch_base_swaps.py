#!/usr/bin/env python3
"""Pull Uniswap v3 Swap events from a Base pool via eth_getLogs.

Outputs CSV compatible with the existing format used by the backtest.
"""
from __future__ import annotations
import argparse, csv, json, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
import urllib.request

import os as _os
# Set BASE_RPC en el entorno (ej: https://rpc.waizard.ai/<tu-token>/base)
RPC = _os.environ.get("BASE_RPC", "")
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
HEADERS = {"Content-Type": "application/json"}


def rpc(method: str, params: list[Any]) -> Any:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for attempt in range(8):
        try:
            req = urllib.request.Request(RPC, data=body, headers=HEADERS, method="POST")
            with urllib.request.urlopen(req, timeout=60) as r:
                resp = json.loads(r.read())
            if "error" in resp:
                raise RuntimeError(resp["error"])
            return resp["result"]
        except Exception as e:  # noqa: BLE001
            if attempt == 7:
                raise
            time.sleep(0.8 * (attempt + 1))


def hex_to_int(h: str) -> int:
    return int(h, 16)


def to_signed(value: int, bits: int) -> int:
    if value & (1 << (bits - 1)):
        return value - (1 << bits)
    return value


def parse_swap(log: dict[str, Any]) -> dict[str, Any]:
    data = log["data"][2:]
    # data layout: amount0(int256), amount1(int256), sqrtPriceX96(uint160), liquidity(uint128), tick(int24)
    amount0 = to_signed(hex_to_int(data[0:64]), 256)
    amount1 = to_signed(hex_to_int(data[64:128]), 256)
    sqrtP = hex_to_int(data[128:192])
    liquidity = hex_to_int(data[192:256])
    tick = to_signed(hex_to_int(data[256:320]), 256)  # padded to 32 bytes
    sender = "0x" + log["topics"][1][-40:]
    recipient = "0x" + log["topics"][2][-40:]
    return {
        "amount0": amount0,
        "amount1": amount1,
        "sqrtPriceX96": sqrtP,
        "liquidity": liquidity,
        "tick": tick,
        "sender": sender,
        "recipient": recipient,
        "block_number": hex_to_int(log["blockNumber"]),
        "tx_hash": log["transactionHash"],
        "log_index": hex_to_int(log["logIndex"]),
    }


def fetch_chunk(pool: str, from_block: int, to_block: int) -> list[dict]:
    logs = rpc("eth_getLogs", [{
        "address": pool,
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "topics": [SWAP_TOPIC],
    }])
    return [parse_swap(l) for l in logs]


def fetch_block_timestamps(blocks: set[int], workers: int = 16) -> dict[int, int]:
    out: dict[int, int] = {}
    def one(b: int) -> tuple[int, int]:
        r = rpc("eth_getBlockByNumber", [hex(b), False])
        return b, hex_to_int(r["timestamp"])
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed([ex.submit(one, b) for b in blocks]):
            b, ts = fut.result()
            out[b] = ts
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pool", required=True)
    p.add_argument("--from-block", type=int, required=True)
    p.add_argument("--to-block", type=int, required=True)
    p.add_argument("--chunk", type=int, default=2000)
    p.add_argument("--workers", type=int, default=10)
    p.add_argument("--out", required=True)
    p.add_argument("--synthetic-ts", action="store_true",
                   help="Use linear interpolation from anchor blocks (Base ~2s blocks)")
    args = p.parse_args()

    ranges = []
    b = args.from_block
    while b <= args.to_block:
        end = min(b + args.chunk - 1, args.to_block)
        ranges.append((b, end))
        b = end + 1

    print(f"[fetch] {len(ranges)} chunks of {args.chunk} blocks each", flush=True)
    all_swaps: list[dict] = []
    pending = list(ranges)
    rounds = 0
    while pending and rounds < 5:
        rounds += 1
        next_pending: list[tuple[int, int]] = []
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(fetch_chunk, args.pool, lo, hi): (lo, hi) for lo, hi in pending}
            done = 0
            for fut in as_completed(futs):
                lo, hi = futs[fut]
                try:
                    swaps = fut.result()
                except Exception as e:  # noqa: BLE001
                    # split the range and retry on next round
                    if hi - lo > 100:
                        mid = (lo + hi) // 2
                        next_pending.append((lo, mid))
                        next_pending.append((mid + 1, hi))
                    else:
                        next_pending.append((lo, hi))
                    print(f"[fetch] r{rounds} {lo}-{hi} FAILED: {e}", flush=True)
                    continue
                all_swaps.extend(swaps)
                done += 1
                if done % 20 == 0:
                    print(f"[fetch] r{rounds} {done}/{len(pending)} ok, {len(all_swaps)} swaps", flush=True)
        print(f"[fetch] round {rounds} done, {len(next_pending)} chunks to retry", flush=True)
        pending = next_pending
    if pending:
        print(f"[fetch] WARN: {len(pending)} chunks still failing after retries", flush=True)

    print(f"[fetch] total swaps: {len(all_swaps)}", flush=True)
    if args.synthetic_ts:
        print("[fetch] synthetic timestamps from 2 anchor blocks…", flush=True)
        anchor_lo = rpc("eth_getBlockByNumber", [hex(args.from_block), False])
        anchor_hi = rpc("eth_getBlockByNumber", [hex(args.to_block), False])
        ts_lo = hex_to_int(anchor_lo["timestamp"])
        ts_hi = hex_to_int(anchor_hi["timestamp"])
        slope = (ts_hi - ts_lo) / max(args.to_block - args.from_block, 1)
        for s in all_swaps:
            s["timestamp"] = int(ts_lo + slope * (s["block_number"] - args.from_block))
    else:
        print("[fetch] resolving block timestamps…", flush=True)
        blocks = {s["block_number"] for s in all_swaps}
        ts = fetch_block_timestamps(blocks, workers=args.workers)
        for s in all_swaps:
            s["timestamp"] = ts.get(s["block_number"], 0)

    all_swaps.sort(key=lambda s: (s["block_number"], s["log_index"]))
    fields = ["amount0", "amount1", "block_number", "liquidity", "recipient", "sender", "sqrtPriceX96", "tick", "timestamp", "tx_hash"]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for s in all_swaps:
            w.writerow(s)
    print(f"[fetch] wrote {args.out}", flush=True)


if __name__ == "__main__":
    main()
