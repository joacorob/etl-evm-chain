#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_pool_price_rpc — precio REAL on-chain del pool via RPC de Waizard
=======================================================================

OJO: Waizard (rpc.waizard.ai) NO es archive — prunea estado Y logs mas alla de
~33 dias. Por eso NO sirve para +6 meses (para eso usamos Binance en
fetch_eth_history.py). Esto baja lo que Waizard SI puede dar: el precio real del
pool en la ventana disponible, como cross-check de que CEX price ~= pool price.

Tecnica: en vez de traer TODOS los swaps (millones), muestreamos una ventanita de
~150 bloques (~5 min) cada `stride_blocks` (~1 hora) y tomamos el sqrtPriceX96 del
ultimo swap. ~720 calls chiquitas para 30 dias en vez de millones.

Pool default: WETH/USDC 0.3% 0x6c561b446416e1a00e8e93e221854d6ea4171372
(token0=WETH 18dec, token1=USDC 6dec) -> precio = (sqrtP/2^96)^2 * 1e12 = ETH en USDC.
"""
import os, csv, json, sys, time, argparse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set BASE_RPC en el entorno (ej: https://rpc.waizard.ai/<tu-token>/base)
RPC = os.environ.get("BASE_RPC", "")
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
POOL = "0x6c561b446416e1a00e8e93e221854d6ea4171372"
OUT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "pool_price_rpc.csv"))


def rpc(method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for attempt in range(6):
        try:
            req = urllib.request.Request(RPC, data=body, headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=60) as r:
                resp = json.loads(r.read())
            if "error" in resp:
                return resp  # dejar que el caller decida (pruned, etc)
            return resp
        except Exception:
            if attempt == 5:
                raise
            time.sleep(0.6 * (attempt + 1))


def price_from_log(l):
    data = l["data"][2:]
    sqrtP = int(data[128:192], 16)
    return (sqrtP / (2 ** 96)) ** 2 * 1e12


def block_ts(blk):
    r = rpc("eth_getBlockByNumber", [hex(blk), False])
    return int(r["result"]["timestamp"], 16) if r.get("result") else None


def oldest_available(bn, span_guess=8_000_000):
    """binary search del bloque mas viejo con logs disponibles."""
    def ok(blk):
        r = rpc("eth_getLogs", [{"address": POOL, "fromBlock": hex(blk), "toBlock": hex(blk + 30), "topics": [SWAP_TOPIC]}])
        return "error" not in r
    lo, hi = bn - span_guess, bn
    while hi - lo > 1000:
        mid = (lo + hi) // 2
        if ok(mid):
            hi = mid
        else:
            lo = mid
    return hi


def sample_window(blk, win):
    r = rpc("eth_getLogs", [{"address": POOL, "fromBlock": hex(blk), "toBlock": hex(blk + win), "topics": [SWAP_TOPIC]}])
    if "error" in r or not r.get("result"):
        return None
    logs = r["result"]
    return price_from_log(logs[-1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stride", type=int, default=1800, help="bloques entre muestras (~1h en Base 2s)")
    ap.add_argument("--win", type=int, default=150, help="ancho de ventana por muestra (~5min)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=OUT)
    args = ap.parse_args()

    bn = int(rpc("eth_blockNumber", [])["result"], 16)
    print(f"current block {bn}; buscando limite de pruning…")
    oldest = oldest_available(bn)
    ts_old, ts_now = block_ts(oldest), block_ts(bn)
    days = (ts_now - ts_old) / 86400
    print(f"historia disponible en Waizard: bloque {oldest} -> {bn}  (~{days:.1f} dias)")

    sample_blocks = list(range(oldest, bn, args.stride))
    print(f"muestreando {len(sample_blocks)} ventanas de {args.win} bloques (stride {args.stride})…")

    rows = {}
    def one(blk):
        px = sample_window(blk, args.win)
        if px is None:
            return None
        ts = block_ts(blk)
        return (ts, px)
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for fut in as_completed([ex.submit(one, b) for b in sample_blocks]):
            res = fut.result()
            done += 1
            if res:
                rows[res[0]] = res[1]
            if done % 50 == 0:
                print(f"  {done}/{len(sample_blocks)}  ({len(rows)} con precio)", end="\r")

    ordered = sorted(rows.items())
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "price"])
        for ts, px in ordered:
            w.writerow([ts, f"{px:.6f}"])
    print()
    if ordered:
        span = (ordered[-1][0] - ordered[0][0]) / 86400
        print(f"escrito {args.out}: {len(ordered)} muestras, {span:.1f} dias")
        print(f"  ETH: ${ordered[0][1]:,.0f} ({time.strftime('%m-%d',time.gmtime(ordered[0][0]))})"
              f" -> ${ordered[-1][1]:,.0f} ({time.strftime('%m-%d',time.gmtime(ordered[-1][0]))})")
    else:
        print("sin muestras (RPC sin historia?)")


if __name__ == "__main__":
    main()
