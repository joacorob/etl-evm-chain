#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_feegrowth — fee REAL día por día, 365 días, del acumulador on-chain
=========================================================================

En vez de bajar millones de swaps, lee el acumulador `feeGrowthGlobal{0,1}X128` del
pool en bloques históricos (archive state, vía publicnode/drpc que SÍ lo sirven a 12m).
feeGrowthGlobalX128 = fees por unidad de liquidez acumulados (X128 fixed point). La
diferencia entre dos días × tu liquidez / 2^128 = los fees que cobraste esos días. Es
el "resumen diario" exacto, sin procesar swap por swap.

Por cada día (365): bloque ~= now - k·86400/12.05s; lee en ese bloque:
  feeGrowthGlobal0X128, feeGrowthGlobal1X128, slot0 (sqrtPriceX96), liquidity (L activa).
Escribe data/pool_feegrowth_daily.csv  (block, ts, sqrtP, price_usd, liquidity, fgg0, fgg1).

Pool: USDC/WETH 0.05% mainnet 0x88e6A0c2...  (token0=USDC 6dec, token1=WETH 18dec).
"""
import os, csv, json, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DATADIR = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data"))
OUT = os.path.join(DATADIR, "pool_feegrowth_daily.csv")

POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
RPCS = ["https://ethereum-rpc.publicnode.com", "https://eth.drpc.org"]
SEL_FGG0 = "0xf3058399"   # feeGrowthGlobal0X128()
SEL_FGG1 = "0x46141319"   # feeGrowthGlobal1X128()
SEL_SLOT0 = "0x3850c7bd"  # slot0()
SEL_LIQ = "0x1a686502"    # liquidity()
SEC_PER_BLOCK = 12.05
DAYS = 365
Q96 = 2 ** 96
_rpc_i = 0


def rpc(method, params, tries=6):
    global _rpc_i
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for a in range(tries):
        url = RPCS[_rpc_i % len(RPCS)]
        try:
            req = urllib.request.Request(url, data=payload,
                                         headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                j = json.load(r)
            if "result" in j and j["result"] not in (None, "0x"):
                return j["result"]
            raise RuntimeError(str(j.get("error"))[:60])
        except Exception as e:
            _rpc_i += 1                       # rota de RPC
            time.sleep(0.5 * (a + 1))
            last = e
    raise RuntimeError(f"rpc fail {method}: {last}")


def call_at(data, block):
    return rpc("eth_call", [{"to": POOL, "data": data}, hex(block)])


def block_ts(block):
    b = rpc("eth_getBlockByNumber", [hex(block), False])
    return int(b["timestamp"], 16)


def price_usd(sqrtP):
    raw = (sqrtP / Q96) ** 2
    return 1e12 / raw if raw > 0 else 0.0


def main():
    head = rpc("eth_getBlockByNumber", ["latest", False])
    bn = int(head["number"], 16); now = int(head["timestamp"], 16)
    print(f"head block {bn} ts {now}")
    rows = []
    for k in range(DAYS, -1, -1):                     # de hace 365d hasta hoy
        blk = bn - int(k * 86400 / SEC_PER_BLOCK)
        if blk < 1:
            continue
        try:
            ts = block_ts(blk)
            fgg0 = int(call_at(SEL_FGG0, blk), 16)
            fgg1 = int(call_at(SEL_FGG1, blk), 16)
            slot0 = call_at(SEL_SLOT0, blk)
            sqrtP = int(slot0[2:66], 16)
            liq = int(call_at(SEL_LIQ, blk), 16)
            rows.append((blk, ts, sqrtP, price_usd(sqrtP), liq, fgg0, fgg1))
            if k % 20 == 0:
                print(f"  d-{k:3d} blk {blk} ${price_usd(sqrtP):,.0f} L={liq} ({len(rows)} ok)", flush=True)
        except Exception as e:
            print(f"  d-{k} blk {blk} FAIL {str(e)[:50]}", flush=True)
        time.sleep(0.05)
    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["block", "ts", "sqrtP", "price_usd", "liquidity", "fgg0", "fgg1"])
        w.writerows(rows)
    print(f"escrito {OUT}: {len(rows)} días")


if __name__ == "__main__":
    main()
