#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_flow — variables de FLUJO y POSICIONAMIENTO para la "foto" direccional
============================================================================

Las features de vol (rv, parkinson, momentum) miden CUANTO se mueve, no PARA DONDE.
Para un modelo estilo-xG de dirección hace falta la geometría causal del trade:
quién está empujando (order-flow) y cómo está posicionado el mercado (funding/basis).

Baja de Binance Futures (USD-M perp ETHUSDT), 18 meses, 1h:
  - klines de FUTUROS  -> incluyen taker_buy_base_volume y n_trades (order-flow)   -> data/eth_perp_hourly.csv
  - funding rate hist  -> costo de carry long/short (cadencia 8h)                   -> data/eth_funding.csv

OI / long-short ratio NO se bajan: Binance solo retiene ~30d de esos endpoints
(igual que el problema de Waizard) y no sirven para el backtest de 18 meses.
"""
import os, csv, json, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DATADIR = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data"))
FAPI = "https://fapi.binance.com"
SYMBOL = "ETHUSDT"
MONTHS = 18
NOW_MS = None  # se setea en main con el reloj del sistema


def get(path, params):
    url = FAPI + path + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception as e:
            if attempt == 4:
                raise
            time.sleep(1.5 * (attempt + 1))


def fetch_perp_klines(now_ms):
    start = now_ms - MONTHS * 30 * 24 * 3600 * 1000
    out, cur = [], start
    while cur < now_ms:
        data = get("/fapi/v1/klines", {"symbol": SYMBOL, "interval": "1h",
                                       "startTime": cur, "limit": 1500})
        if not data:
            break
        for k in data:
            # 0 openTime,1 o,2 h,3 l,4 c,5 vol,6 closeTime,7 quoteVol,8 nTrades,
            # 9 takerBuyBaseVol,10 takerBuyQuoteVol
            out.append((int(k[0] // 1000), float(k[4]), float(k[2]), float(k[3]),
                        float(k[5]), float(k[9]), int(k[8])))
        nxt = int(data[-1][0]) + 3600 * 1000
        if nxt <= cur:
            break
        cur = nxt
        print(f"  perp klines: {len(out)}…", end="\r")
        time.sleep(0.25)
    path = os.path.join(DATADIR, "eth_perp_hourly.csv")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "price", "high", "low", "volume", "taker_buy", "trades"])
        w.writerows(out)
    span = (out[-1][0] - out[0][0]) / 86400
    print(f"\nescrito {path}: {len(out)} velas | span {span:.0f}d")


def fetch_funding(now_ms):
    start = now_ms - MONTHS * 30 * 24 * 3600 * 1000
    out, cur = [], start
    while cur < now_ms:
        data = get("/fapi/v1/fundingRate", {"symbol": SYMBOL, "startTime": cur,
                                            "limit": 1000})
        if not data:
            break
        for d in data:
            out.append((int(d["fundingTime"] // 1000), float(d["fundingRate"])))
        nxt = int(data[-1]["fundingTime"]) + 1
        if nxt <= cur or len(data) < 1000:
            cur = nxt
            if len(data) < 1000:
                break
        else:
            cur = nxt
        print(f"  funding: {len(out)}…", end="\r")
        time.sleep(0.25)
    # dedup por ts
    seen, ded = set(), []
    for ts, r in out:
        if ts not in seen:
            seen.add(ts); ded.append((ts, r))
    ded.sort()
    path = os.path.join(DATADIR, "eth_funding.csv")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "funding_rate"])
        w.writerows(ded)
    span = (ded[-1][0] - ded[0][0]) / 86400
    print(f"\nescrito {path}: {len(ded)} puntos (8h) | span {span:.0f}d")


def main():
    import datetime
    now_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    fetch_perp_klines(now_ms)
    fetch_funding(now_ms)


if __name__ == "__main__":
    main()
