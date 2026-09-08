#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_eth_history — baja klines horarias de ETHUSDT de Binance (publico, sin key)
=================================================================================

El framework solo tenia data de swaps de 2-7 dias. Para forecastear volatilidad y
backtestear decisiones de ancho de rango necesitamos >6 meses. Esto baja ~18 meses
de velas de 1h y las deja en data/eth_hourly.csv con columnas: ts (unix s), price
(close), high, low, volume.

Binance klines endpoint: 1000 velas/request, paginamos hacia atras.
"""
import os, csv, time, sys, argparse
import requests

DATADIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data"))

SYMBOL = "ETHUSDT"
INTERVAL = "1h"
HOURS_BACK = 18 * 30 * 24            # ~18 meses
LIMIT = 1000
HOSTS = ["https://api.binance.com", "https://data-api.binance.vision"]


def fetch_klines(end_ms, limit, host, symbol=SYMBOL):
    url = f"{host}/api/v3/klines"
    params = {"symbol": symbol, "interval": INTERVAL, "endTime": end_ms, "limit": limit}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default=SYMBOL, help="par Binance, ej ETHUSDT / BTCUSDT")
    ap.add_argument("--out", default=None, help="csv de salida (default data/<symbol>_hourly.csv)")
    ap.add_argument("--hours-back", type=int, default=HOURS_BACK)
    args = ap.parse_args()
    symbol = args.symbol
    hours_back = args.hours_back
    OUT = args.out or os.path.join(DATADIR, f"{symbol.replace('USDT','').lower()}_hourly.csv")
    OUT = os.path.abspath(OUT)
    now_ms = int(time.time() * 1000)
    target_oldest = now_ms - hours_back * 3600 * 1000
    rows = {}  # open_time_ms -> [ts, close, high, low, vol]
    end = now_ms
    host = None
    # elegir host que responda
    for h in HOSTS:
        try:
            fetch_klines(now_ms, 5, h, symbol); host = h; break
        except Exception as e:
            print(f"  host {h} fallo: {e}", file=sys.stderr)
    if host is None:
        print("ningun host de Binance respondio", file=sys.stderr); sys.exit(1)
    print(f"usando host {host}")

    while end > target_oldest:
        kl = fetch_klines(end, LIMIT, host, symbol)
        if not kl:
            break
        for k in kl:
            ot = int(k[0])
            rows[ot] = [ot // 1000, float(k[4]), float(k[2]), float(k[3]), float(k[5])]
        oldest = int(kl[0][0])
        if oldest >= end:  # no avanzamos, evitar loop infinito
            break
        end = oldest - 1
        time.sleep(0.25)
        print(f"  ...{len(rows)} velas, mas viejo: {time.strftime('%Y-%m-%d', time.gmtime(oldest/1000))}", end="\r")

    ordered = sorted(rows.values(), key=lambda x: x[0])
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "price", "high", "low", "volume"])
        w.writerows(ordered)

    span_days = (ordered[-1][0] - ordered[0][0]) / 86400
    print()
    print(f"escrito {OUT}")
    print(f"  velas: {len(ordered)}")
    print(f"  desde: {time.strftime('%Y-%m-%d', time.gmtime(ordered[0][0]))}")
    print(f"  hasta: {time.strftime('%Y-%m-%d', time.gmtime(ordered[-1][0]))}")
    print(f"  span:  {span_days:.0f} dias ({span_days/30:.1f} meses)")
    assert span_days > 183, "MENOS de 6 meses de data!"
    print("  OK: >6 meses")


if __name__ == "__main__":
    main()
