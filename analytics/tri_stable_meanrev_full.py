#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tri-stable mean reversion end-to-end con:
- Precio STABLE/ETH correcto según orden token0/token1.
- Volumen normalizado por barra: vol_stable, vol_eth, vol_usd_aprx.
- Johansen -> spread estacionario + OU/half-life.
- Señal por z-score, backtest con costos.
- Triple-barrier + meta-labeling (LogReg) con guardas.
- Filtro de "diferencia real" entre precios (basis bps).
- Sizing fijo por entrada: 1000 stable por leg (configurable).
- CSV de ENTRADAS con órdenes sugeridas por leg y equivalentes en ETH.

Requisitos:
  pip install pandas numpy statsmodels scikit-learn jinja2
"""

import argparse, os, glob, math, json, re, datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, List

import numpy as np
import pandas as pd
from statsmodels.tsa.vector_ar.vecm import coint_johansen
from statsmodels.api import OLS, add_constant
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from jinja2 import Template

# --------------------- Constantes --------------------- #

Q96 = float(2**96)
STABLES = ["DAI", "USDC", "USDT"]
DEC = {"DAI": 18, "USDC": 6, "USDT": 6, "ETH": 18}

# Orden on-chain de los pools:
# - USDCETH*** : token0=USDC (6), token1=WETH (18)  -> token0 es STABLE
# - DAIETH***  : token0=DAI  (18), token1=WETH (18)  -> token0 es STABLE
# - USDTETH*** : token0=WETH (18), token1=USDT (6)   -> token0 es ETH
TOKEN0_IS_STABLE = {"USDC": True, "DAI": True, "USDT": False}

@dataclass
class Cfg:
    data_dir: str = "../data"
    preferred_fee: Optional[int] = None  # 100/500/3000
    bar: str = "1min"
    z_entry: float = 2.0
    z_exit: float = 0.25
    roll_z: int = 720        # ~12h si bar=1min
    cost_bps: float = 0.5    # ida+vuelta aproximada
    k_ar_diff: int = 1
    # triple-barrier
    tb_horizon: int = 720    # barras máx. en posición (12h si 1min)
    tb_stop_z: float = 3.5   # stop si |z| supera esto en contra
    meta_threshold: float = 0.55  # prob mínima para ejecutar
    use_kalman: bool = False # RLS para betas dinámicas
    # entradas / filtro adicional
    unit_stable: float = 1000.0   # tamaño por leg (stable)
    min_basis_bps: float = 5.0    # diferencia mínima entre precios (max pairwise bps) para habilitar entrada

# --------------------- Helpers de fees, precio y volumen --------------------- #

def fee_from_contract_name(name: str) -> int:
    digits = ''.join([c for c in name if c.isdigit()])
    try:
        return int(digits)
    except:
        return 0

def fee_from_filename(path: str) -> Optional[int]:
    # .../DAIETH500_Swap.csv -> 500
    m = re.search(r"ETH(\d+)_Swap\.csv$", os.path.basename(path))
    if m:
        fee = int(m.group(1))
        return fee if fee in {100,500,3000} else None
    return None

def price_stable_per_eth_oriented(sqrtPriceX96: float, dec0: int, dec1: int, token0_is_stable: bool) -> float:
    """
    Calcula STABLE/ETH desde sqrtPrice, respetando el orden (token0, token1):
    p10 = token1/token0 ajustado por decimales.
    - Si token0 es STABLE y token1 es ETH: STABLE/ETH = 1 / p10
    - Si token0 es ETH y token1 es STABLE: STABLE/ETH = p10
    """
    p10 = (sqrtPriceX96 / Q96) ** 2
    p10 *= 10 ** (dec0 - dec1)
    return 1.0 / p10 if token0_is_stable else p10

def normalize_amounts_to_volume(row: pd.Series, stable: str) -> Tuple[float, float]:
    """
    Devuelve (delta_stable, delta_eth) en unidades naturales (stable units, ETH),
    respetando orden token0/token1 del pool para ese stable.
    Usa amount0/amount1 y ajusta por decimales.
    """
    token0_is_stable = TOKEN0_IS_STABLE[stable]
    dec_st = DEC[stable]
    dec_eth = DEC["ETH"]

    amt0 = float(row.get("amount0", 0.0)) if not pd.isna(row.get("amount0", np.nan)) else 0.0
    amt1 = float(row.get("amount1", 0.0)) if not pd.isna(row.get("amount1", np.nan)) else 0.0

    if token0_is_stable:
        # token0 = STABLE, token1 = ETH
        delta_stable = amt0 / (10 ** dec_st)
        delta_eth    = amt1 / (10 ** dec_eth)
    else:
        # token0 = ETH, token1 = STABLE
        delta_eth    = amt0 / (10 ** dec_eth)
        delta_stable = amt1 / (10 ** dec_st)

    return delta_stable, delta_eth

# --------------------- Carga de CSV --------------------- #

def load_pool_csv(path: str) -> pd.DataFrame:
    usecols = [
        "contract_name","sqrtPriceX96","tick","timestamp","tx_hash","liquidity",
        "amount0","amount1"
    ]
    df = pd.read_csv(
        path,
        header=0,
        usecols=usecols,
        low_memory=False,
        na_values=["", "NaN", "nan", "null", "None"]
    )

    # numéricos robustos
    for c in ["sqrtPriceX96", "liquidity", "amount0", "amount1"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["tick", "timestamp"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    na_tick = int(df["tick"].isna().sum())
    na_ts   = int(df["timestamp"].isna().sum())
    if na_tick or na_ts:
        print(f"[WARN][{os.path.basename(path)}] NA tick={na_tick} | NA timestamp={na_ts} -> dropping rows")
    df = df.dropna(subset=["tick", "timestamp"])

    # Cast finales
    df["tick"] = df["tick"].astype("int64")
    df["timestamp"] = df["timestamp"].astype("int64")

    df = df.drop_duplicates(subset=["tx_hash"]).sort_values("timestamp")
    df["ts"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    return df

# --------------------- Selección de fee y series --------------------- #

def select_best_fee(df_all: pd.DataFrame, stable: str, preferred_fee: Optional[int]) -> pd.DataFrame:
    cand = df_all[df_all["stable"] == stable]
    if preferred_fee in {100,500,3000}:
        pick = cand[cand["fee"] == preferred_fee]
        if not pick.empty:
            return pick
    grp = cand.groupby("fee")["liquidity"].median().sort_values(ascending=False)
    best = int(grp.index[0])
    return cand[cand["fee"] == best]

def make_price_volume_series_for_stable(cfg: Cfg, stable: str) -> Tuple[pd.Series, pd.DataFrame]:
    """
    Retorna:
      - serie de precio STABLE/ETH resampleada (last + ffill)
      - DataFrame con volumen por barra: columns=[vol_stable, vol_eth, vol_usd_aprx]
    """
    print(f"[LOAD] Buscando archivos para {stable} en {cfg.data_dir} …")
    files = []
    for fee in (100,500,3000):
        files += glob.glob(os.path.join(cfg.data_dir, f"{stable}ETH{fee}_Swap.csv"))
    if not files:
        raise FileNotFoundError(f"No encontré CSVs para {stable} en {cfg.data_dir}")

    parts = []
    for f in files:
        df = load_pool_csv(f)
        df["stable"] = stable
        f_fee = fee_from_filename(f)
        if f_fee is None:
            f_fee = fee_from_contract_name(str(df["contract_name"].iloc[0]))
            if f_fee not in {100,500,3000}:
                print(f"[WARN][{os.path.basename(f)}] Fee inválido detectado (contract_name={f_fee}). Descarto.")
                continue
        df["fee"] = f_fee
        parts.append(df)

    if not parts:
        raise FileNotFoundError(f"No quedaron CSVs válidos (fees) para {stable} en {cfg.data_dir}")

    allp = pd.concat(parts, ignore_index=True)
    allp = allp[allp["fee"].isin([100,500,3000])].copy()

    sel = select_best_fee(allp, stable, cfg.preferred_fee)

    # Precio STABLE/ETH con orientación correcta
    token0_is_stable = TOKEN0_IS_STABLE[stable]
    if token0_is_stable:
        dec0, dec1 = DEC[stable], DEC["ETH"]
    else:
        dec0, dec1 = DEC["ETH"], DEC[stable]

    sel["px"] = sel["sqrtPriceX96"].astype(np.float64).map(
        lambda s: price_stable_per_eth_oriented(s, dec0, dec1, token0_is_stable)
    )

    # Volumen normalizado por fila (un swap)
    vols = sel.apply(lambda r: normalize_amounts_to_volume(r, stable), axis=1, result_type="expand")
    vols.columns = ["delta_stable", "delta_eth"]
    sel = pd.concat([sel, vols], axis=1)

    # Resampleo a barra: precio last + ffill; volúmenes sum(abs(delta))
    s_px = sel.set_index("ts")["px"].sort_index().resample(cfg.bar).last().ffill().rename(f"{stable}ETH")
    vol_df = sel.set_index("ts")[["delta_stable","delta_eth"]].abs().sort_index().resample(cfg.bar).sum()
    # Aprox USD ~ cantidad de stable movida (peg~1)
    vol_df["vol_usd_aprx"] = vol_df["delta_stable"]
    vol_df.columns = [f"{stable}_vol_stable", f"{stable}_vol_eth", f"{stable}_vol_usd_aprx"]

    chosen_fee = int(sel["fee"].mode().iloc[0])
    print(f"[LOAD] {stable}: {len(s_px)} barras | fee elegido={chosen_fee}")
    return s_px, vol_df

# --------------------- Cointegración y OU --------------------- #

def johansen_w(log_df: pd.DataFrame, k_ar_diff: int = 1) -> np.ndarray:
    y = log_df.values
    jres = coint_johansen(y, det_order=0, k_ar_diff=k_ar_diff)
    w = jres.evec[:, 0]
    w = w / np.linalg.norm(w)
    return w

def fit_ou(spread: pd.Series) -> Tuple[float,float,float,float]:
    s = spread.dropna()
    X = add_constant(s.shift(1).dropna())
    y = s.loc[X.index]
    res = OLS(y, X).fit()
    phi = float(res.params.iloc[1])
    c   = float(res.params.iloc[0])
    mu = c / (1 - phi) if abs(1-phi) > 1e-8 else s.mean()
    kappa = -np.log(max(min(phi, 0.999999), 1e-6))
    hl = np.log(2) / max(kappa, 1e-12)
    return mu, phi, kappa, hl

# --------------------- RLS/Kalman-like betas dinámicas --------------------- #

def rls_dynamic_spread(log_df: pd.DataFrame, lam: float = 0.995, delta: float = 1e-4):
    """
    Regresión online: USDC = alpha + b1*DAI + b2*USDT
    Retorna: spread_t = USDC - (alpha + b1*DAI + b2*USDT) y parámetros por tiempo.
    """
    y = log_df["USDCETH"].values
    X = log_df[["DAIETH","USDTETH"]].values
    n = X.shape[0]
    theta = np.zeros(3)   # [b1, b2, alpha]
    P = (1.0/delta) * np.eye(3)
    b1s, b2s, alphas, spread = [], [], [], []

    for t in range(n):
        xt = np.array([X[t,0], X[t,1], 1.0])
        yhat = theta @ xt
        err = y[t] - yhat
        denom = lam + xt @ P @ xt
        K = (P @ xt) / denom
        theta = theta + K * err
        P = (P - np.outer(K, xt) @ P) / lam
        b1s.append(theta[0]); b2s.append(theta[1]); alphas.append(theta[2])
        spread.append(err)

    dyn = pd.DataFrame({"b1": b1s, "b2": b2s, "alpha": alphas, "spread_rls": spread}, index=log_df.index)
    return dyn

# --------------------- Señales y backtest --------------------- #

def compute_z(spread: pd.Series, roll: int) -> pd.Series:
    m = spread.rolling(roll).mean()
    sd = spread.rolling(roll).std()
    return (spread - m) / sd

def base_signal(z: pd.Series, z_entry: float, z_exit: float) -> pd.Series:
    sig = pd.Series(0, index=z.index, dtype=np.int8)
    sig[z >  z_entry] = -1
    sig[z < -z_entry] =  1
    maintain = sig.replace(0, np.nan).ffill().fillna(0)
    maintain[(z.abs() < z_exit)] = 0
    maintain = maintain.replace(0, np.nan).ffill().fillna(0).astype(np.int8)
    return maintain

def pnl_from_signal(sig: pd.Series, w: np.ndarray, log_prices: pd.DataFrame, cost_bps: float) -> Tuple[pd.Series, pd.Series]:
    dy = log_prices.diff().fillna(0.0)
    w_norm = w / np.sum(np.abs(w))
    comb_ret = (dy @ w_norm).reindex(sig.index).fillna(0.0)
    gross = sig.shift(1).fillna(0) * comb_ret
    trades = sig.diff().abs().fillna(0)
    cost = trades * (cost_bps / 1e4) * comb_ret.abs().rolling(50).mean().fillna(0)  # proxy conservadora
    net = gross - cost
    eq = (1 + net).cumprod()
    return net, eq

# --------------------- Triple-barrier + meta-labeling --------------------- #

def build_entries(z: pd.Series, base_sig: pd.Series, z_exit: float, stop_z: float, horizon: int):
    entries = []
    in_pos = 0
    direction = 0

    for i, (ts, s) in enumerate(base_sig.items()):
        if in_pos == 0 and s != 0:
            in_pos = 1
            t0 = ts
            direction = int(s)
            outcome, t_end = 0, None
            window = z.loc[ts:].iloc[1:1+horizon]
            for ts2, z2 in window.items():
                if abs(z2) < z_exit:  # éxito (reversión a banda neutra)
                    outcome, t_end = 1, ts2
                    break
                if (direction == 1 and z2 < -stop_z) or (direction == -1 and z2 > stop_z):
                    outcome, t_end = 0, ts2
                    break
            if t_end is None:
                t_end = window.index[-1] if len(window) else ts
            entries.append({"t0": t0, "dir": direction, "t_end": t_end, "label": outcome})
        elif in_pos == 1 and s == 0:
            in_pos = 0
            direction = 0
    return pd.DataFrame(entries)

def features_at(log_prices: pd.DataFrame, z: pd.Series, spread: pd.Series, t0: pd.Timestamp, halflife: float) -> Dict:
    win = 120
    idx = z.index.get_loc(t0)
    z_hist = z.iloc[max(0, idx-win):idx+1]
    sp_hist = spread.iloc[max(0, idx-win):idx+1]
    z_slope = z_hist.diff().mean() if len(z_hist) > 1 else 0.0
    z_vol = z_hist.std() if len(z_hist) > 1 else 0.0
    spd_vol = sp_hist.std() if len(sp_hist) > 1 else 0.0
    return {"z": float(z.loc[t0]), "abs_z": float(abs(z.loc[t0])), "z_slope": float(z_slope or 0.0),
            "z_vol": float(z_vol or 0.0), "spread_vol": float(spd_vol or 0.0), "halflife": float(halflife)}

def meta_labeling(log_prices: pd.DataFrame, spread: pd.Series, z: pd.Series, base_sig: pd.Series,
                  z_exit: float, stop_z: float, horizon: int, halflife: float, prob_threshold: float):
    print("[META] Generando dataset triple-barrier …")
    entries = build_entries(z, base_sig, z_exit, stop_z, horizon)
    n = len(entries)
    if n == 0:
        print("[META] No hay entradas para meta-labeling; uso señal base.")
        return base_sig.copy(), None, None
    if n < 25:
        print(f"[META] Pocas entradas (n={n}). Salteo meta-labeling y uso señal base.")
        return base_sig.copy(), entries, {"note": f"skipped_meta_n={n}"}

    feats = [features_at(log_prices, z, spread, row["t0"], halflife) for _, row in entries.iterrows()]
    X = pd.DataFrame(feats, index=entries["t0"])
    y = entries["label"].values.astype(int)

    cutoff = int(len(X)*0.7)
    if cutoff < 1 or (len(X) - cutoff) < 1:
        print(f"[META] Split sin muestras suficientes (len={len(X)}, cutoff={cutoff}). Uso señal base.")
        return base_sig.copy(), entries, {"note": "split_empty"}

    scaler = StandardScaler()
    Xtr, Xte = X.iloc[:cutoff], X.iloc[cutoff:]
    ytr, yte = y[:cutoff], y[cutoff:]
    Xtr_s = scaler.fit_transform(Xtr)
    Xte_s = scaler.transform(Xte)

    if Xtr_s.shape[0] < 1:
        print("[META] Train vacío tras escalar. Uso señal base.")
        return base_sig.copy(), entries, {"note": "train_empty"}

    clf = LogisticRegression(max_iter=200, class_weight="balanced")
    clf.fit(Xtr_s, ytr)

    X_all_s = scaler.transform(X)
    p_all = clf.predict_proba(X_all_s)[:,1]
    entries["proba"] = p_all

    meta_sig = base_sig.copy()
    allowed_starts = set(entries.loc[entries["proba"] >= prob_threshold, "t0"].astype("datetime64[ns, UTC]"))
    for ts in entries["t0"]:
        if ts not in allowed_starts:
            meta_sig.loc[ts] = 0
    meta_sig = meta_sig.replace(0, np.nan).ffill().fillna(0).astype(np.int8)

    acc = (clf.score(Xte_s, yte) if len(Xte_s) else float("nan"))
    print(f"[META] Entradas={n} | keep={int((entries['proba']>=prob_threshold).sum())} | Test acc={acc:.3f}")
    return meta_sig, entries, {"acc_test": float(acc), "n": int(n)}

# --------------------- Filtro basis (diferencia real) y CSV de entradas --------------------- #

def max_pairwise_basis_bps(p: pd.Series) -> float:
    # p: Series con [DAIETH, USDCETH, USDTETH]
    vals = p.values.astype(float)
    if len(vals) != 3 or np.any(~np.isfinite(vals)):
        return np.nan
    maxdiff = max(abs(vals[0]-vals[1]), abs(vals[0]-vals[2]), abs(vals[1]-vals[2]))
    avgp = np.mean(vals)
    if avgp <= 0:
        return np.nan
    return (maxdiff / avgp) * 1e4  # bps

def generate_entry_csv(path: str, sig: pd.Series, z: pd.Series, prices: pd.DataFrame,
                       w: np.ndarray, unit_stable: float, min_basis_bps: float):
    """
    Genera CSV con filas SOLO de ENTRADA (cambio 0->±1) cuando el basis supera min_basis_bps.
    Para cada leg, asigna +/- unit_stable según el signo de (sig * w_norm_i).
    Incluye equivalente en ETH para referencia (qty_eth = notional_stable / price_i).
    """
    print("[SIGNALS] Construyendo CSV de entradas…")
    w_norm = w / np.sum(np.abs(w))
    rows = []
    prev = 0
    for ts, curr in sig.items():
        if prev == 0 and curr != 0:
            # check basis
            p = prices.loc[ts, ["DAIETH","USDCETH","USDTETH"]]
            basis = max_pairwise_basis_bps(p)
            if not np.isfinite(basis) or basis < min_basis_bps:
                prev = curr
                continue  # no entry; diferencia insuficiente

            side = "LONG_SPREAD" if curr > 0 else "SHORT_SPREAD"
            # por leg
            leg_info = {}
            for i, name in enumerate(["DAIETH","USDCETH","USDTETH"]):
                sign_i = np.sign(curr * w_norm[i])  # +long, -short en ese leg
                notional_stable = unit_stable * abs(w_norm[i])  # tamaño fijo ponderado por |w|
                qty_eth = notional_stable / float(p[name]) if float(p[name]) > 0 else np.nan
                leg_info[name] = {
                    "side": "LONG" if sign_i > 0 else "SHORT",
                    "notional_stable": round(notional_stable, 6),
                    "qty_eth_equiv": round(qty_eth, 8)
                }

            row = {
                "timestamp": ts.isoformat(),
                "side": side,
                "z_score": float(z.loc[ts]) if ts in z.index else np.nan,
                "basis_bps": float(basis),
                "DAIETH": float(p["DAIETH"]),
                "USDCETH": float(p["USDCETH"]),
                "USDTETH": float(p["USDTETH"]),
                # legs
                "DAI_leg_side": leg_info["DAIETH"]["side"],
                "DAI_leg_notional_stable": leg_info["DAIETH"]["notional_stable"],
                "DAI_leg_qty_eth": leg_info["DAIETH"]["qty_eth_equiv"],
                "USDC_leg_side": leg_info["USDCETH"]["side"],
                "USDC_leg_notional_stable": leg_info["USDCETH"]["notional_stable"],
                "USDC_leg_qty_eth": leg_info["USDCETH"]["qty_eth_equiv"],
                "USDT_leg_side": leg_info["USDTETH"]["side"],
                "USDT_leg_notional_stable": leg_info["USDTETH"]["notional_stable"],
                "USDT_leg_qty_eth": leg_info["USDTETH"]["qty_eth_equiv"],
            }
            rows.append(row)
        prev = curr

    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
    print(f"[SIGNALS] {len(df)} entradas escritas en {path}")

# --------------------- Reporte HTML --------------------- #

REPORT_TMPL = """
<!doctype html>
<html><head><meta charset="utf-8"><title>Tri-Stable Mean Reversion Report</title>
<style>
body{font-family:system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin:24px;}
h1,h2{margin:0 0 8px}
table{border-collapse:collapse; margin:12px 0; width:100%;}
th,td{border:1px solid #ddd; padding:6px 8px; text-align:right}
th{text-align:left; background:#fafafa}
code{background:#f4f4f4;padding:2px 4px}
small{color:#666}
</style></head><body>
<h1>Tri-Stable Mean Reversion</h1>
<small>Generated: {{ now }}</small>

<h2>Parameters</h2>
<table>
<tr><th>Bar</th><td>{{ bar }}</td><th>z_entry</th><td>{{ z_entry }}</td><th>z_exit</th><td>{{ z_exit }}</td></tr>
<tr><th>roll_z</th><td>{{ roll_z }}</td><th>cost_bps</th><td>{{ cost_bps }}</td><th>k_ar_diff</th><td>{{ k_ar_diff }}</td></tr>
<tr><th>tb_horizon</th><td>{{ tb_horizon }}</td><th>tb_stop_z</th><td>{{ tb_stop_z }}</td><th>meta_threshold</th><td>{{ meta_threshold }}</td></tr>
<tr><th>unit_stable</th><td>{{ unit_stable }}</td><th>min_basis_bps</th><td>{{ min_basis_bps }}</td><th>use_kalman</th><td>{{ use_kalman }}</td></tr>
<tr><th colspan="2">Data dir</th><td colspan="4">{{ data_dir }}</td></tr>
</table>

<h2>Johansen Weights</h2>
<p><code>w = {{ w }}</code></p>

<h2>OU Parameters</h2>
<table>
<tr><th>mu</th><th>phi</th><th>kappa</th><th>half-life (bars)</th></tr>
<tr><td>{{ mu }}</td><td>{{ phi }}</td><td>{{ kappa }}</td><td>{{ hl }}</td></tr>
</table>

<h2>Backtest Stats</h2>
<table>
<tr><th></th><th>Return %</th><th>Sharpe</th><th>Turnover</th><th>Hit Ratio</th></tr>
<tr><th>Base</th><td>{{ base.ret_total }}</td><td>{{ base.sharpe }}</td><td>{{ base.turnover }}</td><td>{{ base.hit }}</td></tr>
<tr><th>Meta</th><td>{{ meta.ret_total }}</td><td>{{ meta.sharpe }}</td><td>{{ meta.turnover }}</td><td>{{ meta.hit }}</td></tr>
</table>

<h2>Monthly PnL (%)</h2>
{{ monthly_table }}

</body></html>
"""

def render_html_report(path: str, params: dict, monthly_df: pd.DataFrame):
    t = Template(REPORT_TMPL)
    tbl = monthly_df.to_html(index=True, float_format=lambda x: f"{x:,.2f}")
    html = t.render(now=dt.datetime.utcnow().isoformat() + "Z",
                    monthly_table=tbl, **params)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

# --------------------- Main --------------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", type=str, default="../data")
    ap.add_argument("--preferred_fee", type=int, default=None)
    ap.add_argument("--bar", type=str, default="1min")
    ap.add_argument("--z_entry", type=float, default=2.0)
    ap.add_argument("--z_exit", type=float, default=0.25)
    ap.add_argument("--roll_z", type=int, default=720)
    ap.add_argument("--k_ar_diff", type=int, default=1)
    ap.add_argument("--cost_bps", type=float, default=0.5)
    ap.add_argument("--tb_horizon", type=int, default=720)
    ap.add_argument("--tb_stop_z", type=float, default=3.5)
    ap.add_argument("--meta_threshold", type=float, default=0.55)
    ap.add_argument("--use_kalman", type=int, default=0, help="1 para activar RLS/Kalman betas dinámicas")
    ap.add_argument("--unit_stable", type=float, default=1000.0)
    ap.add_argument("--min_basis_bps", type=float, default=5.0)
    args = ap.parse_args()

    cfg = Cfg(
        data_dir=args.data_dir,
        preferred_fee=args.preferred_fee,
        bar=args.bar,
        z_entry=args.z_entry,
        z_exit=args.z_exit,
        roll_z=args.roll_z,
        cost_bps=args.cost_bps,
        k_ar_diff=args.k_ar_diff,
        tb_horizon=args.tb_horizon,
        tb_stop_z=args.tb_stop_z,
        meta_threshold=args.meta_threshold,
        use_kalman=bool(args.use_kalman),
        unit_stable=args.unit_stable,
        min_basis_bps=args.min_basis_bps
    )

    print("[START] Tri-stable mean reversion pipeline")
    print(json.dumps(cfg.__dict__, indent=2))

    # 1) Series por stable (precio + volúmenes)
    price_series = {}
    volume_frames = []
    for s in STABLES:
        px_s, vol_s = make_price_volume_series_for_stable(cfg, s)
        price_series[s] = px_s
        volume_frames.append(vol_s)

    # 2) Join y logs
    px = pd.concat([price_series["DAI"], price_series["USDC"], price_series["USDT"]], axis=1).dropna()
    px.columns = ["DAIETH","USDCETH","USDTETH"]
    vol_all = pd.concat(volume_frames, axis=1).reindex(px.index).fillna(0.0)

    ylog = np.log(px)
    print(f"[DATA] Barras sincronizadas: {len(ylog)} | {ylog.index[0]} → {ylog.index[-1]}")

    # 3) Spread
    if cfg.use_kalman:
        print("[SPREAD] Estimando betas dinámicas (RLS/Kalman-like)…")
        dyn = rls_dynamic_spread(ylog)
        spread = dyn["spread_rls"].rename("spread")
        b1, b2, a = dyn["b1"].mean(), dyn["b2"].mean(), dyn["alpha"].mean()
        w = np.array([-b1, 1.0, -b2], dtype=float)
        w = w / np.linalg.norm(w)
        print(f"[SPREAD] RLS medias: b1={b1:.4f}, b2={b2:.4f}, alpha={a:.6f}, w≈{np.round(w,4)}")
    else:
        print("[SPREAD] Johansen cointegration…")
        w = johansen_w(ylog, cfg.k_ar_diff)
        spread = (ylog @ w).rename("spread")
        print(f"[SPREAD] w={np.round(w,4)}")

    # 4) OU
    print("[OU] Ajustando OU/AR(1) sobre el spread…")
    mu, phi, kappa, hl = fit_ou(spread)
    print(f"[OU] mu={mu:.6f} | phi={phi:.6f} | kappa={kappa:.6f} | half-life={hl:.2f} barras")

    # 5) z-score + señal base
    print("[SIGNAL] z-score y señal base…")
    z = compute_z(spread, cfg.roll_z)
    sig_base = base_signal(z, cfg.z_entry, cfg.z_exit)

    # 6) Meta-labeling
    sig_meta, entries, meta_stats = meta_labeling(
        log_prices=ylog, spread=spread, z=z, base_sig=sig_base,
        z_exit=cfg.z_exit, stop_z=cfg.tb_stop_z, horizon=cfg.tb_horizon,
        halflife=hl, prob_threshold=cfg.meta_threshold
    )

    # 7) Backtest base y meta
    print("[BT] Backtest señal base…")
    pnl_base, eq_base = pnl_from_signal(sig_base, w, ylog, cfg.cost_bps)
    print("[BT] Backtest señal meta…")
    pnl_meta, eq_meta = pnl_from_signal(sig_meta, w, ylog, cfg.cost_bps)

    sharpe_base = (pnl_base.mean() / (pnl_base.std() + 1e-12)) * np.sqrt(365*24*60)
    sharpe_meta = (pnl_meta.mean() / (pnl_meta.std() + 1e-12)) * np.sqrt(365*24*60)

    stats_base = {
        "ret_total_%": float((eq_base.iloc[-1] - 1) * 100) if len(eq_base)>0 else 0.0,
        "sharpe": float(sharpe_base),
        "turnover": float(sig_base.diff().abs().mean()),
        "hit_ratio": float((np.sign(pnl_base) > 0).mean())
    }
    stats_meta = {
        "ret_total_%": float((eq_meta.iloc[-1] - 1) * 100) if len(eq_meta)>0 else 0.0,
        "sharpe": float(sharpe_meta),
        "turnover": float(sig_meta.diff().abs().mean()),
        "hit_ratio": float((np.sign(pnl_meta) > 0).mean())
    }
    print("[STATS][BASE]", {k: round(v,4) for k,v in stats_base.items()})
    print("[STATS][META]", {k: round(v,4) for k,v in stats_meta.items()})

    # 8) Resumen mensual
    print("[SUMMARY] Resumen mensual (pnl %) …")
    monthly = pnl_meta.resample("M").sum() * 100.0
    monthly_df = pd.DataFrame({"PnL_%": monthly})
    print(monthly_df.round(3).to_string())

    # 9) Guardar outputs
    print("[SAVE] Escribiendo CSVs, señales y reporte…")
    out = pd.concat(
        [px, vol_all,
         spread.rename("spread"), z.rename("z"),
         sig_base.rename("signal_base"), sig_meta.rename("signal_meta"),
         pnl_base.rename("pnl_base"), pnl_meta.rename("pnl_meta"),
         eq_base.rename("equity_base"), eq_meta.rename("equity_meta")],
        axis=1
    )
    out.to_csv("tri_stable_meanrev_outputs.csv")
    monthly_df.to_csv("tri_stable_meanrev_monthly.csv")

    # 10) CSV de ENTRADAS (con filtro de basis y sizing de 1000 stable por leg)
    generate_entry_csv(
        path="tri_stable_meanrev_entries.csv",
        sig=sig_meta, z=z, prices=px, w=w,
        unit_stable=cfg.unit_stable, min_basis_bps=cfg.min_basis_bps
    )

    params = {
        "bar": cfg.bar, "z_entry": cfg.z_entry, "z_exit": cfg.z_exit, "roll_z": cfg.roll_z,
        "cost_bps": cfg.cost_bps, "k_ar_diff": cfg.k_ar_diff,
        "tb_horizon": cfg.tb_horizon, "tb_stop_z": cfg.tb_stop_z, "meta_threshold": cfg.meta_threshold,
        "unit_stable": cfg.unit_stable, "min_basis_bps": cfg.min_basis_bps,
        "use_kalman": cfg.use_kalman, "data_dir": cfg.data_dir,
        "w": np.round(w,6).tolist(),
        "mu": round(mu,6), "phi": round(phi,6), "kappa": round(kappa,6), "hl": round(hl,2),
        "base": {"ret_total": round(stats_base["ret_total_%"],2), "sharpe": round(stats_base["sharpe"],3),
                 "turnover": round(stats_base["turnover"],4), "hit": round(stats_base["hit_ratio"],3)},
        "meta": {"ret_total": round(stats_meta["ret_total_%"],2), "sharpe": round(stats_meta["sharpe"],3),
                 "turnover": round(stats_meta["turnover"],4), "hit": round(stats_meta["hit_ratio"],3)},
    }
    render_html_report("tri_stable_meanrev_report.html", params, monthly_df)

    print("[DONE] Escritos: tri_stable_meanrev_outputs.csv, tri_stable_meanrev_monthly.csv, tri_stable_meanrev_entries.csv y tri_stable_meanrev_report.html")

if __name__ == "__main__":
    main()
