#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, re, glob, math, warnings, sys
from typing import List, Dict, Tuple
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import classification_report, accuracy_score

STABLES = ("DAI", "USDC", "USDT")
TICK_SPACING: Dict[str, int] = {"100": 1, "500": 10, "3000": 60, "10000": 200}
FILE_RGX = re.compile(r"(?P<stable>DAI|USDC|USDT)ETH(?P<fee>100|500|3000)_Swap\.csv$", re.IGNORECASE)

def hr(msg=""):
    print("\n" + "="*16 + f" {msg} " + "="*16)

def mem(df: pd.DataFrame) -> str:
    try: return f"{df.memory_usage(deep=True).sum()/1024/1024:.2f} MB"
    except: return "N/A"

def summarize_dt(df: pd.DataFrame, ts_col="timestamp"):
    if len(df)==0: print("  ▸ (DF vacío)"); return
    print(f"  ▸ Rango temporal: {df[ts_col].min()} → {df[ts_col].max()} | Filas: {len(df)}")

# ---------------- CLI ----------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=str, default="../data")
    ap.add_argument("--freq", type=str, default="30s")  # menos RAM por defecto
    ap.add_argument("--horizon", type=int, default=300)

    # thresholds fijos (ticks crudos)
    ap.add_argument("--entry-ticks", type=float, default=None)
    ap.add_argument("--target-ticks", type=float, default=None)

    # thresholds por spacing del fee a operar
    ap.add_argument("--signal-fee", type=str, default="500", choices=list(TICK_SPACING.keys()))
    ap.add_argument("--entry-mult", type=float, default=0.3, help="múltiplos spacing para entrada base")
    ap.add_argument("--target-mult", type=float, default=0.6, help="múltiplos spacing para target base")

    # thresholds dinámicos por distribución
    ap.add_argument("--entry-q", type=float, default=0.60, help="quantile(|dev|) por stable para la barra de entrada")
    ap.add_argument("--target-frac", type=float, default=0.50, help="fracción de |dev actual| como target adicional")

    ap.add_argument("--prob-thr", type=float, default=0.55)
    ap.add_argument("--tmin", type=str, default=None)
    ap.add_argument("--tmax", type=str, default=None)
    ap.add_argument("--clip-pct", type=float, default=99.9, help="winsorize dev al pct y 100-pct")
    ap.add_argument("--report", action="store_true")
    return ap.parse_args()

# ---------------- IO ----------------
def find_files(data_dir: str) -> List[str]:
    hr("BUSCANDO ARCHIVOS")
    files = sorted(glob.glob(os.path.join(data_dir, "*ETH*_Swap.csv")))
    keep = []
    for f in files:
        b = os.path.basename(f)
        if FILE_RGX.search(b): keep.append(f); print(f"  ✓ {b}")
        else: print(f"  · Ignorado: {b}")
    if not keep: raise FileNotFoundError("No se encontraron CSVs {stable}ETH{fee}_Swap.csv")
    print(f"Total archivos válidos: {len(keep)}")
    return keep

def read_csv_ticks(path: str) -> pd.DataFrame:
    base = os.path.basename(path); m = FILE_RGX.search(base)
    stable, fee = m.group("stable").upper(), m.group("fee")
    print(f"\n[LECTURA] {base}  -> stable={stable} fee={fee}")
    usecols = ["timestamp","tick","tx_hash"]
    try:
        df = pd.read_csv(path, usecols=usecols)
    except Exception:
        df = pd.read_csv(path, usecols=["timestamp","tick","tx_hash"], engine="python", on_bad_lines="skip")

    if "tx_hash" in df.columns:
        before = len(df); df = df.drop_duplicates(subset=["tx_hash"]); print(f"  · Dedup tx_hash: {before}->{len(df)}")

    df = df.dropna(subset=["timestamp","tick"]).copy()
    df["tick"] = pd.to_numeric(df["tick"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["tick"]).copy()
    df["tick"] = df["tick"].astype(np.int64)
    df["timestamp"] = pd.to_datetime(df["timestamp"].astype(np.int64), unit="s", utc=True)
    df = df.sort_values("timestamp")
    print(f"  · Filas limpias: {len(df)} | tick median: {df['tick'].median()} | Mem: {mem(df)}")
    summarize_dt(df)

    # AUTO-POLARIDAD:
    # si median(tick) < 0 ⇒ token1/token0 < 1 (WETH/STABLE) ⇒ USD/ETH = -tick
    # si median(tick) > 0 ⇒ token1/token0 > 1 (STABLE/WETH) ⇒ USD/ETH =  tick
    med = df["tick"].median()
    if med < 0:
        usd_per_eth_tick = -df["tick"].values
        print("  · Polarity: median<0 → usd_per_eth_tick = -tick")
    else:
        usd_per_eth_tick =  df["tick"].values
        print("  · Polarity: median>0 → usd_per_eth_tick =  tick")

    out = pd.DataFrame({
        "timestamp": df["timestamp"].values,
        "usd_per_eth_tick": usd_per_eth_tick,
        "stable": stable, "fee": fee
    })
    return out

def resample_by_fee(df_fee: pd.DataFrame, freq: str) -> pd.DataFrame:
    freq = str(freq).lower()
    tmp = df_fee[["timestamp","usd_per_eth_tick"]].set_index("timestamp").resample(freq).last().ffill().infer_objects(copy=False)
    out = tmp.reset_index(); out["stable"] = df_fee["stable"].iloc[0]; out["fee"] = df_fee["fee"].iloc[0]
    print(f"  · Resample '{freq}': {len(out)} filas | Mem: {mem(out)}"); summarize_dt(out)
    return out

def median_across_fees(rows: pd.DataFrame, freq: str, tmin=None, tmax=None) -> pd.DataFrame:
    hr("SERIES EN TICKS POR STABLE (mediana fees)")
    dfs = []
    for st in STABLES:
        sub = rows[rows["stable"]==st][["timestamp","usd_per_eth_tick"]].copy()
        if sub.empty: raise ValueError(f"Falta data para {st}")
        sub = sub.groupby("timestamp", as_index=False).median(numeric_only=True)
        sub = sub.set_index("timestamp").resample(str(freq).lower()).last().ffill().infer_objects(copy=False)
        sub.columns = [st]; dfs.append(sub)
    merged = pd.concat(dfs, axis=1)
    if tmin: merged = merged[merged.index >= pd.to_datetime(tmin, utc=True)]
    if tmax: merged = merged[merged.index <= pd.to_datetime(tmax, utc=True)]
    before = len(merged); merged = merged.dropna(subset=list(STABLES)); after = len(merged)
    print(f"  · Merge & dropna(3 stables): {before}->{after}")
    out = merged.reset_index(); summarize_dt(out, "timestamp"); print(f"  · Mem: {mem(out)}")
    return out

# ---------------- features & labels ----------------
def compute_dynamic_bars(absdev: pd.Series, spacing: int,
                         entry_ticks_abs: float|None, entry_mult: float, entry_q: float,
                         target_ticks_abs: float|None, target_mult: float, target_frac: float
                         ) -> Tuple[float,float]:
    base_entry = entry_ticks_abs if entry_ticks_abs is not None else entry_mult*spacing
    base_target = target_ticks_abs if target_ticks_abs is not None else target_mult*spacing
    q_entry = float(np.nanquantile(absdev.values, entry_q)) if len(absdev) else base_entry
    entry_bar = max(base_entry, q_entry)  # asegura suficientes elegibles pero no “0”
    # target dinámico: como mínimo base_target, y proporcional al tamaño actual (se aplicará punto a punto)
    target_bar_base = max(base_target, 1.0)  # al menos 1 tick
    return entry_bar, target_bar_base

def build_features_and_labels(df_ticks: pd.DataFrame, freq_secs: int, horizon_secs: int,
                              spacing: int,
                              entry_ticks_abs: float|None, target_ticks_abs: float|None,
                              entry_mult: float, target_mult: float, entry_q: float, target_frac: float,
                              clip_pct: float|None):
    hr("FEATURES & LABELS (ticks + umbrales dinámicos por stable)")
    df = df_ticks.copy()
    df["consensus_tick"] = df[["DAI","USDC","USDT"]].median(axis=1)

    # DEV por stable
    for st in STABLES:
        dev = (df[st] - df["consensus_tick"]).astype(float)
        if clip_pct:
            lo, hi = np.nanpercentile(dev, [100-clip_pct, clip_pct])
            dev = np.clip(dev, lo, hi)
        df[f"dev_{st}_ticks"] = dev
        df[f"absdev_{st}"] = np.abs(dev)

    # Rolling (~5min si freq=10s -> 30)
    win = max(5, int(round(300 / max(1, freq_secs))))
    print(f"  · Rolling window pasos: {win}")
    for st in STABLES:
        a = df[f"absdev_{st}"]
        mean_ = a.rolling(win, min_periods=win//2).mean()
        std_  = a.rolling(win, min_periods=win//2).std(ddof=1).replace(0.0, np.nan)
        df[f"z_{st}"]   = ((a - mean_) / std_).fillna(0.0)
        df[f"vol_{st}"] = df[f"dev_{st}_ticks"].diff().rolling(win, min_periods=win//2).std(ddof=1).fillna(0.0)
        df[f"mom_{st}"] = df[f"dev_{st}_ticks"].diff().fillna(0.0)

    horizon_steps = max(1, int(round(horizon_secs / max(1, freq_secs))))
    print(f"  · Horizonte: {horizon_secs}s -> {horizon_steps} pasos")

    # Umbrales dinámicos por stable (imprimimos)
    bars = {}
    for st in STABLES:
        entry_bar, target_bar_base = compute_dynamic_bars(
            df[f"absdev_{st}"], spacing,
            entry_ticks_abs, entry_mult, entry_q,
            target_ticks_abs, target_mult, target_frac
        )
        bars[st] = (entry_bar, target_bar_base)
        print(f"  · BARRAS {st}: entry≈{entry_bar:.2f} ticks | target_base≈{target_bar_base:.2f} ticks | spacing={spacing}")

    # Labels
    for st in STABLES:
        abs_arr = df[f"absdev_{st}"].to_numpy(float)
        future_min = np.full_like(abs_arr, np.nan)
        for i in range(len(df)-horizon_steps):
            w = abs_arr[i+1:i+1+horizon_steps]
            if w.size: future_min[i] = np.nanmin(w)
        entry_bar, target_bar_base = bars[st]
        # target relativo (fracción del tamaño actual)
        dyn_target = np.maximum(target_bar_base, target_frac * abs_arr)
        improvement = abs_arr - future_min
        y = np.zeros(len(df), dtype=np.int8)
        y[(abs_arr >= entry_bar) & ((improvement >= dyn_target) | (future_min <= entry_bar/2.0))] = 1
        df[f"y_{st}"] = y
        print(f"  · Labels {st}: positivos={int(y.sum())} / N={len(y)}  (entry≥{entry_bar:.2f}; target≥max({target_bar_base:.2f}, {target_frac}*|dev|))")

        # guardamos barras por fila para usar en el train/signal
        df[f"entry_bar_{st}"]  = entry_bar
        df[f"target_bar_{st}"] = target_bar_base  # informativo

    summarize_dt(df); print("  · Mem:", mem(df))
    return df

# ---------------- model & signals ----------------
def train_and_signal(df: pd.DataFrame, prob_thr: float, signal_fee: str, report: bool):
    hr("TRAIN & SIGNAL (per stable)")
    results, signals = {}, []
    spacing = TICK_SPACING.get(signal_fee, 1)

    for st in STABLES:
        cols = [f"absdev_{st}", f"z_{st}", f"vol_{st}", f"mom_{st}", f"y_{st}", f"entry_bar_{st}", f"dev_{st}_ticks"]
        sub = df[["timestamp"] + cols].dropna()
        # máscara por entrada dinámica
        eligible = sub[sub[f"absdev_{st}"] >= sub[f"entry_bar_{st}"]]
        print(f"\n[{st}] elegibles dinámicos: {len(eligible)}")
        pos_total = int(eligible[f"y_{st}"].sum())
        print(f"    · Positivos totales (eligible): {pos_total}")

        if len(eligible) < 500 or pos_total < 50:
            warnings.warn(f"Datos escasos {st}: {len(eligible)} puntos, {pos_total} positivos.")
            continue

        X = eligible[[f"absdev_{st}", f"z_{st}", f"vol_{st}", f"mom_{st}"]]
        y = eligible[f"y_{st}"].astype(int).values

        tscv = TimeSeriesSplit(n_splits=5)
        tr, te = list(tscv.split(X))[-1]
        clf = LogisticRegression(max_iter=300, class_weight="balanced", solver="lbfgs")
        clf.fit(X.iloc[tr], y[tr])
        y_pred  = clf.predict(X.iloc[te])
        y_proba = clf.predict_proba(X.iloc[te])[:,1]
        acc = accuracy_score(y[te], y_pred)
        results[st] = {"accuracy": acc, "test_n": int(len(te)), "positives_test": int(y[te].sum())}
        print(f"    · ACC (último fold): {acc:.3f} | test_n={len(te)} | positives={int(y[te].sum())}")
        if report:
            print("    · Classification report:")
            print(classification_report(y[te], y_pred, digits=3, zero_division=0))

        # NOW (última fila)
        now_row = X.tail(1)
        now_idx = X.index[-1]
        dev_now = float(eligible.loc[now_idx, f"dev_{st}_ticks"])
        abs_now = abs(dev_now)
        entry_bar_now = float(eligible.loc[now_idx, f"entry_bar_{st}"])
        p_now = float(clf.predict_proba(now_row)[:,1][0])
        side = "SHORT" if dev_now>0 else "LONG"
        q_ticks = int(math.ceil(abs_now/spacing))*spacing
        pct = (1.0001**q_ticks - 1.0)*100.0

        print(f"    · NOW {st}: dev={dev_now:.2f} ticks (abs={abs_now:.2f}) | entry_bar≈{entry_bar_now:.2f} | P(revert)={p_now:.3f}")
        if (abs_now >= entry_bar_now) and (p_now >= prob_thr):
            sig = {
                "stable": st, "fee": signal_fee, "side": side,
                "abs_dev_ticks": round(abs_now,2),
                "entry_ticks_quantized": q_ticks,
                "deviation_pct_est": round(pct,4),
                "prob_revert": round(p_now,3)
            }
            print(f"    → SEÑAL: {sig}")
            signals.append(sig)
        else:
            reason = []
            if abs_now < entry_bar_now: reason.append("abs<entry_bar")
            if p_now < prob_thr:       reason.append("p<thr")
            print(f"    · No dispara señal ({' & '.join(reason) if reason else 'condiciones no cumplidas'})")

    return results, signals

# ---------------- main ----------------
def main():
    args = parse_args()
    args.freq = str(args.freq).lower()
    spacing = TICK_SPACING[args.signal_fee]

    hr("CONFIG")
    print(f"data={args.data} | freq={args.freq} | horizon={args.horizon}s")
    print(f"signal_fee={args.signal_fee} (spacing={spacing})")
    print(f"entry: ticks={args.entry_ticks} mult={args.entry_mult} q={args.entry_q}")
    print(f"target: ticks={args.target_ticks} mult={args.target_mult} frac={args.target_frac}")
    print(f"prob_thr={args.prob_thr} | tmin={args.tmin} | tmax={args.tmax} | clip_pct={args.clip_pct}")

    files = find_files(args.data)

    fee_rows = []
    for f in files:
        try:
            raw = read_csv_ticks(f)
            rs  = resample_by_fee(raw, args.freq)
            fee_rows.append(rs)
        except Exception as e:
            print(f"[ERROR] {os.path.basename(f)}: {e}")
    if not fee_rows:
        print("[FATAL] No se pudo leer ningún archivo válido."); sys.exit(1)

    rows = pd.concat(fee_rows, ignore_index=True)
    hr("ROWS RESAMPLED")
    print(f"Filas={len(rows)} | Mem={mem(rows)}")
    print(rows.head(5).to_string(index=False))

    series = median_across_fees(rows, args.freq, args.tmin, args.tmax)

    # Diagnóstico: los desvíos sanos deben ser chicos (p99 << 200 ticks)
    tmp = series.copy()
    tmp["consensus_tick"] = tmp[["DAI","USDC","USDT"]].median(axis=1)
    for st in STABLES:
        dev = (tmp[st] - tmp["consensus_tick"]).abs()
        p50, p90, p99 = np.nanpercentile(dev, [50, 90, 99])
        print(f"[DIAG] |dev| ticks {st}: p50={p50:.2f}  p90={p90:.2f}  p99={p99:.2f}")

    freq_secs = int(pd.to_timedelta(args.freq).total_seconds())
    df = build_features_and_labels(
        series, freq_secs, args.horizon, spacing,
        args.entry_ticks, args.target_ticks,
        args.entry_mult, args.target_mult, args.entry_q, args.target_frac,
        args.clip_pct
    )

    results, signals = train_and_signal(df, args.prob_thr, args.signal_fee, args.report)

    hr("MÉTRICAS")
    if not results: print("Sin resultados (datos insuficientes).")
    else:
        for st, r in results.items():
            print(f"{st}: acc={r['accuracy']:.3f} | test_n={r['test_n']} | positives={r['positives_test']}")

    hr("SEÑALES AHORA")
    if not signals: print("No hay señales con los umbrales actuales.")
    else:
        for s in signals:
            print(f"[{s['stable']}/{s['fee']}] {s['side']} | desv≈{s['abs_dev_ticks']} ticks → "
                  f"entry≈{s['entry_ticks_quantized']} ticks (~{s['deviation_pct_est']}%) "
                  f"| P(revert)={s['prob_revert']}")

    out = os.path.join(args.data, "signals_now.csv")
    try: pd.DataFrame(signals).to_csv(out, index=False); print(f"\nSeñales exportadas a: {out}")
    except Exception as e: print(f"[WARN] No se pudo guardar señales: {e}")

    hr("FIN")

if __name__ == "__main__":
    main()
