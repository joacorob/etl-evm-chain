#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xtrade — "xGoals para trading": P(long/short rentable | foto del mercado)
=========================================================================

Idea (pedido del usuario): igual que el xG del futbol asigna a cada tiro una
probabilidad de gol a partir de una FOTO de la jugada, acá tomamos una foto del
estado del mercado en el instante t (las 23 features ex-ante que ya teniamos) y
entrenamos un CLASIFICADOR CALIBRADO que estima P(que un long sea rentable en el
horizonte H).  short = el complemento.

Etiquetado = TRIPLE BARRIER (Lopez de Prado): desde px[i], en las proximas H horas,
  - toca primero la barrera de arriba (+b)  -> label 1 (gano el long)
  - toca primero la de abajo      (-b)      -> label 0 (gano el short)
  - se vence el tiempo                      -> label por signo del retorno final
b se escala con la vol reciente (b = mult * sigma_H) para que el base-rate ~50%.

LA PREGUNTA HONESTA: el mercado es casi-martingala, asi que P(long gane) ~ 50% por
construccion. Solo hay "xTrade" si la CALIBRACION OOS muestra que cuando el modelo
dice 60% efectivamente gana ~60% (regimenes: momentum / mean-rev / vol). Si pega
siempre al base-rate, no hay edge. Eso es lo que medimos (AUC, Brier, calibracion,
y valor economico OOS).

TimesFM: se usa como UNA FEATURE (P(up) implicita de su forecast de cuantiles), para
medir si aporta señal incremental sobre las features clasicas. Se cachea aparte.

Pasos:
  python3 xtrade.py build              # dataset etiquetado triple-barrier  -> data/xtrade_dataset.csv
  python3 xtrade.py tfm                # feature TimesFM (lento)             -> data/xtrade_tfm.csv
  python3 xtrade.py train              # entrena+evalua walk-forward (con TimesFM si existe el cache)
"""
import os, csv, math, argparse, sys
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from build_dataset import merge_eth_btc, rv, parkinson   # reuso de features
from vol_forecast import ewma_sigma

DATADIR = os.path.abspath(os.path.join(HERE, "..", "..", "..", "data"))
DS = os.path.join(DATADIR, "xtrade_dataset.csv")
TFM = os.path.join(DATADIR, "xtrade_tfm.csv")
PERP = os.path.join(DATADIR, "eth_perp_hourly.csv")
FUND = os.path.join(DATADIR, "eth_funding.csv")

# columnas que NO son features de entrada
NON_FEAT = {"ts", "price", "y_long", "r_real", "exit_h", "timeout"}


# --------------------------------------------------------------------------- #
#  FEATURES (mismas que build_dataset, ex-ante) + LABEL triple-barrier
# --------------------------------------------------------------------------- #
def feature_row(ts, px, hi, lo, vol, bpx, ret, bret, i):
    f = {}
    f["ts"] = int(ts[i]); f["price"] = float(px[i])
    for lb in (24, 72, 168, 336):
        f[f"rv_{lb}"] = rv(ret, i, lb)
    f["ewma"] = ewma_sigma(ret, i)
    f["park_168"] = parkinson(hi, lo, i, 168)
    f["park_72"] = parkinson(hi, lo, i, 72)
    rvs = [rv(ret, j, 24) for j in range(max(1, i - 168), i)]
    rvs = [x for x in rvs if not math.isnan(x)]
    f["volofvol"] = float(np.std(rvs)) if len(rvs) > 3 else float("nan")
    for lb in (24, 72, 168):
        f[f"mom_{lb}"] = float(np.log(px[i] / px[i - lb])) if i - lb >= 0 else float("nan")
    f["abs_mom_168"] = abs(f["mom_168"])
    f["trend_snr"] = f["mom_168"] / (rv(ret, i, 168) * math.sqrt(168) + 1e-9)
    seg = px[max(0, i - 168):i + 1]
    f["drawdown_168"] = float(px[i] / np.max(seg) - 1.0)
    f["range_168"] = float((np.max(seg) - np.min(seg)) / np.mean(seg))
    rseg = ret[max(0, i - 168):i]
    f["acf1"] = float(np.corrcoef(rseg[:-1], rseg[1:])[0, 1]) if (len(rseg) > 10 and np.std(rseg) > 0) else 0.0
    vseg = vol[max(0, i - 168):i]
    vmean_long = np.mean(vol[max(0, i - 720):i]) + 1e-9
    f["vol_z"] = float((np.mean(vseg) - vmean_long) / (np.std(vol[max(0, i - 720):i]) + 1e-9))
    f["vol_trend"] = float(np.mean(vol[max(0, i - 72):i]) / vmean_long)
    f["btc_rv_168"] = rv(bret, i, 168)
    a, b = ret[max(0, i - 168):i], bret[max(0, i - 168):i]
    m = min(len(a), len(b))
    f["eth_btc_corr_168"] = float(np.corrcoef(a[-m:], b[-m:])[0, 1]) if m > 10 else float("nan")
    f["btc_mom_168"] = float(np.log(bpx[i] / bpx[i - 168])) if i - 168 >= 0 else float("nan")
    import time as _t
    tm = _t.gmtime(ts[i])
    f["hour"] = tm.tm_hour; f["dow"] = tm.tm_wday
    return f


# --------------------------------------------------------------------------- #
#  FEATURES DE FLUJO / POSICIONAMIENTO (la "geometria causal" de la foto)
# --------------------------------------------------------------------------- #
def load_flow(ts):
    """Devuelve arrays alineados al grid `ts`: taker_buy, vol_perp, trades, perp_px,
    funding (forward-fill horario). NaN donde falte."""
    n = len(ts)
    tb = np.full(n, np.nan); vp = np.full(n, np.nan)
    trd = np.full(n, np.nan); ppx = np.full(n, np.nan); fr = np.full(n, np.nan)
    if not os.path.exists(PERP):
        return tb, vp, trd, ppx, fr
    perp = {}
    with open(PERP) as f:
        for r in csv.DictReader(f):
            perp[int(r["ts"])] = (float(r["taker_buy"]), float(r["volume"]),
                                  float(r["trades"]), float(r["price"]))
    funding = []
    if os.path.exists(FUND):
        with open(FUND) as f:
            for r in csv.DictReader(f):
                funding.append((int(r["ts"]), float(r["funding_rate"])))
    funding.sort()
    fts = np.array([x[0] for x in funding]); fval = np.array([x[1] for x in funding])
    ts_to_idx = {int(t): k for k, t in enumerate(ts)}
    for t, k in ts_to_idx.items():
        if t in perp:
            a, b, c, d = perp[t]; tb[k] = a; vp[k] = b; trd[k] = c; ppx[k] = d
        if len(fts):
            j = int(np.searchsorted(fts, t, side="right")) - 1   # ultimo funding <= t (sin look-ahead)
            if j >= 0:
                fr[k] = fval[j]
    return tb, vp, trd, ppx, fr


def flow_row(i, tb, vp, trd, ppx, spot, fr):
    """Features direccionales: order-flow imbalance, funding, basis. Solo info <= i."""
    f = {}

    def ofi(lb):
        sb, sv = np.nansum(tb[max(0, i - lb):i]), np.nansum(vp[max(0, i - lb):i])
        return float(2.0 * sb / sv - 1.0) if sv > 0 else float("nan")   # +1 todo buy, -1 todo sell

    f["ofi_24"] = ofi(24); f["ofi_72"] = ofi(72); f["ofi_168"] = ofi(168)
    base = ofi(720)
    f["ofi_z"] = float(f["ofi_24"] - base) if (not math.isnan(f["ofi_24"]) and not math.isnan(base)) else float("nan")
    # tamano medio de trade (proxy de informado vs ruido) y su tendencia
    ats = vp / np.clip(trd, 1, None)
    a_now = np.nanmean(ats[max(0, i - 72):i]); a_long = np.nanmean(ats[max(0, i - 720):i])
    f["trade_size_z"] = float(a_now / a_long - 1.0) if a_long and not math.isnan(a_long) else float("nan")
    # funding: nivel, media de regimen, extremo (crowding), momentum
    f_now = fr[i - 1] if i >= 1 else float("nan")
    f["funding_now"] = float(f_now)
    fm = np.nanmean(fr[max(0, i - 168):i])
    f["funding_mean_168"] = float(fm)
    fstd = np.nanstd(fr[max(0, i - 720):i]) + 1e-9
    f["funding_z"] = float((f_now - np.nanmean(fr[max(0, i - 720):i])) / fstd) if not math.isnan(f_now) else float("nan")
    f["funding_mom"] = float(fm - np.nanmean(fr[max(0, i - 336):max(0, i - 168)])) if i > 336 else float("nan")
    # perp-spot basis (contango/backwardation)
    if not math.isnan(ppx[i]) and spot[i] > 0:
        f["basis"] = float(ppx[i] / spot[i] - 1.0)
        bseg = ppx[max(0, i - 72):i] / np.clip(spot[max(0, i - 72):i], 1e-9, None) - 1.0
        f["basis_mean_72"] = float(np.nanmean(bseg))
    else:
        f["basis"] = float("nan"); f["basis_mean_72"] = float("nan")
    return f


def triple_barrier(px, i, H, mult, sigma_h):
    """Devuelve (label_long, retorno_real_signed, horas_hasta_salida, timeout?)."""
    p0 = px[i]
    b = mult * sigma_h                      # ancho de barrera en log
    up, dn = p0 * math.exp(b), p0 * math.exp(-b)
    end = min(i + H, len(px) - 1)
    for j in range(i + 1, end + 1):
        if px[j] >= up:
            return 1, float(px[j] / p0 - 1.0), j - i, 0
        if px[j] <= dn:
            return 0, float(px[j] / p0 - 1.0), j - i, 0
    r = float(px[end] / p0 - 1.0)
    return (1 if r > 0 else 0), r, end - i, 1


def build(H=168, step=8, mult=1.0, start_frac=0.06):
    ts, px, hi, lo, vol, bpx = merge_eth_btc()
    ret, bret = np.diff(np.log(px)), np.diff(np.log(bpx))
    tb, vp, trd, ppx, fr = load_flow(ts)
    has_flow = not np.all(np.isnan(tb))
    n = len(px)
    idx = list(range(int(n * start_frac), n - H, step))
    rows = []
    for i in idx:
        f = feature_row(ts, px, hi, lo, vol, bpx, ret, bret, i)
        if has_flow:
            f.update(flow_row(i, tb, vp, trd, ppx, px, fr))
        sigma_h = rv(ret, i, 168) * math.sqrt(H)        # vol esperada del horizonte
        if math.isnan(sigma_h) or sigma_h <= 0:
            continue
        lab, r_real, exit_h, to = triple_barrier(px, i, H, mult, sigma_h)
        f["y_long"] = lab; f["r_real"] = r_real; f["exit_h"] = exit_h; f["timeout"] = to
        rows.append(f)
    cols = list(rows[0].keys())
    with open(DS, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols); w.writeheader()
        for r in rows:
            w.writerow(r)
    span = (rows[-1]["ts"] - rows[0]["ts"]) / 86400
    br = np.mean([r["y_long"] for r in rows])
    to = np.mean([r["timeout"] for r in rows])
    print(f"escrito {DS}: {len(rows)} fotos | span {span:.0f}d | base-rate long={br*100:.1f}% "
          f"| timeouts={to*100:.0f}% | barrera=±{mult:.1f}σ_H, H={H}h, step={step}h")


# --------------------------------------------------------------------------- #
#  FEATURE TimesFM:  P(up) implicita del forecast de cuantiles
# --------------------------------------------------------------------------- #
def build_tfm(H=168, ctx=512):
    import timesfm
    eth = os.path.join(DATADIR, "eth_hourly.csv")
    ts, px = [], []
    with open(eth) as f:
        for r in csv.DictReader(f):
            ts.append(int(r["ts"])); px.append(float(r["price"]))
    ts, px = np.array(ts), np.array(px)
    ts_to_i = {int(t): k for k, t in enumerate(ts)}
    logpx = np.log(px)
    want = []
    with open(DS) as f:
        for r in csv.DictReader(f):
            want.append(int(float(r["ts"])))

    m = timesfm.TimesFM_2p5_200M_torch.from_pretrained(timesfm.TimesFM_2p5_200M_torch.DEFAULT_REPO_ID)
    m.compile(timesfm.ForecastConfig(max_context=ctx, max_horizon=H, normalize_inputs=True,
                                     use_continuous_quantile_head=True, fix_quantile_crossing=True))
    # niveles de cuantil que devuelve TimesFM 2.5 (mean + deciles 0.1..0.9)
    qlev = np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])

    rows, bts, bin_, b0 = [], [], [], []

    def flush():
        if not bin_:
            return
        _, q = m.forecast(horizon=H, inputs=bin_)        # q: (B,H,n_q)
        for k in range(len(bin_)):
            qk = np.asarray(q[k])[-1]                     # cuantiles al final del horizonte
            qv = qk[-9:] if qk.shape[-1] >= 9 else qk     # log-precio
            cur = b0[k]
            # P(up) = 1 - CDF(cur): fraccion (interpolada) de masa por encima del precio actual
            if cur <= qv[0]:
                p_up = 1.0
            elif cur >= qv[-1]:
                p_up = 0.0
            else:
                j = int(np.searchsorted(qv, cur))
                lo_q, hi_q = qv[j - 1], qv[j]
                frac = (cur - lo_q) / (hi_q - lo_q + 1e-12)
                cdf = qlev[j - 1] + frac * (qlev[j] - qlev[j - 1])
                p_up = 1.0 - float(cdf)
            med_ret = float(qv[4] - cur)                  # retorno mediano forecast (log)
            band = float(qv[-1] - qv[0])                  # dispersion (q90-q10)
            rows.append((bts[k], p_up, med_ret, band))
        bts.clear(); bin_.clear(); b0.clear()

    for wts in want:
        i = ts_to_i.get(wts)
        if i is None or i < ctx:
            continue
        bts.append(wts); bin_.append(logpx[i - ctx:i].astype(np.float32)); b0.append(float(logpx[i]))
        if len(bin_) >= 32:
            flush(); print(f"  {len(rows)}/{len(want)}…", end="\r")
    flush()
    rows.sort()
    with open(TFM, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["ts", "tfm_pup", "tfm_medret", "tfm_band"]); w.writerows(rows)
    print(f"\nescrito {TFM}: {len(rows)} fotos")


# --------------------------------------------------------------------------- #
#  TRAIN + EVAL walk-forward
# --------------------------------------------------------------------------- #
def load_train():
    rows = []
    with open(DS) as f:
        for r in csv.DictReader(f):
            rows.append({k: float(v) for k, v in r.items()})
    tfm = {}
    if os.path.exists(TFM):
        with open(TFM) as f:
            for r in csv.DictReader(f):
                tfm[int(float(r["ts"]))] = (float(r["tfm_pup"]), float(r["tfm_medret"]), float(r["tfm_band"]))
    return rows, tfm


def _metrics(name, p, y, r_real):
    """imprime AUC, Brier, y valor economico OOS de seguir la señal."""
    from sklearn.metrics import roc_auc_score, brier_score_loss
    auc = roc_auc_score(y, p) if len(set(y)) > 1 else float("nan")
    brier = brier_score_loss(y, p)
    base = y.mean()
    brier_base = base * (1 - base)                       # Brier de predecir siempre el base-rate
    # economico: long si p>0.5, short si p<0.5; pnl = signo * retorno real
    sig = np.where(p > 0.5, 1.0, -1.0)
    pnl = (sig * r_real).mean()
    hit = ((sig > 0) == (r_real > 0)).mean()
    print(f"  {name:22}AUC={auc:.3f}  Brier={brier:.4f} (base {brier_base:.4f})  "
          f"pnl/trade={pnl*100:+.3f}%  hit={hit*100:.1f}%")
    return auc, brier, pnl


def calibration_table(p, y, bins=10):
    print("\n  calibracion OOS (¿P dicha ≈ frecuencia real?):")
    print(f"    {'P pred':>10}{'n':>7}{'real':>9}")
    edges = np.linspace(0, 1, bins + 1)
    for a, b in zip(edges[:-1], edges[1:]):
        msk = (p >= a) & (p < b) if b < 1 else (p >= a) & (p <= b)
        if msk.sum() >= 5:
            print(f"    {(a+b)/2:>10.2f}{int(msk.sum()):>7}{y[msk].mean():>9.2f}")


def train():
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.preprocessing import StandardScaler

    rows, tfm = load_train()
    has_tfm = len(tfm) > 0
    FLOW = {"ofi_24", "ofi_72", "ofi_168", "ofi_z", "trade_size_z", "funding_now",
            "funding_mean_168", "funding_z", "funding_mom", "basis", "basis_mean_72"}
    all_cols = [c for c in rows[0] if c not in NON_FEAT]
    classic_cols = [c for c in all_cols if c not in FLOW]
    flow_cols = [c for c in all_cols if c in FLOW]
    base_cols = all_cols

    def matrix(cols, with_tfm):
        X, Y, R, T = [], [], [], []
        for r in rows:
            t = int(r["ts"])
            if with_tfm and t not in tfm:
                continue
            row = [r[c] for c in cols]
            if with_tfm:
                row += list(tfm[t])
            if any(math.isnan(v) for v in row):
                continue
            X.append(row); Y.append(int(r["y_long"])); R.append(r["r_real"]); T.append(t)
        return np.array(X), np.array(Y), np.array(R), np.array(T)

    def run(label, cols, with_tfm):
        X, Y, R, T = matrix(cols, with_tfm)
        n = len(X); cut = int(n * 0.60)
        sc = StandardScaler().fit(X[:cut])
        Xtr, Xte = sc.transform(X[:cut]), sc.transform(X[cut:])
        clf = GradientBoostingClassifier(n_estimators=150, max_depth=2, learning_rate=0.03,
                                         subsample=0.8, random_state=0)
        # calibracion con CV interno solo en train (sin tocar el test futuro)
        cal = CalibratedClassifierCV(clf, method="isotonic", cv=3)
        cal.fit(Xtr, Y[:cut])
        p = cal.predict_proba(Xte)[:, 1]
        print(f"\n[{label}]  n={n}  test OOS={n-cut}  features={X.shape[1]}")
        _metrics(label, p, Y[cut:], R[cut:])
        return p, Y[cut:], R[cut:]

    print("=" * 78)
    print(f"xTrade — clasificador calibrado de P(long rentable)  | TimesFM cache: {has_tfm}")
    print("=" * 78)
    # base-rate economico de referencia
    _, _, R_all, _ = matrix(base_cols, False)
    print(f"referencia: always-long pnl/trade = {R_all[int(len(R_all)*0.6):].mean()*100:+.3f}%  "
          f"(si esto es ~0, el mercado es martingala)")

    run("clasicas (vol/mom)", classic_cols, False)
    pf = yf = rf = None
    if flow_cols:
        pf, yf, rf = run("FLUJO solo (ofi/funding/basis)", flow_cols, False)
    run("clasicas+flujo", base_cols, False)
    if has_tfm:
        run("TimesFM sola", [], True)
        run("todo+TimesFM", base_cols, True)

    if pf is not None:
        # significancia del AUC del modelo de FLUJO (winner) por bootstrap
        from sklearn.metrics import roc_auc_score
        rng = np.random.RandomState(0)
        aucs = []
        for _ in range(1000):
            idx = rng.randint(0, len(yf), len(yf))
            if len(set(yf[idx])) > 1:
                aucs.append(roc_auc_score(yf[idx], pf[idx]))
        aucs = np.array(aucs)
        p_gt = float((aucs > 0.5).mean())
        print(f"\n[FLUJO] AUC bootstrap: media={aucs.mean():.3f}  "
              f"IC95=[{np.percentile(aucs,2.5):.3f}, {np.percentile(aucs,97.5):.3f}]  "
              f"P(AUC>0.5)={p_gt:.2f}")
        # robustez: pnl/trade del modelo de flujo por mitades del test OOS
        h = len(rf) // 2
        sigf = np.where(pf > 0.5, 1.0, -1.0)
        pnl1 = (sigf[:h] * rf[:h]).mean() * 100
        pnl2 = (sigf[h:] * rf[h:]).mean() * 100
        print(f"[FLUJO] pnl/trade 1a mitad OOS={pnl1:+.3f}%  2a mitad={pnl2:+.3f}%  "
              f"(¿estable o se rompe?)")
        print("\ncalibracion OOS del modelo de FLUJO (el ganador):")
        calibration_table(pf, yf)
    print("\nLECTURA: AUC>0.5 con IC95 que no cruza 0.5 y pnl estable en ambas mitades")
    print("=> señal direccional real (modesta). Si el IC cruza 0.5, es ruido afortunado.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "tfm", "train"])
    ap.add_argument("--H", type=int, default=168)
    ap.add_argument("--step", type=int, default=8)
    ap.add_argument("--mult", type=float, default=1.0)
    ap.add_argument("--ctx", type=int, default=512)
    args = ap.parse_args()
    if args.cmd == "build":
        build(H=args.H, step=args.step, mult=args.mult)
    elif args.cmd == "tfm":
        build_tfm(H=args.H, ctx=args.ctx)
    elif args.cmd == "train":
        train()
