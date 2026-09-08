#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
vol_forecast — forecaster de DISPERSION para decidir ancho de rango v3
======================================================================

Tesis (ver REPORT_VOL.md): una posicion concentrada en Uni v3 es short-gamma /
short-vol. No hay que predecir la DIRECCION del precio (imposible, ~random walk),
hay que predecir la DISPERSION/volatilidad sobre el horizonte de holding. La vol
SI es predecible (clustering / efecto GARCH).

Este modulo:
  - carga precio horario (data/eth_hourly.csv)
  - calcula log-returns y realized vol
  - 3 forecasters de sigma sobre las proximas H horas:
        naive   : sigma_next = sigma_pasado (random walk en vol)
        ewma    : RiskMetrics EWMA (lambda)
        garch   : GARCH(1,1) MLE (scipy), forecast multi-step
  - evalua skill out-of-sample (QLIKE / MSE / correlacion vs realized)

La salida clave que consume el backtest: forecast de sigma_window (desvio del
log-precio acumulado sobre H horas) en cada punto de rebalanceo.
"""
import os, csv, math
import numpy as np
from scipy.optimize import minimize

HOURLY = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "eth_hourly.csv"))


# ----------------------------- data -----------------------------------------
def load_hourly(path=HOURLY):
    ts, px = [], []
    with open(path) as f:
        for r in csv.DictReader(f):
            ts.append(int(r["ts"])); px.append(float(r["price"]))
    ts, px = np.array(ts), np.array(px)
    ret = np.diff(np.log(px))           # log-returns horarios
    return ts, px, ret


# --------------------------- forecasters ------------------------------------
def realized_sigma(ret, i, lookback):
    """sigma horaria realizada en las `lookback` horas previas a i."""
    w = ret[max(0, i - lookback):i]
    return float(np.std(w)) if len(w) > 2 else float("nan")


def ewma_sigma(ret, i, lam=0.94):
    """sigma horaria EWMA (RiskMetrics) hasta i-1, recursiva."""
    var = np.var(ret[:min(i, 50)]) if i > 5 else np.var(ret[:50])
    for t in range(max(1, i - 500), i):
        var = lam * var + (1 - lam) * ret[t - 1] ** 2
    return math.sqrt(var)


def garch11_fit(ret_window):
    """MLE de GARCH(1,1): sigma2_t = omega + alpha*r_{t-1}^2 + beta*sigma2_{t-1}.
    Devuelve (omega, alpha, beta, sigma2_last)."""
    r = ret_window - np.mean(ret_window)
    var0 = np.var(r)

    def nll(params):
        omega, alpha, beta = params
        if omega <= 0 or alpha < 0 or beta < 0 or alpha + beta >= 0.999:
            return 1e10
        s2 = var0
        ll = 0.0
        for t in range(1, len(r)):
            s2 = omega + alpha * r[t - 1] ** 2 + beta * s2
            if s2 <= 0:
                return 1e10
            ll += math.log(s2) + r[t] ** 2 / s2
        return 0.5 * ll

    x0 = [var0 * 0.05, 0.08, 0.90]
    res = minimize(nll, x0, method="Nelder-Mead",
                   options={"maxiter": 800, "xatol": 1e-12, "fatol": 1e-9})
    omega, alpha, beta = res.x
    # recomputar sigma2_last
    s2 = var0
    for t in range(1, len(r)):
        s2 = omega + alpha * r[t - 1] ** 2 + beta * s2
    return omega, alpha, beta, s2


def garch_forecast_window(omega, alpha, beta, s2_last, H):
    """Varianza ACUMULADA esperada sobre H pasos -> sigma del log-precio en H horas.
    E[sigma2_{t+k}] converge a la incondicional uncond = omega/(1-alpha-beta)."""
    persist = alpha + beta
    uncond = omega / (1 - persist) if persist < 1 else s2_last
    total = 0.0
    s2k = s2_last
    for k in range(1, H + 1):
        s2k = uncond + (persist ** (k - 1)) * (s2_last - uncond)  # E[sigma2_{t+k}]
        total += s2k
    return math.sqrt(total)   # sigma del retorno acumulado a H horas


# ------------------- sigma de la ventana (lo que usa el backtest) -----------
def window_sigma_forecasts(ret, i, H, garch_lookback=720):
    """Devuelve dict con el forecast de sigma del log-precio acumulado sobre las
    proximas H horas, segun cada modelo, usando solo info hasta i (sin look-ahead)."""
    out = {}
    # naive: sigma_window = sigma_horaria_pasada * sqrt(H)  (random walk en vol)
    sig_h = realized_sigma(ret, i, lookback=H)
    out["naive"] = sig_h * math.sqrt(H)
    # ewma
    sig_e = ewma_sigma(ret, i)
    out["ewma"] = sig_e * math.sqrt(H)
    # garch
    lo = max(0, i - garch_lookback)
    if i - lo > 100:
        try:
            o, a, b, s2 = garch11_fit(ret[lo:i])
            out["garch"] = garch_forecast_window(o, a, b, s2, H)
        except Exception:
            out["garch"] = out["ewma"]
    else:
        out["garch"] = out["ewma"]
    return out


def realized_window_sigma(ret, i, H):
    """sigma REALIZADA del log-precio acumulado en las H horas DESPUES de i (target)."""
    w = ret[i:i + H]
    if len(w) < H // 2:
        return float("nan")
    # desvio del retorno acumulado ~ sqrt(sum r^2) bajo media ~0
    return math.sqrt(np.sum(w ** 2))


# ------------------------------ evaluacion ----------------------------------
def evaluate(H=168, step=24, start_frac=0.3):
    """Walk-forward: en cada punto de rebalanceo compara forecast vs realized."""
    ts, px, ret = load_hourly()
    n = len(ret)
    i0 = int(n * start_frac)
    pts = list(range(i0, n - H, step))
    models = ["naive", "ewma", "garch"]
    preds = {m: [] for m in models}
    real = []
    for i in pts:
        rz = realized_window_sigma(ret, i, H)
        if math.isnan(rz):
            continue
        fc = window_sigma_forecasts(ret, i, H)
        if any(math.isnan(fc[m]) for m in models):
            continue
        real.append(rz)
        for m in models:
            preds[m].append(fc[m])
    real = np.array(real)
    print(f"=== Skill de forecast de sigma_window (H={H}h, {len(real)} ventanas) ===")
    print(f"{'modelo':8}{'MSE':>12}{'QLIKE':>10}{'corr':>8}{'bias':>9}")
    res = {}
    for m in models:
        p = np.array(preds[m])
        mse = np.mean((p - real) ** 2)
        # QLIKE sobre varianza
        v_p, v_r = p ** 2, real ** 2
        qlike = np.mean(np.log(v_p) + v_r / v_p)
        corr = np.corrcoef(p, real)[0, 1]
        bias = np.mean(p - real) / np.mean(real)
        res[m] = dict(mse=mse, qlike=qlike, corr=corr, bias=bias)
        print(f"{m:8}{mse:>12.2e}{qlike:>10.3f}{corr:>8.3f}{bias:>+8.1%}")
    best = min(res, key=lambda m: res[m]["qlike"])
    print(f"\nmejor por QLIKE: {best}  (corr={res[best]['corr']:.3f})")
    print("corr > 0 => la vol ES predecible; mejor que naive => el modelo aporta.")
    return res


if __name__ == "__main__":
    evaluate()
