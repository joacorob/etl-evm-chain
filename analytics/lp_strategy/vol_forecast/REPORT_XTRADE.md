# xTrade — "xGoals para trading": P(long/short rentable | foto del mercado)

**Idea (usuario):** igual que el xG del fútbol asigna a cada tiro una probabilidad de
gol desde una *foto* de la jugada, tomar una foto del estado del mercado y estimar
**P(long rentable)** en el horizonte H. short = complemento.

**Método (López de Prado):** etiquetado **triple-barrier** (toca +b / −b / timeout→signo,
con b = 1·σ_H escalada por vol reciente) + **clasificador calibrado** (GradientBoosting +
`CalibratedClassifierCV` isotónico) walk-forward (train 60% / test 40%, sin look-ahead).
Mismas 23 features ex-ante de `build_dataset.py`. TimesFM entra como **feature**
(P(up) implícita de su forecast de cuantiles). Archivo: `xtrade.py`.

## Resultado (ETH 1h, 18 meses, 1507 fotos, OOS=603, barrera ±1σ_H, H=168h)

| modelo | AUC ↑ | Brier ↓ (base 0.247) | pnl/trade | hit |
|---|---|---|---|---|
| always-long (ref) | — | — | −0.466% | — |
| features (23) | 0.523 | 0.257 | −0.306% | 47.3% |
| **TimesFM sola** | **0.447** | 0.255 | −0.545% | 46.8% |
| features+TimesFM | 0.518 | 0.266 | +0.043% | 50.9% |

**Calibración OOS no monótona** (features): P=0.55→0.37 real, P=0.65→0.52, P=0.75→0.45.
Invertida/ruidosa → cuando dice 60% que sube, no sube 60%.

## Conclusión

- **AUC ≈ 0.52 = moneda. No hay xTrade direccional.** base-rate long 46.8% (período
  bajista ETH −38%); ningún modelo lo timea OOS.
- **Por qué la analogía con xG se rompe:** el fútbol NO es adversarial (la conversión de
  un tiro es estable, nadie la arbitra → xG calibra). La dirección del precio SÍ es
  eficiente/adversarial: si existiera P(long)=60% estable, ya la habrían arbitrado a 50%.
- **TimesFM no aporta:** sola AUC 0.447 (anti-señal), combinada solo lleva el pnl a ~0
  (+0.043%, dentro del ruido) sin arreglar la calibración. Mismo **techo de información**
  que el experimento de vol (`REPORT_VOL.md`): el modelo grande no inventa señal donde
  no la hay.
- **Esto es el método funcionando**, no fallando: reporta "no edge" en vez de alucinar
  uno (overfit in-sample). Consistente con que la parte predecible no discrimina la
  decisión.

## v2 — features de FLUJO/POSICIONAMIENTO (la crítica del usuario era correcta)

Las 23 features eran de *dispersión* (rv/parkinson/momentum): miden CUÁNTO se mueve, no
PARA DÓNDE. Se agregaron variables direccionales reales (`fetch_flow.py` baja perp klines
+ funding de Binance Futures, 540d): **order-flow imbalance** (taker buy/sell, OFI 24/72/168h),
**funding rate** (nivel/regimen/z/momentum), **perp-spot basis**, tamaño medio de trade.

| modelo | AUC | pnl/trade | hit |
|---|---|---|---|
| always-long (ref) | — | −0.466% | — |
| clásicas (vol/mom) 23f | 0.523 | −0.306% | 47.3% |
| **FLUJO solo 11f** | **0.533** | **+0.610%** | **52.6%** |
| clásicas+flujo 34f | 0.530 | −0.105% | 50.1% |
| TimesFM sola | 0.447 | −0.545% | 46.8% |

**Hallazgo 1:** el flujo es la familia correcta — única con pnl>0 y hit>50%. Vol/TimesFM dan
cero. **Hallazgo 2:** mezclar clásicas+flujo EMPEORA (las 23 de vol son ruido direccional
que diluye). Menos features, mejor.

**PERO los tests de rigor lo bajan a "no robusto todavía":**
- AUC bootstrap IC95 = **[0.486, 0.576]** → cruza 0.5 (P(AUC>0.5)=0.91, no significativo al 95%).
- pnl/trade por mitades OOS: 1ª = **−0.715%**, 2ª = **+1.930%** → todo el edge en un sub-período.
- calibración no monótona (P=0.85 → 0.48 real) → no son probabilidades reales, son apuestas
  que cayeron justas en el tramo bueno.

**Veredicto v2:** flujo > vol conceptual y empíricamente, pero el "+0.61%" es un **régimen
afortunado**, no señal estable. La pieza con mecanismo económico real es el **funding**
(longs crowded pagan → revierte); próximo paso = aislarla y analizar por régimen, no tirar
11 features juntas al GBM. La disciplina (IC + estabilidad + calibración) es lo que separa
xTrade real de espejismo de backtest.

## v3 — los dos finalistas con la misma vara (funding vs in-range)

`funding_regime.py` (A) y `xinrange.py` (B), evaluados con IC95 bootstrap + estabilidad
por mitades OOS + calibración:

| métrica | A: funding aislado | B: P(in-range) |
|---|---|---|
| AUC OOS | ~0.51 (corr funding↔fwd ≈ −0.01) | **0.663** |
| IC95 cruza umbral | **sí** ([−0.15%,+0.86%] pnl) | **no** (AUC [0.617,0.709]) |
| P(señal>0) | 0.91 | **1.00** |
| estable por mitades | no (−0.86% / +1.58%) | **sí** (AUC 0.815 / 0.559, + en ambas) |
| calibración | — | **monótona, Spearman +0.82** |

**Ganador: B (P in-range), sin discusión.** A (funding) repite el patrón del flujo: señal
débil, todo el pnl en un sub-período, IC cruza 0 → régimen afortunado, no edge.

**Lección central:** B gana porque in-range es un target **NO adversarial** (dispersión,
no dirección): la vol tiene memoria y nadie la arbitra, entonces la foto calibra. Todo lo
direccional (precio, funding) es eficiente → el edge aparente nunca sobrevive el IC + la
partición temporal.

**Matiz honesto:** P(in-range) calibrada da la probabilidad de fill/permanencia, NO el PnL
neto. Es el input para elegir ancho con criterio (apuntar a una tasa de in-range objetivo,
sizing), pero por `REPORT_VOL` "tight vs wide" neto-de-fees igual no gana (la parte
predecible de la vol afecta igual a tight y wide). B = control de riesgo/sizing, no alpha
por sí solo.

## v4 — optimizador de ancho conectado a backtest_real (max fees·P(in-range) − LVR)

`xrange_optimizer.py`: conecta el P(in-range|w) calibrado (GBM, AUC OOS **0.816** sobre el
menú ±8/10/15/25/40%) con la mecánica real de `backtest_real` (LP value v3 + short estático
delta-neutral). Fundamentos (búsqueda web): LVR full-range = σ²/8·año, concentrado ·η(w);
capital efficiency η(w)=1/(1−((1−w)/(1+w))^¼); fee_apr(w)=fee_ref·η(w)/η(ref). Backtest
ROLLING con re-center real (rebal+perp bps) — tight paga el churn.

**Resultado OOS (rolling, APR aprox):**

| estrategia | APR | re-centers |
|---|---|---|
| **vol-adaptive (w∝σ reciente)** | **+28%** | 31 |
| fija ±10% | +26% | 39 |
| fija ±15% | +25% | 31 |
| MODELO P(in-range)≥0.6 | +18% | 30 |
| MODELO EV (fees·P−costo) | +13% | 37 |
| fija ±8% | +17% | 55 (churn/trap) |
| fija ±40% | +0% | 29 |

**Veredicto:** el modelo calibrado P(in-range), en sus 3 variantes (EV fees·P−LVR teórico,
EV con costo-de-romper, y tightest-con-P≥umbral), **NO le gana a la heurística simple
vol-adaptive** (~28%), pese a AUC 0.816. El sweet spot fijo es ±10-15%. El "trap" de
`backtest_real` se reproduce (±8% churna 55×, se hunde). **Mismo techo de información de
toda la investigación, ahora sobre el objetivo económico:** predecir in-range es fácil,
convertirlo en mejor elección de ancho que "escalá con σ trailing" no se puede.

**Accionable:** ancho ∝ EWMA(vol realizada) clip ±10-40% + short estático del delta. El
ML/calibración no agrega. La plata real está en fee tier × volumen (acá fee sintético
30%@±40%), no en el forecast. Caveats: fee sintético, sin slippage/competencia, 1 activo,
18 meses → niveles de APR optimistas, vale el ranking relativo.

## Dónde el approach foto→probabilidad SÍ sirve (targets NO adversariales)

1. **P(in-range) de una posición LP** a H — tiene memoria (vol clustering), no se arbitra,
   y es directamente la decisión de ancho. Cambio de 1 línea en el labeling (`y_inrange`).
2. **P(régimen de fees > LVR)** — el motor real de rentabilidad (xyield).
3. **P(cola: liquidación/gap)** — colas, no dirección.

## Cómo correr
```bash
cd analytics/lp_strategy/vol_forecast
python3 xtrade.py build      # dataset triple-barrier -> data/xtrade_dataset.csv
python3 xtrade.py tfm        # feature TimesFM (lento) -> data/xtrade_tfm.csv
python3 xtrade.py train      # entrena + evalúa walk-forward (3 modelos + calibración)
```
