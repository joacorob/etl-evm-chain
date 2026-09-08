# ¿Se puede predecir si conviene rango chico en Uni v3? (TimesFM / vol-forecasting)

**Pregunta original:** usar TimesFM (foundation model de series de Google) para
predecir si conviene poner posiciones v3 en rangos chicos. Goal: ganarle al
impermanent loss.

**TL;DR:** La pregunta correcta no es predecir el *precio* (≈ random walk, imposible)
sino la *dispersión* de la próxima ventana. La dispersión es parcialmente predecible
**en agregado** (corr ≈ 0.42 vol-forecast vs vol realizada), pero la parte que
decide tight-vs-wide en cada ventana **NO es forecasteable** una semana antes — ni
con EWMA, ni con un GBM de 23 features (incl. correlación ETH-BTC), ni con TimesFM.
El techo con foresight perfecto de la vol es alto (corr ≈ -0.53), pero ningún
predictor operable se le acerca out-of-sample. **Veredicto: no le ganás al IL
prediciendo el ancho; el motor real es fees > LVR (hedge / fee tier / régimen).**

---

## La tesis (por qué dispersión y no precio)

Una posición concentrada en Uni v3 es matemáticamente **short-gamma / short-vol**:
cobrás fees (theta) y perdés contra IL/LVR cuando el precio se mueve (gamma). El IL
**no depende de la dirección**, depende de **cuánto se mueve** y de **si sale del
rango**. Por eso:
- predecir dirección de ETH = inútil (y la literatura confirma que los foundation
  models no le ganan al naive en returns financieros).
- lo que importa es la **dispersión esperada** sobre el horizonte de holding.

Rango chico = más fees por dólar (más concentración) pero se sale antes (deja de
cobrar y cristaliza IL). El "edge" teórico = ir **tight en calma, wide en vol alta**.

---

## Data (el pedido: +6 meses)

| fuente | qué | rango | uso |
|---|---|---|---|
| Binance `ETHUSDT` 1h | `data/eth_hourly.csv` | **18 meses** (2024-12 → 2026-06) | serie principal |
| Binance `BTCUSDT` 1h | `data/btc_hourly.csv` | 18 meses | cross-asset (corr ETH-BTC) |
| Waizard RPC (pool 0.3% on-chain) | `data/pool_price_rpc.csv` | **31 días** | cross-check |

**Nota importante sobre Waizard:** la RPC `rpc.waizard.ai` **NO es archive** —
prunea estado *y* logs más allá de **~33 días**. No puede dar +6 meses (confirma lo
anotado en memoria). Por eso la serie larga sale de Binance (precio CEX ≈ precio del
pool por arbitraje: el cross-check de 31d coincide, $2,178→$1,794). Waizard sí se usó
para muestrear el precio real on-chain del pool WETH/USDC 0.3% como validación.

---

## Pipeline (archivos en `analytics/lp_strategy/vol_forecast/`)

1. `fetch_eth_history.py` — baja klines horarias de Binance (`--symbol` para BTC).
2. `fetch_pool_price_rpc.py` — precio real on-chain del pool vía Waizard (sparse hourly).
3. `vol_forecast.py` — forecasters de σ (naive / EWMA / GARCH) + eval de skill.
4. `backtest_adaptive.py` — backtest LP ancho-adaptativo vs fijo + análisis de régimen.
5. `build_dataset.py` — **dataset único mergeado** (23 features + targets) → `data/lp_decision_dataset.csv`.
6. `predict_decision.py` — GBM walk-forward para predecir `tight_adv`.
7. `timesfm_signal.py` — TimesFM 2.5 como estimador de dispersión.

---

## Hallazgos

### 1. La vol es predecible… en agregado (pero el simple gana)
`vol_forecast.py` (H=168h, 373 ventanas):

| modelo | corr vs vol realizada | QLIKE |
|---|---|---|
| naive (RV trailing) | **0.42** | mejor |
| EWMA | 0.42 | ~ |
| GARCH(1,1) | -0.10 | peor (inestable en esta data) |

→ La vol *tiene* memoria (corr 0.42), pero el RV trailing simple alcanza; GARCH no aporta.

### 2. Backtest adaptativo: en tendencia, todo LP pierde vs HODL
`backtest_adaptive.py` (16 meses, ETH **-38%**, 69 ventanas de 7d, fee 8%@±50% ~1/w):

| estrategia | ret | vs HODL | in-range | ancho~ |
|---|---|---|---|---|
| tight fijo (±5%) | -70% | -64% | 70% | 5% |
| wide fijo (±40%) | -41% | -27% | 100% | 40% |
| adaptive (vol) | -66% | -58% | 85% | 9% |
| HODL 50/50 | -19% | — | — | — |

→ En un downtrend sostenido el LP compra ETH mientras cae (termina 100% ETH en el
fondo) → IL direccional > fees. Wide pierde menos. Esto es estructural, no del modelo.

### 3. Análisis de régimen: el techo es alto, el forecast no lo alcanza
Por ventana, net-vs-HODL de tight vs wide, cruzado con la σ:

- `corr(σ_realizada, ventaja_tight) = -0.53` → **con foresight perfecto, la decisión
  es muy aprovechable** (más vol → tight peor, como esperábamos).
- `corr(σ_forecast EWMA, ventaja_tight) = -0.05` → **el forecast simple no captura
  la señal**. La parte de la vol que decide el resultado es la innovación NO predecible.

### 4. Dataset mergeado + GBM: overfit, no generaliza
`build_dataset.py` (503 ventanas, 23 features: vol multi-horizonte, Parkinson high/low,
momentum, vol-of-vol, volumen, **corr ETH-BTC**, vol BTC, estacionalidad) +
`predict_decision.py` (GBM walk-forward, split temporal 60/40):

| señal | corr(·, tight_adv) TEST OOS |
|---|---|
| GBM (train) | +0.85 |
| **GBM (test OOS)** | **+0.08** |
| EWMA | +0.07 |
| oráculo (σ realizada) | +0.55 |

Decisión OOS: GBM -0.32% = always-wide -0.32% → **edge ≈ 0**. Top features:
`rv_336`, `eth_btc_corr_168`, `acf1`, `volofvol`. Tienen señal in-sample, **no OOS**.

### 5. TimesFM (la librería de Google)
`timesfm_signal.py` — TimesFM 2.5 200M, alimentado con log-precio horario, dispersión
predicha = ancho de banda de cuantiles (q90-q10) al horizonte.

503 ventanas, test OOS=202:

| señal | corr(-disp, tight_adv) TEST OOS | corr(disp, σ realizada) |
|---|---|---|
| **TimesFM 2.5** | **+0.10** | +0.29 |
| EWMA (baseline) | +0.07 | +0.42 |
| oráculo (σ realizada) | +0.55 | — |

→ **TimesFM no aporta.** Para decidir el ancho da +0.10 OOS, estadísticamente
indistinguible del EWMA (+0.07) y lejísimos del oráculo (+0.55). Y como predictor de
vol es **peor** que el RV trailing (0.29 vs 0.42). Un modelo de 200M params, alimentado
solo con precio, no le gana al desvío de los últimos 7 días. Coherente con que ni el
GBM supervisado de 23 features generaliza: la vol decisiva a 7d es casi ruido.

### 6. TimesFM forecasteando la SERIE DE VOL (el experimento decisivo)
`timesfm_volseries.py` — en vez de darle el precio, le damos la **serie de RV**
(vol realizada rolling-24h, por hora) y forecasteamos su path a 7d. Es el uso
canónico de un modelo de series, y la vol tiene mucha más memoria que el precio.

**Predecir la vol futura realizada (503 ventanas, test OOS=202):**

| modelo | corr ↑ | QLIKE ↓ |
|---|---|---|
| **TimesFM** | 0.418 | **-3.745** (mejor) |
| EWMA | 0.452 | -3.577 |
| naive | 0.416 | -3.044 |

→ Dándole la serie correcta, **TimesFM SÍ es un forecaster de vol competente**
(empata al EWMA en corr, mejor QLIKE). El approach #5 (darle el precio) lo subutilizaba.

**Pero para decidir tight-vs-wide:**

| modelo | corr(-σ_pred, tight_adv) OOS |
|---|---|
| TimesFM | **+0.003** |
| EWMA | +0.065 |
| naive | +0.075 |
| oráculo (σ realizada) | +0.546 |

→ **El hallazgo clave:** aunque TimesFM predice la vol *agregada* (0.42), eso da
**cero** skill de decisión (+0.003). No es un problema de modelo — es un **techo de
información**: la parte predecible de la vol ya está incorporada en el resultado de
tight *y* de wide por igual (no discrimina); lo que decide cada ventana es el **path
específico** (round-trip vs tendencia, excursiones exactas) = la innovación NO
predecible. El oráculo (-0.53) usa σ realizada, que incluye esa innovación; ningún
forecaster puede acceder a ella. **Fin de la discusión: no es cómo usás TimesFM.**

---

## Sensibilidad al fee real (clave para "ganarle al IL")
net-vs-HODL promedio por ventana según el APR base de fees (±5% vs ±40%, oráculo):

| fee base @±50% | tight | wide | oráculo (best/window) |
|---|---|---|---|
| 5% | -1.82% | -0.55% | -0.32% |
| 8% | -1.42% | -0.48% | -0.04% |
| 15% | -0.48% | -0.31% | **+0.68%** |
| 25% | +0.85% | -0.07% | +1.82% |
| 40% | +2.86% | +0.29% | +3.56% |

→ El LP recién le gana al IL con **fees reales ≥ ~15-25% APR**. Ahí sí importa el
ancho. Por debajo, ningún ancho salva (en este período bajista).

---

## Conclusión / recomendación

1. **No se le gana al IL prediciendo el ancho.** La decisión tight-vs-wide tiene
   techo alto (oráculo -0.53) pero es **no-operable**: la vol decisiva no se
   forecastea a 7d ni con TimesFM ni con 23 features.
2. **TimesFM no aporta** sobre el baseline (ver abajo) — coherente con que ni un GBM
   supervisado generaliza. Es un predictor de nivel, y el nivel/vol de cripto a 7d es
   casi ruido.
3. **El motor real de rentabilidad** no es elegir el ancho, es **fee > LVR**: fee tier
   correcto + volumen + (opcional) hedge delta-neutral. Eso ya lo tenés en
   `backtest_real.py` / `xyield_engine.py`. Ahí está el edge, no en el forecast.
4. Si igual querés adaptar el ancho: usá **EWMA de vol realizada** (simple, robusto,
   corr 0.42 con la vol agregada). No justifica el peso de TimesFM/GARCH.

## Cómo correr
```bash
cd analytics/lp_strategy/vol_forecast
python3 fetch_eth_history.py                 # ETH 18mo
python3 fetch_eth_history.py --symbol BTCUSDT # BTC 18mo
python3 fetch_pool_price_rpc.py              # cross-check on-chain (Waizard, 31d)
python3 vol_forecast.py                      # skill de forecast de vol
python3 backtest_adaptive.py                 # adaptive vs fijo + régimen
python3 build_dataset.py --step 24           # dataset mergeado (503 ventanas)
python3 predict_decision.py                  # GBM walk-forward
python3 timesfm_signal.py                    # TimesFM sobre el PRECIO (subóptimo)
python3 timesfm_volseries.py                 # TimesFM sobre la SERIE DE VOL (el bueno)
```
