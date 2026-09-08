# ¿Cuánto tendría hoy? — $5k USDC + $5k ETH hace 12 meses

**Setup:** $10.000 ($5k USDC + $5k ETH) el **2025-06-16** (ETH $2.544). Hoy (2026-06-16)
ETH $1.794 → **ETH −29.5%** en el año. Estrategia ganadora del estudio: ancho
**vol-adaptive** (∝ σ reciente, clip ±10-40%) + **short estático delta-neutral** + re-center
real. Script: `backtest_12mo.py`. Mecánica = `backtest_real` (LP value v3, IL real del path,
fee in-range escalado por concentración η, PnL del short, funding).

## Benchmarks (no hacer nada)

| | valor hoy | retorno |
|---|---|---|
| todo en USDC | $10.000 | +0.0% |
| HODL 50/50 (5k+5k ETH) | **$8.525** | −14.7% |
| HODL todo ETH | $7.051 | −29.5% |

## La estrategia (fee 30%@±40%, funding 5%, modo de hedge)

| variante | valor hoy | retorno | nota |
|---|---|---|---|
| LP sin hedge | $10.376 | +3.8% | fees compensan IL |
| **LP + short ESTÁTICO** | **$11.448** | **+14.5%** | hedge captó +$1.340 de la caída |
| LP + re-hedge dinámico | $10.206 | +2.1% | se whipsapea (peor, como ya sabíamos) |

El short estático protege el principal del −29.5% de ETH; el dinámico se whipsapea
(confirma `backtest_real`). Desglose static: fees +$6.076, hedge +$1.340, funding −$267,
costos −$247, 61 re-centers.

## El FEE REAL, medido de los swaps on-chain (no más asumir)

`measure_real_fees.py` mide el fee directo de `data/USDC_ETH_Pool_Swap.csv` = swaps reales
del pool **USDC/WETH 0.05% de mainnet** (`0x88e6A0c2...`, el de más volumen). Para cada
swap: mi share del fee = `L_mía / L_total` (el campo `liquidity` del log), fee = 0.05%·volumen.

**Resultado (posición $10k, in-range):**

| ancho | fee APR REAL | η vs ±40% |
|---|---|---|
| ±8% | 157% | 5.1× |
| ±10% | **119%** | 3.9× |
| ±15% | 80% | 2.6× |
| ±25% | 49% | 1.6× |
| ±40% | **30.6%** | 1.0× |

Esto era un **snapshot de 2 días de volumen alto** ($153M/día). Para el número de verdad
bajé **366 días** del acumulador on-chain (ver abajo) — el snapshot resultó OPTIMISTA.

### El dato REAL: 366 días on-chain (sin asterisco)

`fetch_feegrowth.py` + `analyze_feegrowth.py` leen el acumulador `feeGrowthGlobal{0,1}X128`
del pool en bloques históricos (archive state vía publicnode/drpc, que SÍ lo sirven a 12m).
ΔfeeGrowth × tu liquidez = los fees que cobraste. Un día = una lectura. **365 días reales,
2025-06-17 → 2026-06-17.**

**Fee APR real por mes (posición $10k, in-range):**

| mes | ±10% | ±40% | | mes | ±10% | ±40% |
|---|---|---|---|---|---|---|
| 2025-06 | 89% | 23% | | 2025-12 | 98% | 25% |
| 2025-07 | 84% | 22% | | 2026-01 | 51% | 13% |
| 2025-08 | 131% | 34% | | 2026-02 | 133% | 34% |
| 2025-09 | 64% | 17% | | 2026-03 | 61% | 16% |
| 2025-10 | 127% | 33% | | 2026-04 | 48% | 12% |
| 2025-11 | 183% | 47% | | 2026-05 | **27%** | **7%** |
| | | | | 2026-06 | 101% | 26% |

→ El fee varía **brutalmente** mes a mes (±40%: de 7% en may-26 a 47% en nov-25). Por eso
2 días no sirven. **Promedio ANUAL real:**

| ancho | fee APR REAL anual | $/año sobre $10k |
|---|---|---|
| ±10% | **91.7%** | $9.166 |
| ±15% | 61.6% | $6.163 |
| ±25% | 37.4% | $3.744 |
| **±40%** | **23.6%** | $2.359 |

**>>> FEE_REF REAL @±40% = 23.6% APR** (no 30%, no 30.6% — esos eran ventanas activas).

## Respuesta honesta FINAL (con el fee real anual on-chain)

Inyectando el 23.6% real al backtest sobre los 12 meses (ETH −29%):

| | valor hoy | retorno |
|---|---|---|
| **LP + short estático (delta-neutral)** | **$10.149** | **+1.5%** |
| LP sin hedge | $9.077 | −9.2% |
| todo USDC | $10.000 | 0% |
| HODL 50/50 | $8.525 | −14.7% |
| HODL todo ETH | $7.051 | −29.5% |

- **Tendrías ~$10.150 (+1.5%)** — esencialmente FLAT, pero **le ganaste a HODL 50/50 por
  ~$1.600 y a HODL ETH por ~$3.100**, en un año que ETH cayó 29%. Desglose: fees reales
  +$4.777, hedge +$1.340, neto de funding/costos.
- **El hedge estático fue lo que te salvó** (sacó el −29% direccional); los fees reales
  alcanzaron justo para cubrir el LVR y quedar arriba de breakeven.
- **Sin hedge perdías** (−9.2%): los fees no cubren el IL direccional en un trend bajista.
- **El snapshot de 2 días (30.6%→$11.570) era optimista**; el real anual (23.6%→$10.149)
  es el número que vale.

**TL;DR número REAL:** **~$10.150 (+1.5%)** delta-neutral, con fee on-chain real de 366
días. Ganó a todo "hold", quedó ~flat en absoluto, y vive del hedge + un fee que en este
pool promedió 23.6%@±40% pero osciló 7-47% según el mes.

### Cross-check con Waizard (Base) — confirma el número

Usando el RPC de Waizard sobre el pool **WETH/USDC 0.3% de Base** (`0x6c561b44...`, el que
memoria marca como ganador), mismo método (feeGrowthGlobal, últimos 30 días que Waizard
retiene): **±40% = 20% APR, ±10% = 79% APR**. En línea con mainnet (±40% anual 23.6%).
Dos pools, dos chains, dos fee tiers, dos proveedores RPC → mismo rango ~20-25%@±40%. Sólido.

## ¿Se puede automatizar? Sí — arquitectura

Todo esto es un **keeper bot** sin ML (la decisión es una fórmula de vol):

1. **Datos** (cada bloque/hora): precio del pool (RPC [[reference_base_rpc]]) + σ reciente
   (EWMA de returns, ventana 168h). Sin TimesFM ni modelos — el estudio probó que no agregan.
2. **Ancho**: `w = clip(2·σ_7d, 0.10, 0.40)`. Una línea.
3. **LP** (Uniswap v3 `NonfungiblePositionManager` en Base, gas barato):
   `mint` / `collect` (fees) / `decreaseLiquidity`+`burn`+`mint` para re-center cuando el
   precio sale del rango o en el boundary semanal.
4. **Hedge** (perp): short ESTÁTICO del delta ETH inicial en Hyperliquid/GMX/dYdX. Clave:
   **no re-hedgear en cada movimiento** (se whipsapea); ajustar solo si el tamaño cambia
   mucho. Mantener margen para no liquidarte.
5. **Trigger**: cron/keeper; re-center on-exit + re-evaluar ancho semanal.
6. **Building blocks ya en el repo**: `xyield_engine.py`, `leverage_loop.py`, `v3_math.py`,
   `fetch_base_swaps.py`. Falta pegar el position manager + el conector de perp.

**Riesgos a vigilar:** churn de re-center (lo controla el piso ±10% + ancho-vol), funding
del perp (puede ser INGRESO si longs pagan), liquidación del short (margen), riesgo de
contrato, fallo del keeper. **El edge no es el bot — es el fee tier × volumen del pool.**
Conviene correrlo en un pool con fees reales altos (no ETH/USDC mayor), o en cbBTC/WETH
([[project_cbbtc_weth_lp]]) donde ya medimos los números reales.

## Cómo correr
```bash
cd analytics/lp_strategy/vol_forecast
python3 fetch_feegrowth.py    # baja 365 días de feeGrowthGlobal on-chain (publicnode/drpc archive)
python3 analyze_feegrowth.py  # fee APR real día/mes/año + inyecta al backtest (EL DATO REAL)
python3 measure_real_fees.py  # (alternativo) fee de swaps crudos, ventana corta del repo
python3 backtest_12mo.py      # simulación en dólares, sensibilidad al fee
```
