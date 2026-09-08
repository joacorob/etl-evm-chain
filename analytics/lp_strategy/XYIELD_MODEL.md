# xYield — modelo de ingreso pasivo en stables con lógica "xG"

> Objetivo: ingreso pasivo **>15% anual en stables** combinando Uniswap v3
> concentrado + Aave + hedge en Hyperliquid. Marco mental: **xG del fútbol**.

## 1. La analogía xG (por qué importa acá)

En fútbol el **xG** mide la *calidad esperada* de las chances, no los goles que
entraron (ruidosos, sesgados por suerte). Acá pasa lo mismo con el yield:

- **APR de cabecera (headline)** = goles. Una posición LP muestra 59% APR porque
  el precio se quedó quieto una semana y estuvo in-range todo el tiempo. Eso es
  un *gol con suerte*: no es repetible.
- **xYield** = la chance esperada. Es el APR neto *estructural*, descompuesto en
  sus drivers repetibles, separando la señal del path de precio puntual.

Regla operativa: **dimensionás por xYield, no por el último realizado.**

## 2. Descomposición

```
xYield = xFee·(in-range)  +  xFunding  +  xFloor  −  LVR  −  costos
```

| Término | Qué es | Dato real hoy (jun-2026) |
|---|---|---|
| `xFee` | fee tier × volumen / TVL_in_range, **mientras está in-range** | medido del pool real |
| `xFunding` | funding del short en Hyperliquid (el short **cobra** si funding>0) | **+3 a +11%/año, MUY volátil** |
| `xFloor` | Aave supply sobre el stable ocioso / margen | **3.1% (Base USDC)** |
| `LVR` | *loss-versus-rebalancing*: lo que el LP le regala al arbitrajista; ∝ σ² × concentración | el costo a vencer |
| costos | gas + slippage de re-center + fee del perp | ~5-10 bps por evento |

**LVR es el enemigo.** Proveer liquidez = vender vol (short gamma). Si los fees
no superan el LVR, perdés aunque estés delta-neutral. Para ETH a vol 0.95 hacen
falta ~11.4% de fee APR en full-range sólo para empatar; concentrado ±W escala
fee y LVR **juntos**, así que la pregunta de fondo no es el ancho sino:
**¿este pool genera más fees que LVR?**

## 3. Arquitectura (3 sleeves)

- **A — Piso (Aave USDC):** preserva capital, ancla la base. ~3.1%. Bajo riesgo.
- **B — LP estable (USDC/USDT concentrado):** hoy rinde **0.3-5%**, flojo. Poco peso.
- **C — Motor α (LP ETH/USDC concentrado + SHORT ETH en Hyperliquid):**
  delta-neutral. Cobra fees + funding, paga LVR. Es el único que puede empujar
  el portafolio a >15% — **si el fee supera al LVR**.

El short en HL neutraliza el delta direccional del LP. **Clave: el hedge debe
re-ajustarse seguido** (bot, ~diario/semanal). Set-and-forget = sangra gamma
(el modelo lo confirma: mediana **negativa** sin re-hedge ni re-center).

## 4. Resultado del Monte Carlo (motor α, ±12%, 365d, vol medida 39%)

Todo depende del fee in-range sostenido. Es la pregunta xG:

| Escenario fee in-range | xYield motor | mediana | P(>15%) | P(<0) |
|---|---|---|---|---|
| **Conservador 30%** (≈ realizado de vida) | **≈ 0%** | -0.3% | 0% | 53% |
| **Optimista 59%** (= 7d annualizado crudo) | **+25%** | +25% | 94% | 0% |

Portafolio mezclando con piso+LP-estable: con el motor conservador **no llega a
15%** (~1-2%); con el optimista, sobra.

## 5. Conclusión honesta (mid-2026)

**El >15% pasivo en stables NO está regalado en este régimen.** El carry fácil
se comprimió: funding HL **3-11% y volátil** (no los 11% de los bulls), Aave
**~3%**, LP estable **<5%**. El único motor que cruza 15% es el LP de ETH
delta-neutral, y vive o muere según una sola variable: **el fee sostenido del
pool vs su LVR**, que hoy está entre "breakeven" y "+25%" — incertidumbre real,
no resuelta con 7 días de data quieta.

**Próximo paso decisivo (xG-style):** medir el ratio fee/LVR *realizado* del
pool WETH/USDC 0.3% Base sobre el path real (no annualizar una semana). Tengo
los swaps; se computa el LVR real contra los fees reales y se sabe si el motor
es +25% o breakeven. Hasta tener eso: **no sobre-dimensionar el sleeve C.**

## 5-bis. VEREDICTO EMPÍRICO (reverse-engineering de ~1.800 posiciones reales)

Usando el endpoint lista interno de revert (`?token0=&token1=` en lowercase,
`sort=underlying_value`, `has-withdrawn=false`) medimos el fee_apr **realizado y
sostenido** de posiciones reales con plata, viejas (age>30d) y sin retiros — no
un backtest de una semana. Script: `revert_reveng.py`.

**Fee APR REAL sostenido (bruto, concentradas ±15-25%, gestionadas):**

| Estrategia | fee_apr med | p75 |
|---|---|---|
| WETH/USDT 0.3% mainnet | **19.6%** | 25% |
| WETH/USDC 0.05% arbitrum | **19.1%** | 24% |
| WETH/USDC 0.05% mainnet | **17.2%** | 24% |
| WBTC/USDC 0.3% | 13.1% | 17% |
| DAI/USDC 0.01% | 5.3% | 6% |
| **USDC/USDT (todos los tiers)** | **1-2%** | 3% |

**Pero el fee bruto NO es lo que te llevás.** El NET real (WETH/USDC, n=31):

| Métrica | mediana | p25 | p75 |
|---|---|---|---|
| fee_apr bruto | 15.8% | 10% | 22% |
| **ROI vs HODL anualizado** | **−5.9%** | −32% | +7% |
| posiciones que le ganan a HODL | **42%** | | |

El IL se come el fee: **el LP mediano de ETH/USDC pierde contra holdear.** Coincide
con tu propia frase a Mario (ene-2026): *"no conozco a nadie que de manera
sostenida tenga profit con LP providing."*

**Por qué el delta-neutral igual tiene sentido:** ese −5.9% es `fee − IL_total`,
y el IL_total es casi todo **direccional** (ETH se movió). El short en HL
**neutraliza la parte direccional**, dejándote `fee − LVR + funding`. Eso convierte
el −6% del LP crudo en un neto positivo de un dígito alto / ~10-13% para un
operador competente, y >15% sólo cuando el funding pega un pico o sos del cuartil
top (p75 bruto 22-25%). El spread p25→p75 (bruto 10→22%, net −32→+7%) **es la
prueba xG del "finishing":** la gestión/timing decide, no el promedio.

## 6. Conclusión ejecutiva (qué te recomiendo de verdad)

1. **El >15% pasivo set-and-forget en stables NO existe hoy** con estos ladrillos.
   El carry se comprimió (funding 3-11% volátil, Aave 3%, stable-LP 1-5%).
2. **Stable/stable LP y Aave = piso (1-5%), no motor.** Olvidate de sacar 15% ahí.
3. **El único motor real es ETH/USDC(USDT) concentrado ±15-25%** (fee bruto 16-20%
   real), **delta-hedgeado en Hyperliquid** (strip del IL direccional + cobra
   funding). Net realista **8-13%**, con upside a >15% en ventanas de funding alto.
4. **No es pasivo:** requiere re-center + re-hedge por bot y elegir bien el pool/
   ancho. La dispersión demuestra que es skill, no renta.
5. **Para forzar >15% hay que aceptar algo más:** (a) apalancar con loop en Aave
   (riesgo liquidación), (b) estructura junior/senior tranche de Mario (tomás el
   tramo junior = más yield, primero en perder), o (c) timing de entradas por
   funding/vol. Ninguna es "pasiva pura".

## 8. Funding real (HL) y el tramo junior/senior — cómo forzar >15%

**Funding histórico Hyperliquid (real, medido):** está **capeado ~+11%/año por
diseño**. ETH: media +3.8%, mediana +7.4%, nunca >15%. BTC: media +7.3%, mediana
+11%. El short cobra positivo 71% (ETH)/86% (BTC) del tiempo. → el funding aporta
~+5% al motor, NO es fuente de 15% por sí solo. Esto ancla el motor delta-neutral
en **~8-14% neto** (filo del objetivo).

**Tramo junior/senior (idea de Mario) — `tranche_model.py`:** partís el capital;
el SENIOR cobra cupón fijo ~5% (cobra primero, casi sin riesgo: p5=+5%, 0% pérdida
en el rango modelado); el JUNIOR queda apalancado ~2.5x sobre el exceso del motor.

Con motor ~10% mediana, senior 60%@5%:
| Tramo | media | mediana | p5 | P(>15%) | P(pérdida) |
|---|---|---|---|---|---|
| **Senior (60%)** | +5.0% | +5.0% | +5.0% | — | **0%** |
| **Junior (40%)** | +17.5% | +17.6% | **−28%** | 54% | **26%** |

**El junior cruza 15% de media, pero con cola real** (1 de cada 4 años pierde,
p5 −28%). La estructura NO crea alpha: redistribuye riesgo. **Sólo te apalanca de
verdad si el senior es plata de OTRO** (le pagás 5%, te quedás el upside). Si es
todo tuyo, partirlo es contabilidad mental — tu retorno mezclado sigue siendo el
~10% del motor. El equivalente "DeFi" del mismo lever = loop en Aave (pedir stable
contra stable al ~4-5% y redepositar).

## 9. Síntesis de las 3 vueltas (qué hacer, ordenado por realismo)

1. **Piso sin laburo (3-5%):** Aave USDC + algo de DAI/USDC LP. Pasivo de verdad.
2. **Motor gestionado (8-14%):** ETH/USDC(USDT) 0.05-0.3% concentrado ±15-25%,
   re-center por bot, **short ETH en HL** para strip del IL direccional + cobrar
   funding. Esto es lo más cerca de ">15% honesto" sin estructura, y requiere
   trabajo + skill (la dispersión es enorme).
3. **>15% forzado (con riesgo explícito):** tramo junior (si conseguís senior
   barato) o loop apalancado en Aave sobre el motor. Cola real: podés perder.
4. **Olvidate de:** stable/stable LP (1-5%, muerto), set-and-forget delta-neutral
   (sangra gamma), y de los backtests que te prometen 80-400%/mes (overfit).

## 10. Profundización: apalancar el motor (`leverage_loop.py`) + ruina

**Las 3 formas de forzar >15% son la MISMA palanca:** loop en Aave (depositás
stable, pedís stable en eMode ~90% LTV, deployás lo prestado en el motor), tramo
junior, o leverage directo. Todas: `net_equity = m·motor − (m−1)·borrow`.

**Cross-check de mercado:** Ethena **sUSDe rinde 3.6%** (TVL $1.74B) — ese es el
precio del funding-carry puro de los pros, confirmando funding comprimido. El loop
de sUSDe está MUERTO (carry 3.6% < borrow 5%). Por eso el motor LP+hedge (~10%)
es mejor base que el carry puro: los fees del LP suman arriba.

**Apalancamiento del motor (media 10%, borrow 5%) — el volatility tax es real:**

| leverage | CAGR 5a med | CAGR p5 | maxDD med | P(DD>50%) | P(ruina) |
|---|---|---|---|---|---|
| 1.0x | 9.9% | −2% | −6% | 1% | 0% |
| 2.0x | 13.7% | −14% | −16% | 19% | 0% |
| **2.5x** | **15.1%** | −28% | −22% | **30%** | 1% |
| 3.0x | 16.3% | −75% | −27% | 37% | 5% |
| 4.0x | 16.8% | −98% | −40% | 46% | 18% |

**El hallazgo clave:** la CAGR mediana se **estanca en ~15-17% pasado 2.5x** —el
volatility tax y la cola izquierda se comen el extra— mientras el drawdown y la
ruina explotan. Subir leverage más allá de ~2.5x es una **trampa**: tomás mucha más
cola por casi nada de retorno compuesto extra (es Kelly en acción).

**Sweet spot ≈ 2.0-2.5x:** lleva la mediana a ~14-15% CAGR, P(>15% en un año)
60-64%, con drawdown mediano −16/−22% y ruina ~1%. Ese es el punto honesto.

**El riesgo NO es liquidación de Aave** (stable/stable eMode = LTV estable, ~0
liquidación por precio). Es **drawdown económico del motor × leverage**. El riesgo
operativo aparte: el **short en HL puede liquidarse si ETH pumpea** sin margen —
mantené el perp a bajo leverage (el LP gana cuando ETH sube y compensa).

## 11. Veredicto final del loop (4 vueltas)

Para >15% en stables, ordenado por lo que de verdad funciona:
- **Realista y honesto:** motor DN gestionado (~10%) apalancado **2-2.5x** →
  **CAGR ~14-15%**, aceptando ~30% de probabilidad de un drawdown >50% en 5 años.
  Es lo más cerca de un >15% defendible, y NO es pasivo.
- **Si querés menos cola:** motor sin apalancar (~10%) + piso Aave. ~8-11%, dormís.
- **Trampa:** apalancar >3x (ruina), stable/stable LP (1-5%), sUSDe loop (carry
  negativo), backtests de 80-400%/mes (overfit).

## 12. STRESS TEST con historia REAL (`backtest_real.py`) — rompe el modelo cómodo

Todo lo anterior asumía vol ~39% (la semana quieta). Corrí el motor sobre el path
REAL de ETH del último año: **$2577 → $4829 → $1679** (−35% neto, **vol 68%**,
round-trip brutal). Resultado por ancho × modo de hedge (fee_in=30%):

| Ancho | sin hedge | hedge **static** | hedge dyn |
|---|---|---|---|
| **±15% (tight)** | −40% | −20% | −39% ☠️ |
| ±25% | −20% | −1% | −6% |
| **±40% (wide)** | −9% | **+9%** | +5% |
| ±60% | −16% | −0.5% | **+13%** |

**Tres hallazgos que rompen el modelo cómodo:**
1. **Tight ±15% delta-neutral es una TRAMPA en vol alta** — perdió 20-40% este año.
   La concentración que parecía buena (más fee) se autodestruye por IL/whipsaw.
2. **Hay que ir ANCHO (±40-60%) + hedge ESTÁTICO** para sobrevivir un round-trip.
   El short estático capturó +17k del movimiento direccional; el **re-hedge
   dinámico se whipsappea** (compra caro/vende barato) y queda en −1k.
3. **El edge real = ADAPTAR el ancho a la vol** (ancho cuando hay vol). Eso explica
   la dispersión enorme de revert (p25 −32% / p75 +24%): es timing/gestión, xG puro.

**Realismo del fee:** ±40% gana MENOS fee que ±15% (menos concentración) — fee_in
realista ~20-25%, no 30. A fee_in=20%, ±40% static dio **−0.5% este año**. O sea:
**en el peor tipo de año (vol 68% round-trip), el mejor delta-neutral realista fue
~breakeven.** El motor hace plata en años más calmos; en un año como este, no.

⚠️ **Implicación brutal para el leverage:** apalancar 2.5x un motor que este año
hizo −20% (config naive) = **−50% a −95%**. El sweet-spot 2.5x de la sección 10
asume motor +10%; con un año malo real, la ruina es concreta. **Apalancá solo si
adaptás el ancho y aceptás que un año tipo-2025/26 te lastima fuerte.**

## 13. VEREDICTO DEFINITIVO — full-cycle 2.7 años (Binance ETHUSDT 1000d)

Extendí el backtest a **2.7 años reales (2023-09 → 2026-06)**: ETH **flat**
(1643→1679) pero vol **67%** sostenida (rango 48-82%), múltiples round-trips. Es
el test más justo posible: sin tendencia, el hedge direccional no regala nada y
queda expuesto el puro fee-vs-LVR.

**Calibración del fee contra revert:** `fee_k=4` reproduce exactamente el ~18-20%
realizado que mide revert para ±15-25% (±20% → 20% realizado). A ESE fee real:

| Fee asumido | CAGR full-cycle (±40% static) |
|---|---|
| **fee_k=4 (revert-real, ~18-20%)** | **−6.2%** ← la realidad |
| fee_k=8 (2× del real) | +3.4% |
| fee_k=12 (3× del real) | +11.6% |

**Rolling 1-año, TODAS las configs (a fee real):** media −13 a −16%, **P(>15%)=0%**,
P(pérdida) 95-100%. Adaptar el ancho a la vol **no salva** (−16 a −21%).

### La conclusión inapelable

> **A fees reales (los que pagan los pools de verdad) y vol real (67%), el motor
> delta-neutral ETH/USDC PIERDE ~6%/año a lo largo del ciclo.** Para ser positivo
> necesitarías el DOBLE del fee que existe. El "+10%" del Monte Carlo (secciones
> 4-10) era un espejismo de vol calma (39%); ETH estuvo a 48-82% todo el período.

El +9% del backtest de un año (sección 12) era **el hedge ganando con la caída de
ETH** (−35% ese año), NO fee alpha — un viento de cola direccional irrepetible.

**Qué SÍ gana plata en stables (todo <15%, todo real):** Aave USDC ~3%, DAI/USDC
LP ~5%, sUSDe ~3.6%, créditos RWA/curated 8-12% (con riesgo de contrato/crédito).
**Nada pasivo y seguro cruza 15% hoy.** El ">15% en stables" del pedido original,
después de 6 vueltas con data real, **no existe** — y el "motor" que parecía la vía
en realidad pierde a lo largo del ciclo. El valor de este trabajo es **negativo
pero crítico**: te ahorra deployar (y peor, apalancar) una estrategia que
backtestea a −6%/año.

## 7. Cómo correrlo

```bash
python analytics/lp_strategy/xyield_engine.py    # modelo + Monte Carlo (HL+Aave+7d pool)
python analytics/lp_strategy/revert_reveng.py    # fee real sostenido (~1.800 posiciones revert)
python analytics/lp_strategy/tranche_model.py --engine-mean 10 --engine-std 11  # junior/senior
python analytics/lp_strategy/leverage_loop.py --engine-mean 10 --borrow 5       # leverage + ruina
python analytics/lp_strategy/backtest_real.py    # stress test sobre ETH real (necesita /tmp/eth_daily.csv)
```
