# Bot de _Mean Reversion_ para pools Stable/ETH

Este directorio contiene el script `stable_eth_meanrevert_bot.py`, una herramienta de **back-testing** que busca oportunidades de _mean reversion_ (reversión a la media) entre los distintos pools Stablecoin/ETH de **Uniswap v3**.

El bot examina los precios de **DAI/ETH**, **USDC/ETH** y **USDT/ETH** para los tres _fee tiers_ (100, 500 y 3000). Cuando el precio de un pool se aleja demasiado del consenso (la mediana del resto de pools) abre una posición **LONG** (si está barato) o **SHORT** (si está caro) y la cierra cuando el precio vuelve a la media o cuando se activa un _stop-loss_.

> ⚙️ **En resumen**: Simula comprar ETH cuando está relativamente barato frente a los stables y vender cuando está caro, intentando capturar pequeñas ineficiencias de precio entre pools.

---

## Flujo general del programa

1. **Carga de datos** (`--data`):
   Lee archivos CSV con nombre `*_Swap.csv` que contienen los _swaps_ históricos de cada pool.
2. **Construcción de eventos**:
   Convierte cada _tick_ en un precio aproximado USD/ETH y genera un flujo de eventos cronológico.
3. **Selección del modo de simulación** (`--mode`):
   - `pct`: Usa un umbral de desviación porcentual fija.
   - `zscore`: Usa un _z-score_ sobre una ventana móvil para criterios de entrada/salida (más robusto).
4. **Simulación del bot**:
   Recorre los eventos y abre/cierra posiciones según las reglas elegidas, aplicando comisiones, _stop-loss_, etc.
5. **Exportación de resultados** (`--trades_csv`):
   Guarda cada trade en un CSV, calcula PnL y genera un resumen mensual/anual si se pide `--report`.

---

## Parámetros de línea de comandos

| Parámetro         | Ejemplo         | Explicación                                                                        |
| ----------------- | --------------- | ---------------------------------------------------------------------------------- | --- | ---------------------------------------------------------------- |
| `--data`          | `./data`        | Carpeta con los CSV de swaps (`DAIETH100_Swap.csv`, etc.). **Obligatorio**.        |
| `--freq`          | `60s`           | Frecuencia de re-muestreo (solo en modo `pct`). Normalmente 60 s.                  |
| `--thr`           | `0.10`          | Umbral de entrada en **%** para modo `pct`. Ej.: `0.10` significa ±0,10 %.         |
| `--mode`          | `zscore`        | Tipo de simulación: `pct` (porcentaje) o `zscore` (z-score).                       |
| `--lookback`      | `1440`          | Tamaño de la ventana (nº de filas) para calcular z-score. 1440 ≈ 1 día a 60 s.     |
| `--entry_z`       | `3.0`           | Valor                                                                              | z   | a partir del cual se abre una posición en modo `zscore`.         |
| `--exit_z`        | `0.3`           | Valor                                                                              | z   | al que se cierra la posición (significa casi vuelta a la media). |
| `--max_hold`      | `10080`         | Máximo de filas que se puede mantener una posición (≈1 semana).                    |
| `--stop_pct`      | `2.0`           | _Stop-loss_: si la pérdida no realizada alcanza −2 % se fuerza la salida.          |
| `--open_fee_pct`  | `0.01`          | Comisión simulada (%) al abrir una operación, para cubrir _slippage_ y fees.       |
| `--report`        | `--report`      | Si se incluye, muestra en pantalla la tabla de trades y el resumen de rendimiento. |
| `--notional`      | `1000`          | Tamaño fijo de cada trade (en unidades _stable_). PnL = retorno × notional.        |
| `--start_capital` | `80000`         | Capital inicial ficticio para calcular balance final.                              |
| `--trades_csv`    | `my_trades.csv` | Archivo de salida con los trades generados.                                        |

---

## Ejemplo rápido

```bash
python analytics/stable_eth_meanrevert_bot.py \
  --data ./data \
  --mode zscore \
  --lookback 1440 \
  --entry_z 3.0 \
  --exit_z 0.3 \
  --stop_pct 2.0 \
  --open_fee_pct 0.01 \
  --notional 1000 \
  --start_capital 80000 \
  --trades_csv my_trades.csv \
  --report
```

Esto cargará todos los CSV de `./data`, ejecutará la simulación y guardará los trades en `my_trades.csv`, además de mostrar un resumen en pantalla.

---

## Requisitos

- Python 3.8+
- Paquetes: `pandas`, `numpy`, `argparse`, `tqdm`

Instalación rápida:

```bash
pip install -r requirements.txt
```

¡Listo! Con este _README_ deberías poder entender y ejecutar el bot sin problemas.

### Diferencias entre los modos `pct` y `zscore`

Ambos modos analizan la **desviación** del precio de un pool frente al consenso, pero lo hacen de forma distinta:

#### 1. Modo `pct` – desviación porcentual fija

- Se calcula la desviación instantánea:  
  \(\text{dev} = \frac{\text{precio_pool} - \text{precio_medio}}{\text{precio_medio}}\).
- Si `|dev|` supera el umbral `--thr` (por ejemplo `0.10` ≡ 0,10 %), se abre operación.
  - **LONG** (comprar) si `dev` < 0 (el pool está barato).
  - **SHORT** (vender) si `dev` > 0 (el pool está caro).
- Se cierra cuando el precio vuelve a la media (`dev` ≈ 0) o se dispara el _stop-loss_.

Ejemplo rápido:

| Precio medio  | Precio pool   | Desviación | `--thr` | Señal             |
| ------------- | ------------- | ---------: | ------: | ----------------- |
| 1 800 USD/ETH | 1 815 USD/ETH |     0,83 % |  0,10 % | **SHORT** (caro)  |
| 1 800 USD/ETH | 1 784 USD/ETH |    −0,89 % |  0,10 % | **LONG** (barato) |

#### 2. Modo `zscore` – desviación relativa al historial

- Para cada pool se mantiene un historial de las últimas `--lookback` desviaciones.
- Se calcula el _z-score_:  
  \(z = \frac{\text{dev} - \mu}{\sigma}\), donde \(\mu\) y \(\sigma\) son la media y desviación estándar de ese historial.
- Se opera cuando `|z|` supera `--entry_z` (por ejemplo `3.0`).  
  El cierre ocurre cuando `|z|` baja de `--exit_z` **o** se cumple `--max_hold`/stop-loss.

Ejemplo rápido (números inventados):

| Desviación actual | Media histórica (μ) | Desv. típica (σ) | _z-score_ | `--entry_z` | Señal     |
| ----------------: | ------------------: | ---------------: | --------: | ----------: | --------- |
|           +0,30 % |              0,00 % |           0,08 % |      3,75 |         3,0 | **SHORT** |
|           −0,32 % |              0,01 % |           0,09 % |     −3,67 |         3,0 | **LONG**  |

En otras palabras, el modo `pct` usa un umbral **absoluto** en porcentaje, mientras que `zscore` ajusta el umbral según la **volatilidad** reciente de cada pool, siendo más estricto cuando el mercado está tranquilo y más flexible cuando hay más ruido.
