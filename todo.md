# TODO - Web3 ETL Go Indexer (Apollo-based Clone)

Este proyecto es un indexador ETL en Go basado en [chainbound/apollo](https://github.com/chainbound/apollo). Su propósito es leer eventos desde un contrato EVM (via ABI), desde un bloque inicial, y persistirlos en una base de datos o archivo CSV.

---

## 🎯 Objetivo General

- Dado:

  - `RPC_URL`
  - `CONTRACT_ADDRESS`
  - `ABI_FILE`
  - `START_BLOCK`
  - `CONFIG_FILE`

- El indexador:
  1. Se conecta al nodo.
  2. Escanea eventos desde el bloque indicado hasta el más reciente.
  3. Decodifica usando el ABI.
  4. Persiste cada evento (nombre, params, tx_hash, timestamp, etc.).
  5. Lo guarda en el destino elegido (`mysql` o `csv`).

---

## 📁 Estructura del Proyecto

```
etl-web3/
├── cmd/
│   └── indexer.go
├── internal/
│   ├── config/         # Lee config.yaml
│   │   └── config.go
│   ├── indexer/        # Orquestador principal
│   │   └── indexer.go
│   ├── rpc/            # Manejo de RPC
│   │   └── client.go
│   ├── parser/         # Decodifica logs
│   │   └── parser.go
│   ├── sink/           # Almacenamiento
│   │   ├── mysql.go
│   │   └── csv.go
├── abi/                # ABIs cargadas por config
│   └── my_contract.json
├── config.yaml         # Configuración principal
├── go.mod
└── go.sum
```

---

## ⚙️ Configuración (config.yaml)

```yaml
rpc_url: "https://mainnet.infura.io/v3/YOUR_KEY"
start_block: 12345678
contracts:
  - name: MyContract
    address: "0x123abc..."
    abi: "./abi/my_contract.json"
storage:
  type: "mysql" # o "csv"
  mysql:
    dsn: "user:pass@tcp(127.0.0.1:3306)/mydb"
  csv:
    output_dir: "./data"
retry:
  attempts: 3
  delay_ms: 1500
```

---

## ✅ Tareas por Módulo

### 1. 🧠 Config Loader

- [x] Leer `config.yaml` con `gopkg.in/yaml.v2`
- [x] Validar rutas y parámetros básicos
- [x] Cargar ABI desde archivo JSON

### 2. 🌐 RPC Client

- [x] Conectar al nodo usando `ethclient.Dial`
- [x] Implementar retries configurables (`attempts`, `delay_ms`)
- [x] Obtener bloques con `eth_getBlockByNumber`
- [x] Obtener logs con `eth_getLogs` entre rangos

### 3. 🔎 Indexer

- [x] Iterar desde `start_block` hasta `latest_block`
- [x] Dividir en chunks (ej. 1000 bloques por llamada)
- [x] Por cada batch:

  - [x] Llamar a `fetchLogs`
  - [x] Parsear logs
  - [x] Guardar eventos en sink

- [x] Mostrar progreso en consola:
  ```
  [OK] Block 182000 → 182999 | Events: 48 | Time: 1.3s
  ```

### 4. 🧠 Parser

- [x] Usar ABI para identificar `topic0` (event signature)
- [x] Decodificar cada log en un `map[string]interface{}`
- [x] Agregar metadata:
  - `event_name`
  - `tx_hash`
  - `block_number`
  - `timestamp`
  - `tx_from`

### 4.1 🎯 Filtro por evento único

Implementar soporte para escuchar únicamente eventos específicos, optimizando la llamada `eth_getLogs` para que el nodo devuelva solo los logs deseados.

- [x] Extender `config.yaml` añadiendo, dentro de cada contrato, la clave `events`, por ejemplo:\
  ```yaml
  contracts:
    - address: "0xa0b8…e6eb48"
      abi: "./abi/token.json"
      events:
        - Transfer
  ```
- [x] En `internal/config/config.go`:
  - Agregar el campo `Events []string \`yaml:"events"\``a`ContractConfig`.
  - Cargar y validar la lista (puede estar vacía para mantener retro-compatibilidad).
- [x] Durante la inicialización del `Indexer`, calcular el `topic0` de cada evento usando la ABI (`ev.ID`) y almacenarlo.
- [x] Modificar el `FilterQuery` en `internal/indexer/indexer.go` para incluir `Topics: [][]common.Hash{{topic1, topic2, …}}` y filtrar directamente en el nodo.
- [x] Si la lista `events` está vacía, mantener el comportamiento actual (escuchar todos los eventos).

### 4.2 ⚡️ Iteración por eventos en chunks

Implementar la obtención de logs únicamente mediante `eth_getLogs`, evitando descargar bloques completos. El flujo debe funcionar por ventanas (chunks) de bloques.

- [x] Construir `FilterQuery` por chunk:
  - `FromBlock`: bloque inicial del chunk
  - `ToBlock`: bloque final del chunk
  - `Addresses`: lista de contratos
  - `Topics`: `[][]common.Hash{{sig1, sig2, …}}` (event signatures permitidas)
- [x] Dividir el rango total en chunks de tamaño configurable (`chunk_size`).
- [x] Iterar chunks y llamar a `GetLogs` para cada uno.
- [x] Asegurar **no** usar `eth_getBlockByNumber` dentro del loop principal de escaneo.
- [x] Optimizar metadatos:
  - Reemplazar `BlockByNumber` por `HeaderByNumber` para timestamp.
  - Implementar caché simple `map[uint64]uint64` para reutilizar timestamp de bloques ya consultados.
- [x] Modificar `parser/enrichWithBlockAndTx` para reemplazar la llamada a `BlockByNumber` por `HeaderByNumber` (o cachear el timestamp) y así evitar traer el bloque completo en cada evento.

### 5. 💾 Sink (Storage)

#### CSV

- [x] Crear archivo por evento (ej. `Transfer.csv`)
- [x] Escribir headers si no existen
- [x] Agregar fila por evento

#### MySQL

- [ ] Crear tabla si no existe (`event_{event_name}`)
- [ ] Mapear tipos Go a SQL (`string`, `int`, `decimal`, etc.)
- [ ] Insertar con `tx_hash`, `timestamp`, y cada parámetro

### 6. 🚨 Logging & Retry

- [x] Usar `logrus` o `zap`
- [x] Logs de progreso por bloque
- [x] Retry automático en:
  - Timeout de RPC
  - Error de red
  - Fallos temporales de sink

### 7. 📦 CLI Launcher

- [ ] Comando principal:

  ```bash
  go run cmd/indexer.go --config=config.yaml
  ```

- [ ] Flags opcionales para override:
  - `--start-block`
  - `--rpc-url`
  - `--storage-type`

### 8. 🛠️ Housekeeping & DevOps

- [x] **`.gitignore` completo**

  - Crear un `.gitignore` en la raíz que excluya artefactos generados, archivos temporales y secretos:
    - `.progress.json`
    - `data/*.csv`
    - `internal/progress/`
    - `*.log`
    - `bin/`
    - `*.env`
    - Archivos de IDE y sistema (`.DS_Store`, `.vscode/`, etc.).

- [x] **`config.yaml.example` versionado**

  - Añadir un `config.yaml.example` con la misma estructura pero valores de ejemplo/mock.
  - Instruir en el README a clonar el ejemplo: `cp config.yaml.example config.yaml` y luego editar.
  - Incluir el `config.yaml` real en `.gitignore` para evitar subir credenciales.

- [x] **Nombrado seguro de archivos CSV**
  - Al persistir, usar el patrón: `<ContractName>_<EventName>.csv` (ej. `USDC_Transfer.csv`).
  - Implementar en `internal/sink/csv.go`:
    ```go
    filename := fmt.Sprintf("%s_%s.csv", contract.Name, eventName)
    ```
  - Garantiza que ejecuciones múltiples o contratos distintos no sobrescriban datos.

### 9.0 🌐 REST API – Setup & Infra

- [x] Implementar micro-servicio HTTP (framework sugerido: `gin`, `chi` o `echo`).
- [x] Crear paquete `internal/api/` con:
  - `server.go` (router & middlewares)
  - `handlers.go` (handlers de los endpoints)
  - `models.go` (`JobRequest`, `JobResponse`, `JobStatus`)
- [x] Puerto configurable (`API_PORT`).
- [x] Middleware de logging y recuperación de _panics_.
- [x] Mapa concurrente `map[string]*JobStatus` protegido con `sync.RWMutex` para registrar jobs.

### 9.1 🎛️ POST /jobs – Lanzar indexado

- [x] Recibir un JSON con los parámetros del `config.yaml` (`rpc_url`, `start_block`, `contracts`, `storage`, etc.).
- [x] La ABI se envía como **string** con la firma del evento (ej.: `"Transfer(address,address,uint256)"`).
- [x] Validar payload y construir `Config` interno.
- [x] Generar un `job_id` (UUID) y responder `{ "job_id": "<uuid>" }`.
- [x] Lanzar el `Indexer` en una _goroutine_ asociada al `job_id`.
- [x] Ejemplo de uso:
  ```bash
  curl -X POST http://localhost:8080/jobs \
       -H 'Content-Type: application/json' \
       -d '{ "rpc_url": "https://…", "start_block": 123, "contracts": [...], "abi": "Transfer(address,address,uint256)" }'
  ```

### 9.2 🔍 GET/DELETE /jobs/{job_id} – Estado y Control

- [x] `GET /jobs/{job_id}` devuelve `queued | running | finished | error` y metadatos: bloque actual, eventos procesados, errores, etc.
- [x] `DELETE /jobs/{job_id}` permite cancelar un job en ejecución (opcional).
- [x] `JobStatus` debe actualizarse mediante callbacks o canales desde el `Indexer`.
- [x] Al finalizar exitosamente, el estado pasa a `finished`; en caso de error, a `error` con detalle.

---

## 🧪 Test Manual

- [ ] Indexar `Transfer` de USDC
- [ ] Validar datos decodificados
- [ ] Validar persistencia CSV
- [ ] Validar persistencia en MySQL
- [ ] Verificar logging + retry

---

## 🔁 Bonus: Reanudación

- [x] Guardar último bloque en `.progress.json`
- [x] Al iniciar, continuar desde último bloque si existe
- [ ] Guardar csv en un s3

---

## 🌱 Futuras mejoras

- [ ] Webhook sink
- [ ] Soporte a múltiples contratos en paralelo
- [ ] Soporte a múltiples eventos por contrato
- [ ] API REST para exponer datos
- [ ] Metrics (Prometheus)
