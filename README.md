# WebSocket Streamers

Este repositorio contiene dos clientes WebSocket preparados para ejecutar 24/7
y persistir datos en PostgreSQL con reconexión automática, validaciones y
logging JSON detallado.

## Requisitos comunes

* Python 3.10+
* Dependencias Python indicadas en `requirements.txt`
* Base de datos PostgreSQL 16 accesible con las tablas necesarias
* Archivo `.env` en la raíz del proyecto para la configuración

Instalar dependencias una sola vez:

```bash
pip install -r requirements.txt
```

## Capital.com (`streaming_capital.py`)

Variables obligatorias en `.env`:

```
CAPITAL_API_KEY=tu_api_key
CAPITAL_EMAIL=tu_usuario
CAPITAL_PASSWORD=tu_password
PGHOST=localhost
PGPORT=5432
PGDATABASE=tu_bd
PGUSER=tu_usuario
PGPASSWORD=tu_password
CAPITAL_TARGET_EPICS=AVGO,AMD,NVDA
CAPITAL_STREAM_TABLE=capital_market_quotes
CAPITAL_PING_INTERVAL=300
CAPITAL_RECONNECT_DELAY=5
```

La tabla `capital_market_quotes` se crea automáticamente con la estructura
esperada. Ejecuta el servicio con:

```bash
python streaming_capital.py
```

## EOD Historical Data (`streaming_eodhd.py`)

Variables obligatorias en `.env`:

```
API_TOKEN=tu_token_de_eodhd
SYMBOLS=AVGO:AVGO:11,AMD:AMD:7,NVDA:NVDA:6
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tu_bd
DB_USER=tu_usuario
DB_PASSWORD=tu_password
```

Cada entrada en `SYMBOLS` sigue el formato `LOCAL:REMOTE:ASSET_ID`. Si el
símbolo remoto no incluye mercado (por ejemplo `AVGO`), el cliente añadirá el
sufijo configurado en `EOD_SYMBOL_SUFFIX` (por defecto `.US`) antes de
suscribirse, lo que evita respuestas 403 de la API. Ajusta opcionalmente:

```
RECONNECT_DELAY=5              # segundos antes de reintentar
WS_HEARTBEAT_SECONDS=120       # ping automático del cliente aiohttp
EOD_SYMBOL_SUFFIX=.US          # sufijo por defecto para símbolos sin mercado
DB_POOL_MAX=10                 # conexiones máximas del pool PostgreSQL
```

Previo a insertar trades o quotes, el cliente valida las claves críticas y
persiste los datos en las tablas `trades_real_time` y `quotes_real_time` (se
crean automáticamente si no existen). Ejecuta el colector con:

```bash
python streaming_eodhd.py
```

Ambos procesos muestran logs estructurados, detectan respuestas de error del
servidor y se reconectan cada 5 segundos cuando la sesión cae.
