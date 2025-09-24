from __future__ import annotations

"""Servicio unificado para los streams de Capital.com y EODHD."""

import asyncio
import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

try:
    from websocket import WebSocketApp
except ImportError as exc:  # pragma: no cover - verificación de dependencias
    raise ImportError("Se requiere el paquete 'websocket-client'") from exc

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
SUPERVISOR_LOGGER = logging.getLogger("StreamingSupervisor")


# ---------------------------------------------------------------------------
# Utilidades comunes
# ---------------------------------------------------------------------------

def configure_logging() -> None:
    """Configura el logging raíz con un formato homogéneo."""

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = logging.getLevelName(level_name)
    if isinstance(level, str):
        level = logging.INFO

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root_logger.addHandler(handler)
    root_logger.setLevel(level)


def load_environment(path: Optional[Path] = None) -> None:
    """Carga variables desde un .env sin sobreescribir valores existentes."""

    env_path = path or Path(".env")
    if env_path.exists():
        load_dotenv(env_path, override=False)


# ---------------------------------------------------------------------------
# Capital.com
# ---------------------------------------------------------------------------

CAPITAL_API_BASE = "https://api-capital.backend-capital.com/api/v1"
CAPITAL_STREAM_URL = "wss://api-streaming-capital.backend-capital.com/connect"


@dataclass
class CapitalConfig:
    api_key: str
    email: str
    password: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    target_epics: List[str]
    table_name: str = "capital_market_quotes"
    ping_interval_seconds: int = 300
    reconnect_delay_seconds: int = 5
    inactivity_timeout_seconds: int = 60
    activity_log_interval_seconds: int = 60

    @classmethod
    def from_env(cls) -> "CapitalConfig":
        raw_epics = os.environ.get("CAPITAL_TARGET_EPICS", "AVGO,AMD,NVDA")
        epics = [item.strip() for item in raw_epics.split(",") if item.strip()]

        missing = [
            key
            for key in ("CAPITAL_API_KEY", "CAPITAL_EMAIL", "CAPITAL_PASSWORD")
            if not os.environ.get(key)
        ]
        if missing:
            raise RuntimeError(
                "Faltan credenciales de Capital.com en el entorno: "
                + ", ".join(missing)
            )

        db_missing = [
            key
            for key in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD")
            if not os.environ.get(key)
        ]
        if db_missing:
            raise RuntimeError(
                "Falta configuración de PostgreSQL en el entorno: "
                + ", ".join(db_missing)
            )

        cfg = cls(
            api_key=os.environ["CAPITAL_API_KEY"].strip(),
            email=os.environ["CAPITAL_EMAIL"].strip(),
            password=os.environ["CAPITAL_PASSWORD"].strip(),
            db_host=os.environ["PGHOST"].strip(),
            db_port=int(os.environ["PGPORT"].strip()),
            db_name=os.environ["PGDATABASE"].strip(),
            db_user=os.environ["PGUSER"].strip(),
            db_password=os.environ["PGPASSWORD"].strip(),
            target_epics=epics,
            table_name=os.environ.get("CAPITAL_STREAM_TABLE", "capital_market_quotes"),
            ping_interval_seconds=int(os.environ.get("CAPITAL_PING_INTERVAL", "300")),
            reconnect_delay_seconds=int(os.environ.get("CAPITAL_RECONNECT_DELAY", "5")),
            inactivity_timeout_seconds=int(os.environ.get("CAPITAL_INACTIVITY_TIMEOUT", "60")),
            activity_log_interval_seconds=int(
                os.environ.get("CAPITAL_ACTIVITY_LOG_INTERVAL", "60")
            ),
        )

        if not cfg.target_epics:
            raise RuntimeError("No hay EPICs configurados para la suscripción")

        return cfg


class CapitalWebSocketStreamer:
    """Stream de Capital.com con reconexión automática y watchdog."""

    def __init__(self, config: CapitalConfig) -> None:
        self.config = config
        self.logger = logging.getLogger("CapitalWebSocket")
        self.stop_event = threading.Event()
        self.connected_event = threading.Event()
        self.ws_lock = threading.Lock()
        self.ws_app: Optional[WebSocketApp] = None
        self.db_conn = self._create_db_connection()
        self.asset_ids = self._load_asset_ids()
        self.tokens: Optional[Dict[str, str]] = None
        self.ping_thread = threading.Thread(
            target=self._ping_loop, name="CapitalPing", daemon=True
        )
        self.ping_thread.start()
        self.watchdog_thread = threading.Thread(
            target=self._inactivity_watchdog,
            name="CapitalInactivityWatchdog",
            daemon=True,
        )
        self.watchdog_thread.start()
        self.last_message_at = time.monotonic()
        self.last_activity_log = time.monotonic() - config.activity_log_interval_seconds
        self.messages_since_log = 0
        self.last_activity_epic: Optional[str] = None
        self.last_activity_ts: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Inicialización
    # ------------------------------------------------------------------
    def _create_db_connection(self) -> psycopg2.extensions.connection:
        self.logger.info(
            "Conectando a PostgreSQL %s:%s", self.config.db_host, self.config.db_port
        )
        conn = psycopg2.connect(
            host=self.config.db_host,
            port=self.config.db_port,
            dbname=self.config.db_name,
            user=self.config.db_user,
            password=self.config.db_password,
        )
        conn.autocommit = True
        self._ensure_destination_table(conn)
        return conn

    def _ensure_destination_table(self, conn: psycopg2.extensions.connection) -> None:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.config.table_name} (
            id BIGSERIAL PRIMARY KEY,
            asset_id INTEGER NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            tipo TEXT NOT NULL,
            precio_compra DOUBLE PRECISION,
            cantidad_disponible_compra DOUBLE PRECISION,
            precio_venta DOUBLE PRECISION,
            cantidad_disponible_venta DOUBLE PRECISION,
            epoch BIGINT NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        with conn.cursor() as cur:
            cur.execute(create_sql)
        self.logger.info(
            "Tabla destino '%s' verificada", self.config.table_name
        )

    def _load_asset_ids(self) -> Dict[str, int]:
        self.logger.info(
            "Cargando asset_id para EPICs: %s", ", ".join(self.config.target_epics)
        )
        mapping: Dict[str, int] = {}
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT epic, asset_id FROM assets WHERE epic = ANY(%s)",
                (self.config.target_epics,),
            )
            for row in cur.fetchall():
                epic = str(row["epic"]).strip()
                mapping[epic] = int(row["asset_id"])
        missing = sorted(set(self.config.target_epics) - set(mapping))
        if missing:
            raise RuntimeError(
                "Faltan asset_id en la tabla assets para: " + ", ".join(missing)
            )
        self.logger.info("Asset_id cargados para %s EPICs", len(mapping))
        return mapping

    # ------------------------------------------------------------------
    # Ciclo principal
    # ------------------------------------------------------------------
    def run(self) -> None:
        self.logger.info("Iniciando stream de Capital.com")
        while not self.stop_event.is_set():
            try:
                self.authenticate()
                self._run_websocket()
            except Exception as exc:
                if self.stop_event.is_set():
                    break
                self.logger.error(
                    "Fallo en el loop de Capital.com: %s", exc, exc_info=True
                )
                time.sleep(self.config.reconnect_delay_seconds)
        self.logger.info("Stream de Capital.com detenido")

    def authenticate(self) -> None:
        self.logger.info("Autenticando contra Capital.com")
        session = requests.Session()
        session.headers.update(
            {
                "X-CAP-API-KEY": self.config.api_key,
                "Content-Type": "application/json",
            }
        )
        try:
            response = session.post(
                f"{CAPITAL_API_BASE}/session",
                data=json.dumps(
                    {"identifier": self.config.email, "password": self.config.password}
                ),
                timeout=30,
            )
        finally:
            session.close()
        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Autenticación fallida ({response.status_code}): {response.text[:200]}"
            )
        cst = response.headers.get("CST") or response.headers.get("cst")
        security_token = response.headers.get("X-SECURITY-TOKEN") or response.headers.get(
            "x-security-token"
        )
        if not cst or not security_token:
            try:
                body = response.json()
            except Exception:  # pragma: no cover - logging defensivo
                body = {}
            cst = cst or body.get("CST")
            security_token = security_token or body.get("securityToken")
        if not cst or not security_token:
            raise RuntimeError("Autenticación correcta pero sin tokens recibidos")
        self.tokens = {"cst": cst, "securityToken": security_token}
        self.logger.info("Tokens de sesión obtenidos")

    def _run_websocket(self) -> None:
        headers = []
        if self.tokens:
            headers = [
                f"CST: {self.tokens['cst']}",
                f"X-SECURITY-TOKEN: {self.tokens['securityToken']}",
            ]
        ws_app = WebSocketApp(
            CAPITAL_STREAM_URL,
            header=headers,
            on_open=self._on_open,
            on_message=self._on_message,
            on_close=self._on_close,
            on_error=self._on_error,
        )
        with self.ws_lock:
            self.ws_app = ws_app
        self._mark_activity()
        try:
            ws_app.run_forever(ping_interval=None, ping_timeout=None)
        finally:
            self.connected_event.clear()
            with self.ws_lock:
                self.ws_app = None

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------
    def _on_open(self, _: WebSocketApp) -> None:
        self.logger.info("WebSocket de Capital.com conectado")
        self.connected_event.set()
        self._mark_activity()
        self._send_subscribe()

    def _on_close(
        self, _: WebSocketApp, status_code: Optional[int], msg: Optional[str]
    ) -> None:
        self.logger.warning(
            "WebSocket de Capital.com cerrado: status=%s mensaje=%s", status_code, msg
        )
        self.connected_event.clear()

    def _on_error(self, _: WebSocketApp, error: Exception) -> None:
        self.logger.error("Error en WebSocket de Capital.com: %s", error, exc_info=True)
        self.connected_event.clear()

    def _on_message(self, _: WebSocketApp, message: str) -> None:
        self._mark_activity()
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            self.logger.error("JSON inválido recibido: %s", message)
            return

        destination = payload.get("destination")
        status = payload.get("status")
        if status and status != "OK":
            self.logger.warning("Mensaje de estado no OK: %s", payload)

        if destination == "pong":
            return
        if destination != "quote":
            return

        quote = payload.get("payload")
        if not isinstance(quote, dict):
            self.logger.warning("Quote mal formada: %s", payload)
            return

        epic = quote.get("epic")
        if not epic:
            self.logger.warning("Quote sin EPIC: %s", payload)
            return

        asset_id = self.asset_ids.get(epic)
        if asset_id is None:
            self.logger.error("Sin asset_id mapeado para %s", epic)
            return

        epoch_ms = quote.get("timestamp")
        try:
            epoch_ms_int = int(epoch_ms)
        except (TypeError, ValueError):
            self.logger.warning("Timestamp inválido en payload: %s", payload)
            return

        ts = datetime.fromtimestamp(epoch_ms_int / 1000.0, tz=timezone.utc)
        record = {
            "asset_id": asset_id,
            "timestamp": ts,
            "symbol": epic,
            "tipo": payload.get("destination") or "quote",
            "precio_compra": _to_float(quote.get("bid")),
            "cantidad_disponible_compra": _to_float(quote.get("bidQty")),
            "precio_venta": _to_float(quote.get("ofr")),
            "cantidad_disponible_venta": _to_float(quote.get("ofrQty")),
            "epoch": epoch_ms_int,
        }
        if self._persist_record(record):
            self._log_activity(epic, ts)

    # ------------------------------------------------------------------
    # Envío de mensajes
    # ------------------------------------------------------------------
    def _send_subscribe(self) -> None:
        if not self.tokens:
            self.logger.error("No se puede suscribir sin tokens")
            return
        subscribe_message = {
            "destination": "marketData.subscribe",
            "correlationId": f"subscribe-{int(time.time())}",
            "cst": self.tokens["cst"],
            "securityToken": self.tokens["securityToken"],
            "payload": {"epics": self.config.target_epics},
        }
        self._send_json(subscribe_message)
        self.logger.info(
            "Suscripción enviada para: %s", ", ".join(self.config.target_epics)
        )

    def _send_ping(self) -> None:
        if not self.tokens:
            return
        ping_message = {
            "destination": "ping",
            "correlationId": f"ping-{int(time.time())}",
            "cst": self.tokens["cst"],
            "securityToken": self.tokens["securityToken"],
        }
        self._send_json(ping_message)

    def _send_json(self, payload: Dict[str, object]) -> None:
        message = json.dumps(payload)
        with self.ws_lock:
            ws = self.ws_app
        if ws is None:
            return
        try:
            ws.send(message)
        except Exception as exc:
            self.logger.error("Error enviando mensaje: %s", exc, exc_info=True)

    # ------------------------------------------------------------------
    # Actividad y watchdog
    # ------------------------------------------------------------------
    def _ping_loop(self) -> None:
        while not self.stop_event.is_set():
            if not self.connected_event.wait(timeout=1):
                continue
            if self.stop_event.wait(timeout=self.config.ping_interval_seconds):
                break
            if not self.connected_event.is_set():
                continue
            self._send_ping()
        self.logger.info("Hilo de ping detenido")

    def _mark_activity(self) -> None:
        self.last_message_at = time.monotonic()

    def _inactivity_watchdog(self) -> None:
        while not self.stop_event.wait(timeout=5):
            if not self.connected_event.is_set():
                continue
            elapsed = time.monotonic() - self.last_message_at
            if elapsed >= self.config.inactivity_timeout_seconds:
                self.logger.warning(
                    "Sin mensajes de Capital.com durante %.0f s. Forzando reconexión.",
                    elapsed,
                )
                self._force_reconnect()
        self.logger.info("Watchdog de inactividad detenido")

    def _force_reconnect(self) -> None:
        with self.ws_lock:
            ws = self.ws_app
        if ws is None:
            return
        try:
            ws.close()
        except Exception:
            self.logger.exception("Error al cerrar WebSocket durante la reconexión")
        finally:
            self.connected_event.clear()
            self.last_message_at = time.monotonic()

    def _persist_record(self, record: Dict[str, object]) -> bool:
        columns = (
            "asset_id",
            "timestamp",
            "symbol",
            "tipo",
            "precio_compra",
            "cantidad_disponible_compra",
            "precio_venta",
            "cantidad_disponible_venta",
            "epoch",
        )
        values = tuple(record[col] for col in columns)
        insert_sql = f"""
            INSERT INTO {self.config.table_name} ({', '.join(columns)})
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self.db_conn.cursor() as cur:
                cur.execute(insert_sql, values)
            return True
        except Exception as exc:
            self.logger.error(
                "No se pudo insertar quote de %s: %s",
                record["symbol"],
                exc,
                exc_info=True,
            )
            return False

    def _log_activity(self, epic: str, ts: datetime) -> None:
        self.messages_since_log += 1
        self.last_activity_epic = epic
        self.last_activity_ts = ts
        now = time.monotonic()
        if self.messages_since_log == 1:
            self.logger.info(
                "Primer quote recibido: %s a %s",
                epic,
                ts.isoformat(),
            )
            self.last_activity_log = now
            return
        if now - self.last_activity_log >= self.config.activity_log_interval_seconds:
            self.logger.info(
                "Quotes recibidos en los últimos %.0f s: %s (último %s a %s)",
                now - self.last_activity_log,
                self.messages_since_log,
                self.last_activity_epic,
                self.last_activity_ts.isoformat() if self.last_activity_ts else "?",
            )
            self.messages_since_log = 0
            self.last_activity_log = now

    # ------------------------------------------------------------------
    # Parada
    # ------------------------------------------------------------------
    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.logger.info("Solicitando parada del stream de Capital.com")
        self.stop_event.set()
        self.connected_event.set()
        with self.ws_lock:
            if self.ws_app is not None:
                try:
                    self.ws_app.close()
                except Exception:
                    self.logger.exception("Error al cerrar el WebSocket")
        if self.ping_thread.is_alive() and threading.current_thread() is not self.ping_thread:
            self.ping_thread.join(timeout=5)
        if self.watchdog_thread.is_alive() and threading.current_thread() is not self.watchdog_thread:
            self.watchdog_thread.join(timeout=5)
        try:
            self.db_conn.close()
        except Exception:
            self.logger.exception("Error al cerrar la conexión a base de datos")


# ---------------------------------------------------------------------------
# EODHD
# ---------------------------------------------------------------------------


class SettingsError(RuntimeError):
    pass


@dataclass
class EODSettings:
    api_token: str
    symbols_env: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    reconnect_delay: int = 5
    heartbeat: int = 120
    symbol_suffix: str = ".US"
    inactivity_timeout: int = 60
    activity_log_interval: int = 60

    @classmethod
    def from_env(cls) -> "EODSettings":
        missing = [
            name
            for name in (
                "API_TOKEN",
                "SYMBOLS",
                "DB_HOST",
                "DB_PORT",
                "DB_NAME",
                "DB_USER",
                "DB_PASSWORD",
            )
            if not os.getenv(name)
        ]
        if missing:
            raise SettingsError(
                "Faltan variables obligatorias para EODHD: " + ", ".join(missing)
            )

        reconnect_delay = int(os.getenv("RECONNECT_DELAY", "5"))
        heartbeat = int(os.getenv("WS_HEARTBEAT_SECONDS", "120"))
        suffix = os.getenv("EOD_SYMBOL_SUFFIX", ".US")
        inactivity_timeout = int(os.getenv("WS_INACTIVITY_TIMEOUT", "60"))
        activity_log_interval = int(os.getenv("WS_ACTIVITY_LOG_INTERVAL", "60"))

        return cls(
            api_token=os.environ["API_TOKEN"].strip(),
            symbols_env=os.environ["SYMBOLS"].strip(),
            db_host=os.environ["DB_HOST"].strip(),
            db_port=int(os.environ["DB_PORT"].strip()),
            db_name=os.environ["DB_NAME"].strip(),
            db_user=os.environ["DB_USER"].strip(),
            db_password=os.environ["DB_PASSWORD"].strip(),
            reconnect_delay=reconnect_delay,
            heartbeat=heartbeat,
            symbol_suffix=suffix,
            inactivity_timeout=inactivity_timeout,
            activity_log_interval=activity_log_interval,
        )


def parse_symbol_mappings(raw: str, suffix: str) -> List["SymbolMapping"]:
    mappings: List[SymbolMapping] = []
    seen_remote: set[str] = set()
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        parts = [p.strip() for p in item.split(":") if p.strip()]
        if len(parts) != 3:
            raise SettingsError(
                "Formato inválido en SYMBOLS. Use LOCAL:REMOTE:ASSET_ID"
            )
        local_symbol, remote_symbol, asset_id_raw = parts
        try:
            asset_id = int(asset_id_raw)
        except ValueError as exc:
            raise SettingsError(
                f"asset_id inválido para {local_symbol}: {asset_id_raw}"
            ) from exc

        if "." not in remote_symbol and suffix:
            corrected_symbol = f"{remote_symbol}{suffix}"
            logging.getLogger("EODHD_WS").warning(
                "El símbolo %s no incluye mercado. Usando %s",
                remote_symbol,
                corrected_symbol,
            )
            remote_symbol = corrected_symbol

        if remote_symbol in seen_remote:
            raise SettingsError(f"Símbolo remoto duplicado: {remote_symbol}")
        seen_remote.add(remote_symbol)

        mappings.append(SymbolMapping(local_symbol, remote_symbol, asset_id))
    if not mappings:
        raise SettingsError("No se configuraron símbolos para EODHD")
    return mappings


@dataclass(frozen=True)
class SymbolMapping:
    local_symbol: str
    remote_symbol: str
    asset_id: int


class Database:
    def __init__(self, settings: EODSettings) -> None:
        self._pool = SimpleConnectionPool(
            minconn=1,
            maxconn=int(os.getenv("DB_POOL_MAX", "10")),
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()

    def persist_trade(
        self,
        *,
        asset_id: int,
        symbol: str,
        session: str,
        price: float,
        size: int,
        condition_code: int,
        dark_pool: bool,
        market_status: str,
        event_timestamp: datetime,
    ) -> None:
        conn = self._pool.getconn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO trades_real_time (
                            asset_id, symbol, session, price, size, condition_code,
                            dark_pool, market_status, event_timestamp, ingestion_ts
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            asset_id,
                            symbol,
                            session,
                            price,
                            size,
                            condition_code,
                            dark_pool,
                            market_status,
                            event_timestamp,
                        ),
                    )
        finally:
            self._pool.putconn(conn)

    def persist_quote(
        self,
        *,
        asset_id: int,
        symbol: str,
        session: str,
        bid_price: Optional[float],
        ask_price: Optional[float],
        bid_size: int,
        ask_size: int,
        event_timestamp: datetime,
    ) -> None:
        conn = self._pool.getconn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO quotes_real_time (
                            asset_id, symbol, session, bid_price, ask_price,
                            bid_size, ask_size, event_timestamp, ingestion_ts
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            asset_id,
                            symbol,
                            session,
                            bid_price,
                            ask_price,
                            bid_size,
                            ask_size,
                            event_timestamp,
                        ),
                    )
        finally:
            self._pool.putconn(conn)


def market_session(now_utc: Optional[dtime] = None) -> str:
    current = now_utc or datetime.now(timezone.utc).time()
    if dtime(8, 0) <= current < dtime(13, 30):
        return "pre-market"
    if dtime(13, 30) <= current < dtime(20, 0):
        return "market-time"
    if dtime(20, 0) <= current < dtime(23, 59):
        return "post-market"
    return "overnight"


def timestamp_from_ms(ms: Optional[int]) -> datetime:
    if ms is None:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)


class WebSocketWorker:
    def __init__(
        self,
        *,
        name: str,
        url: str,
        settings: EODSettings,
        database: Database,
        mappings: Dict[str, SymbolMapping],
        payload_symbols: str,
        stop_event: asyncio.Event,
    ) -> None:
        self.name = name
        self.url = url
        self.settings = settings
        self.database = database
        self.mappings = mappings
        self.payload_symbols = payload_symbols
        self.stop_event = stop_event
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_message_at = time.monotonic()
        self._last_activity_log = (
            time.monotonic() - settings.activity_log_interval
        )
        self._messages_since_log = 0
        self._last_symbol: Optional[str] = None
        self._last_ts: Optional[datetime] = None
        self.logger = logging.getLogger("EODHD_WS")

    async def start(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self._run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.stop_event.is_set():
                    break
                self.logger.error(
                    "Error en cliente %s: %s. Reintentando en %ss",
                    self.name,
                    exc,
                    self.settings.reconnect_delay,
                    exc_info=True,
                )
            if self.stop_event.wait(self.settings.reconnect_delay):
                break
        self.logger.info("Cliente %s detenido", self.name)

    async def stop(self) -> None:
        self.stop_event.set()
        if self._session is not None:
            await self._session.close()

    async def _run_once(self) -> None:
        payload = {
            "action": "subscribe",
            "symbols": self.payload_symbols,
            "api_token": self.settings.api_token,
        }
        async with aiohttp.ClientSession() as session:
            self._session = session
            try:
                async with session.ws_connect(
                    self.url,
                    heartbeat=self.settings.heartbeat,
                    receive_timeout=self.settings.heartbeat + 30,
                ) as ws:
                    self.logger.info("Conectado a %s", self.name)
                    await ws.send_json(payload)
                    self._last_message_at = time.monotonic()
                    while not self.stop_event.is_set():
                        try:
                            msg = await ws.receive(timeout=self.settings.inactivity_timeout)
                        except asyncio.TimeoutError:
                            elapsed = time.monotonic() - self._last_message_at
                            self.logger.warning(
                                "Sin mensajes de %s durante %.0f s. Reiniciando conexión.",
                                self.name,
                                elapsed,
                            )
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._last_message_at = time.monotonic()
                            await self._handle_text(msg.data)
                        elif msg.type == aiohttp.WSMsgType.PING:
                            self._last_message_at = time.monotonic()
                            await ws.pong()
                        elif msg.type == aiohttp.WSMsgType.PONG:
                            self._last_message_at = time.monotonic()
                        elif msg.type in {
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSED,
                        }:
                            self.logger.warning(
                                "WebSocket %s cerrado por el servidor", self.name
                            )
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError(f"WebSocket error: {ws.exception()}")
            finally:
                self._session = None

    async def _handle_text(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.logger.error("Mensaje no es JSON válido en %s: %s", self.name, raw)
            return

        if isinstance(data, dict) and "status" in data and "s" not in data:
            status = str(data.get("status", "")).upper()
            message = data.get("message", "")
            if status not in {"OK", "200"}:
                raise RuntimeError(f"Suscripción rechazada ({status}): {message}")
            self.logger.info("Respuesta de control %s: %s", self.name, message or status)
            return

        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict):
                    await self.process_payload(entry)
            return

        if isinstance(data, dict):
            await self.process_payload(data)

    async def process_payload(self, data: Dict[str, object]) -> None:
        raise NotImplementedError

    def _register_activity(self, symbol: str, event_ts: datetime) -> None:
        self._messages_since_log += 1
        self._last_symbol = symbol
        self._last_ts = event_ts
        now = time.monotonic()
        if self._messages_since_log == 1:
            self.logger.info(
                "Primer mensaje %s: %s a %s",
                self.name,
                symbol,
                event_ts.isoformat(),
            )
            self._last_activity_log = now
            return
        if now - self._last_activity_log >= self.settings.activity_log_interval:
            self.logger.info(
                "%s procesó %s mensajes en %.0f s (último %s a %s)",
                self.name,
                self._messages_since_log,
                now - self._last_activity_log,
                self._last_symbol,
                self._last_ts.isoformat() if self._last_ts else "?",
            )
            self._messages_since_log = 0
            self._last_activity_log = now


class TradesWorker(WebSocketWorker):
    async def process_payload(self, data: Dict[str, object]) -> None:
        symbol_remote = data.get("s")
        price = data.get("p")
        if symbol_remote is None or price is None:
            return
        mapping = self.mappings.get(str(symbol_remote))
        if not mapping:
            self.logger.warning(
                "Trade recibido para símbolo no mapeado: %s", symbol_remote
            )
            return

        volume_raw = data.get("v", 0)
        try:
            size = int(volume_raw)
        except (TypeError, ValueError):
            size = 0

        condition_raw = data.get("c")
        condition_code = 0
        if isinstance(condition_raw, list) and condition_raw:
            try:
                condition_code = int(condition_raw[0])
            except (TypeError, ValueError):
                condition_code = 0
        elif condition_raw is not None:
            try:
                condition_code = int(condition_raw)
            except (TypeError, ValueError):
                condition_code = 0

        dark_pool = bool(data.get("dp", False))
        market_status = str(data.get("e", "unknown"))
        event_ts = timestamp_from_ms(data.get("t"))

        try:
            await asyncio.to_thread(
                self.database.persist_trade,
                asset_id=mapping.asset_id,
                symbol=mapping.local_symbol,
                session=market_session(),
                price=float(price),
                size=size,
                condition_code=condition_code,
                dark_pool=dark_pool,
                market_status=market_status,
                event_timestamp=event_ts,
            )
        except Exception as exc:
            self.logger.error(
                "No se pudo insertar trade %s: %s",
                mapping.local_symbol,
                exc,
                exc_info=True,
            )
            return

        self._register_activity(mapping.local_symbol, event_ts)


class QuotesWorker(WebSocketWorker):
    async def process_payload(self, data: Dict[str, object]) -> None:
        symbol_remote = data.get("s")
        if symbol_remote is None:
            return
        mapping = self.mappings.get(str(symbol_remote))
        if not mapping:
            self.logger.warning(
                "Quote recibido para símbolo no mapeado: %s", symbol_remote
            )
            return

        bid_price = data.get("bp")
        ask_price = data.get("ap")
        bid_size_raw = data.get("bs", 0)
        ask_size_raw = data.get("as", 0)
        try:
            bid_size = int(bid_size_raw)
        except (TypeError, ValueError):
            bid_size = 0
        try:
            ask_size = int(ask_size_raw)
        except (TypeError, ValueError):
            ask_size = 0

        event_ts = timestamp_from_ms(data.get("t"))

        try:
            await asyncio.to_thread(
                self.database.persist_quote,
                asset_id=mapping.asset_id,
                symbol=mapping.local_symbol,
                session=market_session(),
                bid_price=float(bid_price) if bid_price is not None else None,
                ask_price=float(ask_price) if ask_price is not None else None,
                bid_size=bid_size,
                ask_size=ask_size,
                event_timestamp=event_ts,
            )
        except Exception as exc:
            self.logger.error(
                "No se pudo insertar quote %s: %s",
                mapping.local_symbol,
                exc,
                exc_info=True,
            )
            return

        self._register_activity(mapping.local_symbol, event_ts)


async def run_eodhd(settings: EODSettings, stop_event: asyncio.Event) -> None:
    mappings_list = parse_symbol_mappings(settings.symbols_env, settings.symbol_suffix)
    mappings = {m.remote_symbol: m for m in mappings_list}
    payload_symbols = ",".join(m.remote_symbol for m in mappings_list)
    logging.getLogger("EODHD_WS").info(
        "Suscribiendo símbolos remotos: %s", payload_symbols
    )

    database = Database(settings)

    trades_url = (
        f"wss://ws.eodhistoricaldata.com/ws/us?api_token={settings.api_token}"
    )
    quotes_url = (
        f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={settings.api_token}"
    )

    trades_worker = TradesWorker(
        name="TRADES",
        url=trades_url,
        settings=settings,
        database=database,
        mappings=mappings,
        payload_symbols=payload_symbols,
        stop_event=stop_event,
    )
    quotes_worker = QuotesWorker(
        name="QUOTES",
        url=quotes_url,
        settings=settings,
        database=database,
        mappings=mappings,
        payload_symbols=payload_symbols,
        stop_event=stop_event,
    )

    worker_tasks = [
        asyncio.create_task(trades_worker.start(), name="EODHD_TRADES"),
        asyncio.create_task(quotes_worker.start(), name="EODHD_QUOTES"),
    ]

    def _on_task_done(task: asyncio.Task) -> None:
        if stop_event.is_set():
            return
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logging.getLogger("EODHD_WS").error(
                "Tarea %s finalizó con error: %s",
                task.get_name(),
                exc,
                exc_info=True,
            )
        else:
            logging.getLogger("EODHD_WS").warning(
                "Tarea %s finalizó sin error pero antes de tiempo", task.get_name()
            )
        stop_event.set()

    for task in worker_tasks:
        task.add_done_callback(_on_task_done)

    try:
        await stop_event.wait()
    finally:
        for task in worker_tasks:
            task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        database.close()


# ---------------------------------------------------------------------------
# Orquestador
# ---------------------------------------------------------------------------

async def run_all() -> None:
    load_environment(Path(".env"))
    configure_logging()

    SUPERVISOR_LOGGER.info("Leyendo configuración de entorno")
    capital_config = CapitalConfig.from_env()
    eod_settings = EODSettings.from_env()

    capital_streamer = CapitalWebSocketStreamer(capital_config)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    def _request_stop(signame: str) -> None:
        SUPERVISOR_LOGGER.warning("Señal %s recibida. Deteniendo servicios...", signame)
        stop_event.set()
        capital_streamer.stop()

    for signame in ("SIGTERM", "SIGINT"):
        signum = getattr(signal, signame, None)
        if signum is None:
            continue
        try:
            loop.add_signal_handler(signum, _request_stop, signame)
        except NotImplementedError:
            def _handler(sig: int, frame, name: str = signame) -> None:  # type: ignore[override]
                SUPERVISOR_LOGGER.warning(
                    "Señal %s recibida (fallback). Deteniendo servicios...",
                    name,
                )
                loop.call_soon_threadsafe(stop_event.set)
                capital_streamer.stop()

            signal.signal(signum, _handler)

    async def _run_capital() -> None:
        try:
            await asyncio.to_thread(capital_streamer.run)
        finally:
            stop_event.set()

    capital_task = asyncio.create_task(_run_capital(), name="CapitalStream")
    eod_task = asyncio.create_task(run_eodhd(eod_settings, stop_event), name="EODHD")

    def _on_task_done(task: asyncio.Task) -> None:
        if stop_event.is_set():
            return
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            SUPERVISOR_LOGGER.error(
                "Tarea %s finalizó con error: %s",
                task.get_name(),
                exc,
                exc_info=True,
            )
        else:
            SUPERVISOR_LOGGER.warning(
                "Tarea %s finalizó inesperadamente", task.get_name()
            )
        stop_event.set()
        capital_streamer.stop()

    capital_task.add_done_callback(_on_task_done)
    eod_task.add_done_callback(_on_task_done)

    try:
        await stop_event.wait()
    finally:
        capital_streamer.stop()
        capital_task.cancel()
        eod_task.cancel()
        await asyncio.gather(capital_task, eod_task, return_exceptions=True)


def main() -> None:
    try:
        asyncio.run(run_all())
    except SettingsError as exc:
        SUPERVISOR_LOGGER.critical("Configuración de EODHD inválida: %s", exc)
        sys.exit(1)
    except RuntimeError as exc:
        SUPERVISOR_LOGGER.critical("Error crítico en Capital.com: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        SUPERVISOR_LOGGER.info("Ejecución interrumpida por el usuario")
    except Exception as exc:
        SUPERVISOR_LOGGER.critical("Fallo crítico: %s", exc, exc_info=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Utilidades auxiliares
# ---------------------------------------------------------------------------

def _to_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    main()
