"""Unified streaming service for Capital.com and EODHD WebSocket feeds."""

from __future__ import annotations

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
except ImportError as exc:  # pragma: no cover - dependency runtime check
    raise ImportError("The 'websocket-client' package is required to run this module") from exc

CAPITAL_API_BASE = "https://api-capital.backend-capital.com/api/v1"
CAPITAL_STREAM_URL = "wss://api-streaming-capital.backend-capital.com/connect"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def configure_logging() -> None:
    """Configure root logging so every module shares the same formatting."""

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


def load_environment(env_path: Optional[Path] = None) -> None:
    """Load environment variables from a .env file without overriding existing values."""

    path = env_path or Path(".env")
    if path.exists():
        load_dotenv(path, override=False)


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
                f"Missing Capital.com credentials in environment: {', '.join(missing)}"
            )

        db_missing = [
            key
            for key in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD")
            if not os.environ.get(key)
        ]
        if db_missing:
            raise RuntimeError(
                "Missing PostgreSQL configuration in environment: "
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
            raise RuntimeError("No EPICs configured to subscribe")

        return cfg


class CapitalWebSocketStreamer:
    """Capital.com WebSocket streamer with automatic reconnection and persistence."""

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
        self.last_message_at = time.monotonic()
        self.last_activity_log = time.monotonic()
        self.processed_messages = 0
        self.watchdog_thread = threading.Thread(
            target=self._inactivity_watchdog,
            name="CapitalInactivityWatchdog",
            daemon=True,
        )
        self.watchdog_thread.start()

    def _create_db_connection(self) -> psycopg2.extensions.connection:
        self.logger.info(
            "Connecting to PostgreSQL %s:%s", self.config.db_host, self.config.db_port
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
        self.logger.info("Ensured destination table '%s' exists", self.config.table_name)

    def _load_asset_ids(self) -> Dict[str, int]:
        self.logger.info(
            "Loading asset ids for EPICs: %s", ", ".join(self.config.target_epics)
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
                "Missing asset_id mappings for EPICs in assets table: "
                + ", ".join(missing)
            )
        self.logger.info("Loaded asset ids for %d EPICs", len(mapping))
        return mapping

    def authenticate(self) -> None:
        self.logger.info("Authenticating against Capital.com API")
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
                f"Authentication failed with status {response.status_code}: {response.text[:200]}"
            )
        cst = response.headers.get("CST") or response.headers.get("cst")
        security_token = response.headers.get("X-SECURITY-TOKEN") or response.headers.get(
            "x-security-token"
        )
        if not cst or not security_token:
            body: Dict[str, object]
            try:
                body = response.json()
            except Exception:  # pragma: no cover - defensive logging
                body = {}
            cst = cst or body.get("CST")  # type: ignore[assignment]
            security_token = security_token or body.get("securityToken")  # type: ignore[assignment]
        if not cst or not security_token:
            raise RuntimeError("Authentication succeeded but tokens were not received")
        self.tokens = {"cst": cst, "securityToken": security_token}
        self.logger.info("Authentication successful, tokens acquired")

    def run(self) -> None:
        self.logger.info("Starting streaming loop")
        while not self.stop_event.is_set():
            try:
                self.authenticate()
                self._run_websocket()
            except Exception as exc:
                if self.stop_event.is_set():
                    break
                self.logger.exception("Error in streaming loop: %s", exc)
                time.sleep(self.config.reconnect_delay_seconds)
        self.logger.info("Streaming loop stopped")

    def _run_websocket(self) -> None:
        headers = []
        if self.tokens:
            headers = [
                f"CST: {self.tokens['cst']}",
                f"X-SECURITY-TOKEN: {self.tokens['securityToken']}",
            ]
        self.logger.info("Connecting to WebSocket stream")
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

    def _on_open(self, ws: WebSocketApp) -> None:  # pragma: no cover - callback
        self.logger.info("WebSocket connection established")
        self.connected_event.set()
        self._mark_activity()
        self._send_subscribe()

    def _on_close(
        self, ws: WebSocketApp, status_code: Optional[int], msg: Optional[str]
    ) -> None:  # pragma: no cover - callback
        self.logger.warning("WebSocket closed: status=%s message=%s", status_code, msg)
        self.connected_event.clear()

    def _on_error(self, ws: WebSocketApp, error: Exception) -> None:  # pragma: no cover
        self.logger.error("WebSocket error: %s", error, exc_info=True)
        self.connected_event.clear()

    def _on_message(self, ws: WebSocketApp, message: str) -> None:  # pragma: no cover
        self._mark_activity()
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON received: %s", message)
            return

        destination = payload.get("destination")
        status = payload.get("status")
        if status and status != "OK":
            self.logger.warning("Received non-OK status message: %s", payload)

        if destination == "pong":
            self.logger.debug("Received pong response")
            return
        if destination != "quote":
            self.logger.debug("Ignoring non-quote message: %s", destination)
            return

        quote = payload.get("payload")
        if not isinstance(quote, dict):
            self.logger.warning("Quote payload malformed: %s", payload)
            return

        epic = quote.get("epic")
        if not epic:
            self.logger.warning("Quote payload without epic: %s", payload)
            return

        asset_id = self.asset_ids.get(epic)
        if asset_id is None:
            self.logger.error("No asset_id mapping for epic %s", epic)
            return

        epoch_ms = quote.get("timestamp")
        if epoch_ms is None:
            self.logger.warning("Quote payload missing timestamp: %s", payload)
            return

        try:
            epoch_ms_int = int(epoch_ms)
        except (TypeError, ValueError):
            self.logger.warning("Invalid timestamp in payload: %s", payload)
            return

        ts = datetime.fromtimestamp(epoch_ms_int / 1000.0, tz=timezone.utc)
        record = {
            "asset_id": asset_id,
            "timestamp": ts,
            "symbol": epic,
            "tipo": payload.get("destination") or "quote",
            "precio_compra": _safe_float(quote.get("bid")),
            "cantidad_disponible_compra": _safe_float(quote.get("bidQty")),
            "precio_venta": _safe_float(quote.get("ofr")),
            "cantidad_disponible_venta": _safe_float(quote.get("ofrQty")),
            "epoch": epoch_ms_int,
        }
        self._persist_record(record)
        self._register_activity(record)

    def _register_activity(self, record: Dict[str, object]) -> None:
        self.processed_messages += 1
        now = time.monotonic()
        description = (
            f"{record['symbol']} bid={_format_price(record['precio_compra'])} "
            f"ask={_format_price(record['precio_venta'])}"
        )
        if self.processed_messages == 1:
            self.logger.info("Primer quote recibido -> %s", description)
            self.last_activity_log = now
            return
        if now - self.last_activity_log >= self.config.activity_log_interval_seconds:
            self.logger.info(
                "Quotes procesadas: %s. Última -> %s",
                self.processed_messages,
                description,
            )
            self.last_activity_log = now

    def _send_subscribe(self) -> None:
        if not self.tokens:
            self.logger.error("Cannot subscribe without tokens")
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
            "Subscription message sent for EPICs: %s", ", ".join(self.config.target_epics)
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
        self.logger.debug("Ping sent")

    def _send_json(self, payload: Dict[str, object]) -> None:
        message = json.dumps(payload)
        with self.ws_lock:
            ws = self.ws_app
            if ws is None:
                self.logger.debug("WebSocket not available for sending message")
                return
            try:
                ws.send(message)
            except Exception as exc:
                self.logger.error("Failed to send message: %s", exc, exc_info=True)

    def _ping_loop(self) -> None:
        while not self.stop_event.is_set():
            if not self.connected_event.wait(timeout=1):
                continue
            if self.stop_event.wait(timeout=self.config.ping_interval_seconds):
                break
            if not self.connected_event.is_set():
                continue
            self._send_ping()
        self.logger.info("Ping thread terminated")

    def _mark_activity(self) -> None:
        self.last_message_at = time.monotonic()

    def _inactivity_watchdog(self) -> None:
        while not self.stop_event.wait(timeout=5):
            if not self.connected_event.is_set():
                continue
            elapsed = time.monotonic() - self.last_message_at
            if elapsed >= self.config.inactivity_timeout_seconds:
                self.logger.warning(
                    "No messages received in %.0f seconds. Forcing reconnection.",
                    elapsed,
                )
                self._force_reconnect()

    def _force_reconnect(self) -> None:
        with self.ws_lock:
            ws = self.ws_app
        if ws is None:
            return
        try:
            ws.close()
        except Exception:
            self.logger.exception("Error closing WebSocket during inactivity restart")
        finally:
            self.connected_event.clear()
            self.last_message_at = time.monotonic()

    def _persist_record(self, record: Dict[str, object]) -> None:
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
            self.logger.debug(
                "Stored quote %s at %s (asset_id=%s)",
                record["symbol"],
                record["timestamp"].isoformat(),
                record["asset_id"],
            )
        except Exception as exc:
            self.logger.error("Failed to persist record: %s", exc, exc_info=True)

    def stop(self) -> None:
        self.stop_event.set()
        self.connected_event.set()
        with self.ws_lock:
            if self.ws_app is not None:
                try:
                    self.ws_app.close()
                except Exception:
                    self.logger.exception("Error closing WebSocket")
        if (
            self.ping_thread.is_alive()
            and threading.current_thread() is not self.ping_thread
        ):
            self.ping_thread.join(timeout=5)
        if (
            self.watchdog_thread.is_alive()
            and threading.current_thread() is not self.watchdog_thread
        ):
            self.watchdog_thread.join(timeout=5)
        if self.db_conn:
            try:
                self.db_conn.close()
            except Exception:
                self.logger.exception("Error closing database connection")


@dataclass(frozen=True)
class SymbolMapping:
    local_symbol: str
    remote_symbol: str
    asset_id: int


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
                "Faltan variables de entorno obligatorias: " + ", ".join(missing)
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


def parse_symbol_mappings(raw: str, suffix: str) -> List[SymbolMapping]:
    mappings: List[SymbolMapping] = []
    seen_remote: set[str] = set()
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        parts = [p.strip() for p in item.split(":") if p.strip()]
        if len(parts) != 3:
            raise SettingsError(
                "Formato inválido en SYMBOLS. Esperado LOCAL:REMOTE:ASSET_ID"
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
                "El símbolo %s no incluye mercado. Usando %s para suscripción",
                remote_symbol,
                corrected_symbol,
            )
            remote_symbol = corrected_symbol

        if remote_symbol in seen_remote:
            raise SettingsError(f"Símbolo remoto duplicado: {remote_symbol}")
        seen_remote.add(remote_symbol)

        mappings.append(SymbolMapping(local_symbol, remote_symbol, asset_id))
    if not mappings:
        raise SettingsError("No se configuraron símbolos para la suscripción")
    return mappings


class Database:
    def __init__(self, settings: EODSettings) -> None:
        self.logger = logging.getLogger("EODHD_WS")
        self._pool = SimpleConnectionPool(
            minconn=1,
            maxconn=int(os.getenv("DB_POOL_MAX", "10")),
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )
        self._ensure_tables()

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()

    def _ensure_tables(self) -> None:
        create_trades = """
        CREATE TABLE IF NOT EXISTS trades_real_time (
            id BIGSERIAL PRIMARY KEY,
            asset_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            session TEXT NOT NULL,
            price DOUBLE PRECISION,
            size BIGINT,
            condition_code INTEGER,
            dark_pool BOOLEAN,
            market_status TEXT,
            event_timestamp TIMESTAMPTZ NOT NULL,
            ingestion_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        create_quotes = """
        CREATE TABLE IF NOT EXISTS quotes_real_time (
            id BIGSERIAL PRIMARY KEY,
            asset_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            session TEXT NOT NULL,
            bid_price DOUBLE PRECISION,
            ask_price DOUBLE PRECISION,
            bid_size BIGINT,
            ask_size BIGINT,
            event_timestamp TIMESTAMPTZ NOT NULL,
            ingestion_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        conn = self._pool.getconn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(create_trades)
                    cur.execute(create_quotes)
        finally:
            self._pool.putconn(conn)

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
    if ms:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return datetime.now(timezone.utc)


def _safe_float(value: Optional[object]) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: Optional[object]) -> int:
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def _format_price(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return "N/A"


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
    ) -> None:
        self.name = name
        self.url = url
        self.settings = settings
        self.database = database
        self.mappings = mappings
        self.payload_symbols = payload_symbols
        self.logger = logging.getLogger("EODHD_WS")
        self._session: Optional[aiohttp.ClientSession] = None
        self._closing = asyncio.Event()
        self._last_message_at = time.monotonic()
        self._processed_messages = 0
        self._last_activity_log = time.monotonic()

    async def start(self) -> None:
        while not self._closing.is_set():
            try:
                await self._run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.error(
                    "Error en cliente %s: %s. Reintentando en %ss",
                    self.name,
                    exc,
                    self.settings.reconnect_delay,
                    exc_info=True,
                )
            await asyncio.sleep(self.settings.reconnect_delay)

    async def stop(self) -> None:
        self._closing.set()
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
                    while not self._closing.is_set():
                        try:
                            msg = await ws.receive(timeout=self.settings.inactivity_timeout)
                        except asyncio.TimeoutError:
                            elapsed = time.monotonic() - self._last_message_at
                            self.logger.warning(
                                "Sin mensajes de %s durante %ss (%.0fs reales). Reiniciando conexión.",
                                self.name,
                                self.settings.inactivity_timeout,
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
                        elif msg.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED}:
                            self.logger.warning(
                                "WebSocket %s cerrado por el servidor", self.name
                            )
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError(f"WebSocket error: {ws.exception()}")
                        else:
                            self.logger.debug(
                                "Mensaje %s ignorado en %s", msg.type, self.name
                            )
            finally:
                self._session = None

    async def _handle_text(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.logger.error("Mensaje no es JSON válido en %s: %s", self.name, raw)
            return

        if isinstance(data, dict) and "status" in data and "s" not in data:
            status = data.get("status")
            message = data.get("message", "")
            status_text = str(status).upper()
            if status_text not in {"OK", "200"}:
                raise RuntimeError(f"Suscripción rechazada ({status}): {message}")
            self.logger.info(
                "Respuesta de control %s: %s", self.name, message or status
            )
            return

        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict):
                    await self.process_payload(entry)
            return

        if not isinstance(data, dict):
            self.logger.debug("Mensaje ignorado en %s: %s", self.name, data)
            return

        await self.process_payload(data)

    async def process_payload(self, data: Dict[str, object]) -> None:
        raise NotImplementedError

    def _register_activity(self, description: str) -> None:
        self._processed_messages += 1
        now = time.monotonic()
        if self._processed_messages == 1:
            self.logger.info("%s: primer mensaje recibido -> %s", self.name, description)
            self._last_activity_log = now
            return
        if now - self._last_activity_log >= self.settings.activity_log_interval:
            self.logger.info(
                "%s: %s mensajes procesados. Último -> %s",
                self.name,
                self._processed_messages,
                description,
            )
            self._last_activity_log = now


class TradesWorker(WebSocketWorker):
    async def process_payload(self, data: Dict[str, object]) -> None:
        symbol_remote = data.get("s")
        price = data.get("p")
        if not symbol_remote or price is None:
            self.logger.debug("Mensaje de trade ignorado: %s", data)
            return
        mapping = self.mappings.get(str(symbol_remote))
        if not mapping:
            self.logger.warning(
                "Trade recibido para símbolo no mapeado: %s", symbol_remote
            )
            return

        try:
            price_value = float(price)
        except (TypeError, ValueError):
            self.logger.warning(
                "Trade con precio inválido para %s: %s", symbol_remote, price
            )
            return

        size = _safe_int(data.get("v"))
        condition_raw = data.get("c")
        condition_code = 0
        if isinstance(condition_raw, list) and condition_raw:
            condition_code = _safe_int(condition_raw[0])
        elif condition_raw is not None:
            condition_code = _safe_int(condition_raw)

        dark_pool = bool(data.get("dp", False))
        market_status = str(data.get("e", "unknown"))
        event_ts = timestamp_from_ms(data.get("t"))

        try:
            await asyncio.to_thread(
                self.database.persist_trade,
                asset_id=mapping.asset_id,
                symbol=mapping.local_symbol,
                session=market_session(),
                price=price_value,
                size=size,
                condition_code=condition_code,
                dark_pool=dark_pool,
                market_status=market_status,
                event_timestamp=event_ts,
            )
            self._register_activity(
                f"{mapping.local_symbol} {price_value:.4f} x {size}"
            )
        except Exception as exc:
            self.logger.error(
                "No se pudo insertar trade %s en base de datos: %s",
                mapping.local_symbol,
                exc,
                exc_info=True,
            )


class QuotesWorker(WebSocketWorker):
    async def process_payload(self, data: Dict[str, object]) -> None:
        symbol_remote = data.get("s")
        if not symbol_remote:
            self.logger.debug("Mensaje de quote sin símbolo: %s", data)
            return
        mapping = self.mappings.get(str(symbol_remote))
        if not mapping:
            self.logger.warning(
                "Quote recibido para símbolo no mapeado: %s", symbol_remote
            )
            return

        bid_price = _safe_float(data.get("bp"))
        ask_price = _safe_float(data.get("ap"))
        bid_size = _safe_int(data.get("bs"))
        ask_size = _safe_int(data.get("as"))
        event_ts = timestamp_from_ms(data.get("t"))

        try:
            await asyncio.to_thread(
                self.database.persist_quote,
                asset_id=mapping.asset_id,
                symbol=mapping.local_symbol,
                session=market_session(),
                bid_price=bid_price,
                ask_price=ask_price,
                bid_size=bid_size,
                ask_size=ask_size,
                event_timestamp=event_ts,
            )
            self._register_activity(
                f"{mapping.local_symbol} bid={_format_price(bid_price)} "
                f"ask={_format_price(ask_price)}"
            )
        except Exception as exc:
            self.logger.error(
                "No se pudo insertar quote %s en base de datos: %s",
                mapping.local_symbol,
                exc,
                exc_info=True,
            )


async def run_eodhd(
    settings: EODSettings,
    *,
    stop_event: Optional[asyncio.Event] = None,
    register_signals: bool = True,
) -> None:
    logger = logging.getLogger("EODHD_WS")
    mappings_list = parse_symbol_mappings(settings.symbols_env, settings.symbol_suffix)
    mappings = {m.remote_symbol: m for m in mappings_list}
    payload_symbols = ",".join(m.remote_symbol for m in mappings_list)
    logger.info("Suscribiendo símbolos remotos: %s", payload_symbols)

    database = Database(settings)

    trades_url = f"wss://ws.eodhistoricaldata.com/ws/us?api_token={settings.api_token}"
    quotes_url = f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={settings.api_token}"

    trades_worker = TradesWorker(
        name="TRADES",
        url=trades_url,
        settings=settings,
        database=database,
        mappings=mappings,
        payload_symbols=payload_symbols,
    )
    quotes_worker = QuotesWorker(
        name="QUOTES",
        url=quotes_url,
        settings=settings,
        database=database,
        mappings=mappings,
        payload_symbols=payload_symbols,
    )

    shutdown_event = stop_event or asyncio.Event()

    def _notify_stop(signame: str) -> None:
        if shutdown_event.is_set():
            return
        logger.warning("Señal %s recibida. Cerrando...", signame)
        shutdown_event.set()

    def _sync_signal_handler(signum: int, frame) -> None:  # type: ignore[override]
        try:
            signame = signal.Signals(signum).name
        except Exception:
            signame = str(signum)
        _notify_stop(signame)

    if register_signals:
        loop = asyncio.get_running_loop()
        for signame in ("SIGTERM", "SIGINT"):
            if not hasattr(signal, signame):
                continue
            sig = getattr(signal, signame)
            try:
                loop.add_signal_handler(sig, _notify_stop, signame)
            except NotImplementedError:
                logger.info(
                    "add_signal_handler no disponible en esta plataforma. Usando signal.signal para %s",
                    signame,
                )
                signal.signal(sig, _sync_signal_handler)
            except RuntimeError as exc:
                logger.warning(
                    "No se pudo registrar handler asíncrono para %s (%s). Usando signal.signal",
                    signame,
                    exc,
                )
                signal.signal(sig, _sync_signal_handler)

    async def _wait_for_shutdown() -> None:
        await shutdown_event.wait()

    try:
        await asyncio.gather(
            trades_worker.start(),
            quotes_worker.start(),
            _wait_for_shutdown(),
        )
    finally:
        shutdown_event.set()
        await asyncio.gather(
            trades_worker.stop(),
            quotes_worker.stop(),
            return_exceptions=True,
        )
        database.close()


async def run_all() -> None:
    """Run Capital.com and EODHD streams in parallel until shutdown."""

    load_environment()
    configure_logging()
    logger = logging.getLogger("UnifiedStreamer")

    try:
        capital_config = CapitalConfig.from_env()
    except Exception as exc:
        logger.critical(
            "No se pudo cargar configuración de Capital.com: %s", exc, exc_info=True
        )
        raise

    try:
        eod_settings = EODSettings.from_env()
    except Exception as exc:
        logger.critical("No se pudo cargar configuración de EODHD: %s", exc, exc_info=True)
        raise

    capital_streamer = CapitalWebSocketStreamer(capital_config)

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def request_shutdown(reason: str) -> None:
        if shutdown_event.is_set():
            return
        logger.warning("Cierre solicitado (%s)", reason)
        loop.call_soon_threadsafe(shutdown_event.set)

    def capital_thread_main() -> None:
        capital_logger = logging.getLogger("CapitalWebSocket")
        capital_logger.info("Capital.com streaming iniciado")
        try:
            capital_streamer.run()
        except Exception:
            capital_logger.exception("Capital.com streaming terminó con error")
            request_shutdown("capital_stream_error")
        finally:
            if not capital_streamer.stop_event.is_set():
                capital_streamer.stop()
            capital_logger.info("Capital.com streaming detenido")

    capital_thread = threading.Thread(
        target=capital_thread_main,
        name="CapitalStreamThread",
        daemon=True,
    )
    capital_thread.start()

    def _signal_handler(signame: str) -> None:
        request_shutdown(f"signal {signame}")

    def _sync_signal_handler(signum: int, frame) -> None:  # type: ignore[override]
        try:
            signame = signal.Signals(signum).name
        except Exception:
            signame = str(signum)
        _signal_handler(signame)

    for signame in ("SIGTERM", "SIGINT"):
        if not hasattr(signal, signame):
            continue
        sig = getattr(signal, signame)
        try:
            loop.add_signal_handler(sig, _signal_handler, signame)
        except NotImplementedError:
            logger.info(
                "add_signal_handler no disponible para %s. Usando signal.signal.",
                signame,
            )
            signal.signal(sig, _sync_signal_handler)
        except RuntimeError as exc:
            logger.warning(
                "No se pudo registrar handler asíncrono para %s (%s). Usando signal.signal.",
                signame,
                exc,
            )
            signal.signal(sig, _sync_signal_handler)

    eod_task = asyncio.create_task(
        run_eodhd(eod_settings, stop_event=shutdown_event, register_signals=False),
        name="EODHDService",
    )

    async def monitor_capital_thread() -> None:
        while not shutdown_event.is_set():
            if not capital_thread.is_alive():
                logger.warning("Capital.com streaming thread finalizó inesperadamente")
                request_shutdown("capital_thread_exit")
                return
            await asyncio.sleep(5)

    async def stop_capital_on_shutdown() -> None:
        await shutdown_event.wait()
        logger.info("Deteniendo Capital.com streaming...")
        if not capital_streamer.stop_event.is_set():
            capital_streamer.stop()
        await asyncio.to_thread(capital_thread.join, 15)
        logger.info("Capital.com streaming detenido por apagado")

    capital_monitor_task = asyncio.create_task(
        monitor_capital_thread(),
        name="CapitalMonitor",
    )
    capital_shutdown_task = asyncio.create_task(
        stop_capital_on_shutdown(),
        name="CapitalShutdown",
    )

    try:
        await eod_task
    except Exception:
        request_shutdown("eodhd_error")
        raise
    finally:
        request_shutdown("shutdown_finalize")
        await asyncio.gather(
            capital_monitor_task,
            capital_shutdown_task,
            return_exceptions=True,
        )
        if capital_thread.is_alive():
            await asyncio.to_thread(capital_thread.join, 5)
        logger.info("Servicios de streaming finalizados")


def main() -> None:
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        logging.getLogger("UnifiedStreamer").info("Ejecución interrumpida por el usuario")
    except Exception as exc:
        logging.getLogger("UnifiedStreamer").critical(
            "Fallo crítico en la ejecución combinada: %s", exc, exc_info=True
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
