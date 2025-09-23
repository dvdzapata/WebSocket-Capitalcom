"""EOD Historical Data WebSocket client for US equities trades and quotes."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timezone
from typing import Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool

load_dotenv()

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOGGER = logging.getLogger("EODHD_WS")


def configure_logging() -> None:
    """Configure root logging with a consistent format."""

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
    LOGGER.setLevel(level)


@dataclass(frozen=True)
class SymbolMapping:
    """Symbol mapping between local identifier and EODHD epic."""

    local_symbol: str
    remote_symbol: str
    asset_id: int


class SettingsError(RuntimeError):
    """Raised when configuration is invalid."""


@dataclass
class Settings:
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
    def from_env(cls) -> "Settings":
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
        except ValueError as exc:  # pragma: no cover - validated manual input
            raise SettingsError(f"asset_id inválido para {local_symbol}: {asset_id_raw}") from exc

        if "." not in remote_symbol and suffix:
            corrected_symbol = f"{remote_symbol}{suffix}"
            LOGGER.warning(
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
    def __init__(self, settings: Settings) -> None:
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


def _format_price(value: Optional[float]) -> str:
    return "N/A" if value is None else f"{value:.4f}"


class WebSocketWorker:
    def __init__(
        self,
        *,
        name: str,
        url: str,
        settings: Settings,
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
                LOGGER.error(
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
                    LOGGER.info("Conectado a %s", self.name)
                    await ws.send_json(payload)
                    self._last_message_at = time.monotonic()
                    while not self._closing.is_set():
                        try:
                            msg = await ws.receive(timeout=self.settings.inactivity_timeout)
                        except asyncio.TimeoutError:
                            elapsed = time.monotonic() - self._last_message_at
                            LOGGER.warning(
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
                            LOGGER.warning("WebSocket %s cerrado por el servidor", self.name)
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError(f"WebSocket error: {ws.exception()}")
                        else:
                            LOGGER.debug("Mensaje %s ignorado en %s", msg.type, self.name)
            finally:
                self._session = None

    async def _handle_text(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            LOGGER.error("Mensaje no es JSON válido en %s: %s", self.name, raw)
            return

        if isinstance(data, dict) and "status" in data and "s" not in data:
            status = data.get("status")
            message = data.get("message", "")
            status_text = str(status).upper()
            if status_text not in {"OK", "200"}:
                raise RuntimeError(f"Suscripción rechazada ({status}): {message}")
            LOGGER.info("Respuesta de control %s: %s", self.name, message or status)
            return

        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict):
                    await self.process_payload(entry)
            return

        if not isinstance(data, dict):
            LOGGER.debug("Mensaje ignorado en %s: %s", self.name, data)
            return

        await self.process_payload(data)

    async def process_payload(self, data: Dict[str, object]) -> None:  # pragma: no cover - overwritten
        raise NotImplementedError

    def _register_activity(self, description: str) -> None:
        self._processed_messages += 1
        now = time.monotonic()
        if self._processed_messages == 1:
            LOGGER.info("%s: primer mensaje recibido -> %s", self.name, description)
            self._last_activity_log = now
            return

        if now - self._last_activity_log >= self.settings.activity_log_interval:
            LOGGER.info(
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
            LOGGER.debug("Mensaje de trade ignorado: %s", data)
            return
        mapping = self.mappings.get(str(symbol_remote))
        if not mapping:
            LOGGER.warning("Trade recibido para símbolo no mapeado: %s", symbol_remote)
            return

        try:
            price_value = float(price)
        except (TypeError, ValueError):
            LOGGER.warning("Trade con precio inválido para %s: %s", symbol_remote, price)
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

        LOGGER.debug(
            "Persistiendo trade %s %.4f x %s", mapping.local_symbol, price_value, size
        )
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
            LOGGER.error(
                "No se pudo insertar trade %s en base de datos: %s",
                mapping.local_symbol,
                exc,
                exc_info=True,
            )


class QuotesWorker(WebSocketWorker):
    async def process_payload(self, data: Dict[str, object]) -> None:
        symbol_remote = data.get("s")
        if not symbol_remote:
            LOGGER.debug("Mensaje de quote sin símbolo: %s", data)
            return
        mapping = self.mappings.get(str(symbol_remote))
        if not mapping:
            LOGGER.warning("Quote recibido para símbolo no mapeado: %s", symbol_remote)
            return

        bid_price = _safe_float(data.get("bp"))
        ask_price = _safe_float(data.get("ap"))
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

        LOGGER.debug(
            "Persistiendo quote %s bid=%s ask=%s",
            mapping.local_symbol,
            bid_price,
            ask_price,
        )
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
            LOGGER.error(
                "No se pudo insertar quote %s en base de datos: %s",
                mapping.local_symbol,
                exc,
                exc_info=True,
            )


async def run(
    settings: Settings,
    *,
    stop_event: Optional[asyncio.Event] = None,
    register_signals: bool = True,
) -> None:
    mappings_list = parse_symbol_mappings(settings.symbols_env, settings.symbol_suffix)
    mappings = {m.remote_symbol: m for m in mappings_list}
    payload_symbols = ",".join(m.remote_symbol for m in mappings_list)
    LOGGER.info("Suscribiendo símbolos remotos: %s", payload_symbols)

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
        LOGGER.warning("Señal %s recibida. Cerrando...", signame)
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
                LOGGER.info(
                    "add_signal_handler no disponible en esta plataforma. Usando signal.signal para %s",
                    signame,
                )
                signal.signal(sig, _sync_signal_handler)
            except RuntimeError as exc:
                LOGGER.warning(
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


def main() -> None:
    configure_logging()
    try:
        settings = Settings.from_env()
    except SettingsError as exc:
        LOGGER.critical("Configuración inválida: %s", exc)
        sys.exit(1)

    try:
        asyncio.run(run(settings))
    except KeyboardInterrupt:
        LOGGER.info("Proceso interrumpido por el usuario")
    except Exception as exc:
        LOGGER.critical("Fallo crítico: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
