"""EOD Historical Data WebSocket client for US equities trades and quotes."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, time as dtime, timezone
from typing import Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOGGER = logging.getLogger("EODHD_WS")
LOGGER.setLevel(LOG_LEVEL)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter(
        json.dumps(
            {
                "ts": "%(asctime)s",
                "level": "%(levelname)s",
                "service": "EODHD_WS",
                "module": "%(module)s",
                "msg": "%(message)s",
            }
        )
    )
)
if not LOGGER.handlers:
    LOGGER.addHandler(handler)


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
            async with session.ws_connect(
                self.url,
                heartbeat=self.settings.heartbeat,
                receive_timeout=self.settings.heartbeat + 30,
            ) as ws:
                LOGGER.info("Conectado a %s", self.name)
                await ws.send_json(payload)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_text(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError(f"WebSocket error: {ws.exception()}")
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        LOGGER.warning("WebSocket %s cerrado por el servidor", self.name)
                        break

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
            "Persistiendo trade %s %.4f x %s", mapping.local_symbol, float(price), size
        )
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

        LOGGER.debug(
            "Persistiendo quote %s bid=%s ask=%s",
            mapping.local_symbol,
            bid_price,
            ask_price,
        )
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


async def run(settings: Settings) -> None:
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

    stop_event = asyncio.Event()

    def _handle_signal(signame: str) -> None:
        LOGGER.warning("Señal %s recibida. Cerrando...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for signame in {"SIGTERM", "SIGINT"}:
        if hasattr(signal, signame):
            loop.add_signal_handler(getattr(signal, signame), _handle_signal, signame)

    async def _stop_on_event() -> None:
        await stop_event.wait()
        await asyncio.gather(trades_worker.stop(), quotes_worker.stop())
        database.close()

    await asyncio.gather(
        trades_worker.start(),
        quotes_worker.start(),
        _stop_on_event(),
    )


def main() -> None:
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
