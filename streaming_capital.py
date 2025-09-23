"""
Capital.com streaming client for capturing AVGO, AMD and NVDA trades via WebSocket
and persisting them into PostgreSQL.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import psycopg2
import requests
from psycopg2.extras import RealDictCursor

try:
    from websocket import WebSocketApp
except ImportError as exc:  # pragma: no cover - dependency runtime check
    raise ImportError(
        "The 'websocket-client' package is required to run this module"
    ) from exc

CAPITAL_API_BASE = "https://api-capital.backend-capital.com/api/v1"
CAPITAL_STREAM_URL = "wss://api-streaming-capital.backend-capital.com/connect"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def configure_logging() -> None:
    """Configure root logger with unified format if needed."""

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
def load_dotenv(env_path: Path) -> None:
    """Simple .env loader that respects existing environment variables."""

    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip()


@dataclass
class Config:
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
    def from_env(cls) -> "Config":
        raw_epics = os.environ.get("CAPITAL_TARGET_EPICS", "AVGO,AMD,NVDA")
        epics = [item.strip() for item in raw_epics.split(",") if item.strip()]

        missing = [
            key
            for key in ("CAPITAL_API_KEY", "CAPITAL_EMAIL", "CAPITAL_PASSWORD")
            if not os.environ.get(key)
        ]
        if missing:
            raise RuntimeError(f"Missing Capital.com credentials in environment: {', '.join(missing)}")

        db_missing = [
            key
            for key in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD")
            if not os.environ.get(key)
        ]
        if db_missing:
            raise RuntimeError(f"Missing PostgreSQL configuration in environment: {', '.join(db_missing)}")

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
    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stop_event = threading.Event()
        self.connected_event = threading.Event()
        self.ws_lock = threading.Lock()
        self.ws_app: Optional[WebSocketApp] = None
        self.db_conn = self._create_db_connection()
        self.asset_ids = self._load_asset_ids()
        self.tokens: Optional[Dict[str, str]] = None
        self.ping_thread = threading.Thread(target=self._ping_loop, daemon=True)
        self.ping_thread.start()
        self.last_message_at = time.monotonic()
        self.watchdog_thread = threading.Thread(
            target=self._inactivity_watchdog,
            name="CapitalInactivityWatchdog",
            daemon=True,
        )
        self.watchdog_thread.start()
        self.processed_messages = 0
        self._last_activity_log = time.monotonic()

    def _create_db_connection(self) -> psycopg2.extensions.connection:
        self.logger.info("Connecting to PostgreSQL %s:%s", self.config.db_host, self.config.db_port)
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
        self.logger.info("Loading asset ids for EPICs: %s", ", ".join(self.config.target_epics))
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
                "Missing asset_id mappings for EPICs in assets table: " + ", ".join(missing)
            )
        self.logger.info("Loaded asset ids for %d EPICs", len(mapping))
        return mapping

    def authenticate(self) -> None:
        self.logger.info("Authenticating against Capital.com API")
        session = requests.Session()
        session.headers.update({
            "X-CAP-API-KEY": self.config.api_key,
            "Content-Type": "application/json",
        })
        try:
            response = session.post(
                f"{CAPITAL_API_BASE}/session",
                data=json.dumps({"identifier": self.config.email, "password": self.config.password}),
                timeout=30,
            )
        finally:
            session.close()
        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Authentication failed with status {response.status_code}: {response.text[:200]}"
            )
        cst = response.headers.get("CST") or response.headers.get("cst")
        security_token = response.headers.get("X-SECURITY-TOKEN") or response.headers.get("x-security-token")
        if not cst or not security_token:
            body = {}
            try:
                body = response.json()
            except Exception:  # pragma: no cover - defensive logging
                pass
            cst = cst or body.get("CST")
            security_token = security_token or body.get("securityToken")
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
            headers = [f"CST: {self.tokens['cst']}", f"X-SECURITY-TOKEN: {self.tokens['securityToken']}"]
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

    def _on_open(self, ws: WebSocketApp) -> None:
        self.logger.info("WebSocket connection established")
        self.connected_event.set()
        self._mark_activity()
        self._send_subscribe()

    def _on_close(self, ws: WebSocketApp, status_code: Optional[int], msg: Optional[str]) -> None:
        self.logger.warning("WebSocket closed: status=%s message=%s", status_code, msg)
        self.connected_event.clear()

    def _on_error(self, ws: WebSocketApp, error: Exception) -> None:
        self.logger.error("WebSocket error: %s", error, exc_info=True)
        self.connected_event.clear()

    def _on_message(self, ws: WebSocketApp, message: str) -> None:
        self._mark_activity()
        self.logger.debug("Received message: %s", message)
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
            "precio_compra": _to_float(quote.get("bid")),
            "cantidad_disponible_compra": _to_float(quote.get("bidQty")),
            "precio_venta": _to_float(quote.get("ofr")),
            "cantidad_disponible_venta": _to_float(quote.get("ofrQty")),
            "epoch": epoch_ms_int,
        }
        self._persist_record(record)

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
        self.logger.info("Subscription message sent for EPICs: %s", ", ".join(self.config.target_epics))

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
            self._log_progress(record)
        except Exception as exc:
            self.logger.error("Failed to persist record: %s", exc, exc_info=True)

    def _log_progress(self, record: Dict[str, object]) -> None:
        self.processed_messages += 1
        symbol = record.get("symbol", "?")

        def _fmt(value: Optional[float]) -> str:
            if value is None:
                return "N/A"
            try:
                return f"{float(value):.4f}"
            except (TypeError, ValueError):
                return str(value)

        description = (
            f"{symbol} bid={_fmt(record.get('precio_compra'))} "
            f"ask={_fmt(record.get('precio_venta'))}"
        )
        now = time.monotonic()
        if self.processed_messages == 1:
            self.logger.info("Primer quote recibido: %s", description)
            self._last_activity_log = now
            return

        if now - self._last_activity_log >= self.config.activity_log_interval_seconds:
            ts = record.get("timestamp")
            ts_str = ts.isoformat() if isinstance(ts, datetime) else str(ts)
            self.logger.info(
                "Quotes procesados: %s. Último %s a %s",
                self.processed_messages,
                symbol,
                ts_str,
            )
            self._last_activity_log = now

    def stop(self) -> None:
        self.stop_event.set()
        self.connected_event.set()
        with self.ws_lock:
            if self.ws_app is not None:
                try:
                    self.ws_app.close()
                except Exception:
                    self.logger.exception("Error closing WebSocket")
        if self.ping_thread.is_alive() and threading.current_thread() is not self.ping_thread:
            self.ping_thread.join(timeout=5)
        if self.watchdog_thread.is_alive() and threading.current_thread() is not self.watchdog_thread:
            self.watchdog_thread.join(timeout=5)
        if self.db_conn:
            try:
                self.db_conn.close()
            except Exception:
                self.logger.exception("Error closing database connection")


def _to_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    load_dotenv(Path(".env"))
    configure_logging()
    config = Config.from_env()
    streamer = CapitalWebSocketStreamer(config)

    def handle_signal(signum: int, frame) -> None:  # type: ignore[override]
        streamer.logger.info("Signal %s received, shutting down", signum)
        streamer.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        streamer.run()
    finally:
        streamer.stop()


if __name__ == "__main__":
    main()
