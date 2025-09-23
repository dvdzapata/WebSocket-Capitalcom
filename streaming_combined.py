"""Unified runner for Capital.com and EODHD WebSocket clients."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import threading
from pathlib import Path

from dotenv import load_dotenv

from streaming_capital import (
    Config as CapitalConfig,
    CapitalWebSocketStreamer,
    configure_logging as configure_capital_logging,
)
from streaming_eodhd import (
    Settings as EODSettings,
    configure_logging as configure_eodhd_logging,
    run as run_eodhd,
)

LOGGER = logging.getLogger("StreamingSupervisor")


async def run_combined() -> None:
    """Start both streaming services and keep them alive until shutdown."""

    load_dotenv(Path(".env"))
    configure_capital_logging()
    configure_eodhd_logging()

    try:
        capital_config = CapitalConfig.from_env()
    except Exception as exc:  # pragma: no cover - validated at runtime
        LOGGER.critical("No se pudo cargar configuración de Capital.com: %s", exc, exc_info=True)
        raise

    try:
        eod_settings = EODSettings.from_env()
    except Exception as exc:  # pragma: no cover - validated at runtime
        LOGGER.critical("No se pudo cargar configuración de EODHD: %s", exc, exc_info=True)
        raise

    try:
        capital_streamer = CapitalWebSocketStreamer(capital_config)
    except Exception as exc:  # pragma: no cover - connection failures
        LOGGER.critical("No se pudo inicializar el streamer de Capital.com: %s", exc, exc_info=True)
        raise

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def request_shutdown(reason: str) -> None:
        if shutdown_event.is_set():
            return
        LOGGER.warning("Cierre solicitado (%s)", reason)
        loop.call_soon_threadsafe(shutdown_event.set)

    def capital_thread_main() -> None:
        LOGGER.info("Capital.com streaming iniciado")
        try:
            capital_streamer.run()
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Capital.com streaming terminó con error")
            request_shutdown("capital_stream_error")
        finally:
            if not capital_streamer.stop_event.is_set():
                capital_streamer.stop()
            LOGGER.info("Capital.com streaming detenido")

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
            LOGGER.info(
                "add_signal_handler no disponible para %s. Usando signal.signal.",
                signame,
            )
            signal.signal(sig, _sync_signal_handler)
        except RuntimeError as exc:
            LOGGER.warning(
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
                LOGGER.warning("Capital.com streaming thread finalizó inesperadamente")
                request_shutdown("capital_thread_exit")
                return
            await asyncio.sleep(5)

    async def stop_capital_on_shutdown() -> None:
        await shutdown_event.wait()
        LOGGER.info("Deteniendo Capital.com streaming...")
        if not capital_streamer.stop_event.is_set():
            capital_streamer.stop()
        await asyncio.to_thread(capital_thread.join, 15)
        LOGGER.info("Capital.com streaming detenido por apagado")

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
        LOGGER.info("Servicios de streaming finalizados")


def main() -> None:
    try:
        asyncio.run(run_combined())
    except KeyboardInterrupt:
        LOGGER.info("Ejecución interrumpida por el usuario")
    except Exception as exc:
        LOGGER.critical("Fallo crítico en la ejecución combinada: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
