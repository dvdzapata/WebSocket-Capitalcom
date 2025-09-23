"""Standalone launcher for the Capital.com WebSocket streamer."""

from __future__ import annotations

import logging
import signal
import sys
from pathlib import Path

from streaming_unified import (
    CapitalConfig,
    CapitalWebSocketStreamer,
    configure_logging,
    load_environment,
)

LOGGER = logging.getLogger("CapitalWebSocket")


def main() -> None:
    load_environment(Path(".env"))
    configure_logging()

    try:
        config = CapitalConfig.from_env()
    except Exception as exc:
        LOGGER.critical("Configuración inválida para Capital.com: %s", exc, exc_info=True)
        sys.exit(1)

    streamer = CapitalWebSocketStreamer(config)

    def handle_signal(signum: int, frame) -> None:  # type: ignore[override]
        try:
            signame = signal.Signals(signum).name
        except Exception:
            signame = str(signum)
        LOGGER.warning("Señal %s recibida. Solicitando apagado...", signame)
        streamer.stop()

    for signame in ("SIGTERM", "SIGINT"):
        if hasattr(signal, signame):
            signal.signal(getattr(signal, signame), handle_signal)

    try:
        streamer.run()
    except KeyboardInterrupt:
        LOGGER.info("Proceso interrumpido por el usuario")
    except Exception as exc:
        LOGGER.critical("Fallo crítico en el streamer de Capital.com: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if not streamer.stop_event.is_set():
            streamer.stop()


if __name__ == "__main__":
    main()
