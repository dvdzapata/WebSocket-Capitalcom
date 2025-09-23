"""Standalone launcher for the EODHD WebSocket clients."""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from streaming_unified import (
    EODSettings,
    SettingsError,
    configure_logging,
    load_environment,
    run_eodhd,
)

LOGGER = logging.getLogger("EODHD_WS")


def main() -> None:
    load_environment(Path(".env"))
    configure_logging()
    try:
        settings = EODSettings.from_env()
    except SettingsError as exc:
        LOGGER.critical("Configuración inválida: %s", exc)
        sys.exit(1)
    except Exception as exc:
        LOGGER.critical("Error leyendo configuración de entorno: %s", exc, exc_info=True)
        sys.exit(1)

    try:
        asyncio.run(run_eodhd(settings))
    except KeyboardInterrupt:
        LOGGER.info("Proceso interrumpido por el usuario")
    except Exception as exc:
        LOGGER.critical("Fallo crítico en EODHD_WS: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
