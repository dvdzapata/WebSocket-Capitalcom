"""Backward compatible launcher that delegates to the unified streamer."""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from streaming_unified import configure_logging, load_environment, run_all

LOGGER = logging.getLogger("UnifiedStreamer")


def main() -> None:
    load_environment(Path(".env"))
    configure_logging()
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        LOGGER.info("Ejecución interrumpida por el usuario")
    except Exception as exc:
        LOGGER.critical("Fallo crítico en la ejecución combinada: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
