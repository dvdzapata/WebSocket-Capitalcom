"""Streaming client helpers for the Capital.com API.

This module contains a small helper class that handles the REST
authentication required before opening a WebSocket connection.  Only the
pieces that we need for the current task are implemented here; the class can
be extended with the rest of the streaming logic elsewhere in the project.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Dict, Optional

try:  # pragma: no cover - executed only when dependency is available
    import requests  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for test environments
    class _FallbackSession:
        def __init__(self) -> None:
            self.headers: Dict[str, str] = {}

        def post(self, *args, **kwargs):  # noqa: D401 - simple stub
            """Placeholder implementation that signals the missing dependency."""
            raise NotImplementedError("The 'requests' package is required for HTTP operations")

    class _FallbackRequestException(Exception):
        """Lightweight substitute for :class:`requests.RequestException`."""

    requests = SimpleNamespace(Session=_FallbackSession, RequestException=_FallbackRequestException)


LOG = logging.getLogger(__name__)


class CapitalStreamerError(RuntimeError):
    """Base exception for the streaming client."""


class CapitalAuthenticationError(CapitalStreamerError):
    """Raised when the REST authentication flow fails."""


@dataclass
class CapitalWebSocketStreamer:
    """Thin helper around the Capital.com authentication endpoint."""

    rest_base_url: str
    identifier: str
    password: str
    api_key: str
    session: requests.Session = field(default_factory=requests.Session)
    request_timeout: Optional[float] = 10.0

    def __post_init__(self) -> None:
        self.rest_base_url = self.rest_base_url.rstrip("/")
        self.session.headers.update({
            "X-CAP-API-KEY": self.api_key,
            "Content-Type": "application/json",
        })

    def authenticate(self) -> Dict[str, str]:
        """Authenticate against the REST API and return the security tokens."""

        url = f"{self.rest_base_url}/session"
        payload = {"identifier": self.identifier, "password": self.password}

        response: Optional[requests.Response] = None
        try:
            response = self.session.post(
                url,
                data=json.dumps(payload),
                timeout=self.request_timeout,
            )
        except requests.RequestException as exc:  # pragma: no cover - trivial
            LOG.error("Error de red autenticando contra %s: %s", url, exc, exc_info=True)
            raise CapitalAuthenticationError(
                "Fallo autenticando contra la API REST de Capital.com"
            ) from exc

        if response is None:
            LOG.error("La autenticación no devolvió respuesta para %s", url)
            raise CapitalAuthenticationError("Respuesta vacía al autenticarse con Capital.com")

        if response.status_code not in (200, 201):
            detail: Optional[str] = None
            try:
                detail_json = response.json()
            except ValueError:
                detail_json = None
            if isinstance(detail_json, dict):
                detail = detail_json.get("errorMessage") or detail_json.get("errorCode")
            if not detail:
                detail = response.text[:200]
            LOG.error(
                "Autenticación rechazada (%s): %s",
                response.status_code,
                detail,
            )
            raise CapitalAuthenticationError(
                f"Autenticación rechazada con estado {response.status_code}"
            )

        cst = response.headers.get("CST")
        xst = response.headers.get("X-SECURITY-TOKEN") or response.headers.get("x-security-token")

        if not cst or not xst:
            tokens_from_body: Dict[str, str] = {}
            try:
                tokens_json = response.json()
            except ValueError:
                tokens_json = None
            if isinstance(tokens_json, dict):
                tokens_from_body = {
                    key.upper(): value
                    for key, value in tokens_json.items()
                    if key and key.lower() in {"cst", "x-security-token", "securitytoken"}
                }
            cst = cst or tokens_from_body.get("CST")
            xst = xst or tokens_from_body.get("X-SECURITY-TOKEN") or tokens_from_body.get("SECURITYTOKEN")

        if not cst or not xst:
            LOG.error("Tokens de seguridad no presentes en la respuesta de autenticación")
            raise CapitalAuthenticationError("Tokens de sesión no devueltos por el endpoint de autenticación")

        tokens = {"CST": cst, "X-SECURITY-TOKEN": xst}
        self.session.headers.update(tokens)
        LOG.info("Autenticación contra Capital.com completada correctamente")
        return tokens

