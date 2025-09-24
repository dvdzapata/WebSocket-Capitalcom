import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from streaming_capital import CapitalAuthenticationError, CapitalWebSocketStreamer, requests


class DummyResponse:
    def __init__(self, status_code=200, headers=None, json_data=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self._json_data = json_data
        self.text = text

    def json(self):
        if isinstance(self._json_data, Exception):
            raise self._json_data
        if self._json_data is None:
            raise ValueError("No JSON data")
        return self._json_data


class DummySession:
    def __init__(self, *, side_effect=None, response=None):
        self.headers = {}
        self._side_effect = side_effect
        self._response = response

    def post(self, *args, **kwargs):
        if self._side_effect is not None:
            raise self._side_effect
        return self._response


def make_streamer(session=None):
    session = session or DummySession(response=DummyResponse(headers={"CST": "a", "X-SECURITY-TOKEN": "b"}))
    return CapitalWebSocketStreamer(
        rest_base_url="https://example.com/api/v1",
        identifier="user",
        password="pass",
        api_key="key",
        session=session,
    )


def test_authenticate_network_error_triggers_controlled_exception():
    session = DummySession(side_effect=requests.RequestException("boom"))
    streamer = make_streamer(session=session)

    with pytest.raises(CapitalAuthenticationError):
        streamer.authenticate()


def test_authenticate_success_returns_tokens_and_updates_session_headers():
    headers = {"CST": "token1", "X-SECURITY-TOKEN": "token2"}
    session = DummySession(response=DummyResponse(headers=headers))
    streamer = make_streamer(session=session)

    tokens = streamer.authenticate()

    assert tokens == headers
    assert session.headers["CST"] == "token1"
    assert session.headers["X-SECURITY-TOKEN"] == "token2"

