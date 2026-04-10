"""Tests for chris_streaming.sse_service.pfcon_client."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from chris_streaming.sse_service.pfcon_client import PfconClient


class TestPfconClient:
    def _make_client(self) -> PfconClient:
        return PfconClient(
            base_url="http://pfcon:30005",
            username="pfcon",
            password="pfcon1234",
        )

    def test_base_url_trailing_slash_stripped(self):
        client = PfconClient("http://pfcon:30005/", "u", "p")
        assert client._base_url == "http://pfcon:30005"

    def test_ensure_token_authenticates(self):
        client = self._make_client()
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"token": "jwt-abc"}
        client._session = MagicMock()
        client._session.post.return_value = mock_resp

        token = client._ensure_token()

        assert token == "jwt-abc"
        client._session.post.assert_called_once_with(
            "http://pfcon:30005/api/v1/auth-token/",
            json={"pfcon_user": "pfcon", "pfcon_password": "pfcon1234"},
        )

    def test_ensure_token_caches(self):
        client = self._make_client()
        client._token = "cached-token"
        token = client._ensure_token()
        assert token == "cached-token"

    def test_auth_headers(self):
        client = self._make_client()
        client._token = "test-token"
        headers = client._auth_headers()
        assert headers == {"Authorization": "Bearer test-token"}

    def test_request_with_reauth_on_401(self):
        client = self._make_client()
        client._token = "expired"

        resp_401 = MagicMock(status_code=401)
        resp_ok = MagicMock(status_code=200)
        resp_ok.json.return_value = {}

        # First call returns 401, re-auth, second call returns 200
        auth_resp = MagicMock()
        auth_resp.raise_for_status = MagicMock()
        auth_resp.json.return_value = {"token": "new-token"}

        call_count = 0

        def mock_request(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return resp_401
            return resp_ok

        client._session = MagicMock()
        client._session.request = mock_request
        client._session.post.return_value = auth_resp

        result = client._request_with_reauth("GET", "http://pfcon:30005/api/v1/test/")
        assert result.status_code == 200
        assert client._token == "new-token"

    def test_schedule_copy(self):
        client = self._make_client()
        client._token = "t"
        mock_resp = MagicMock(status_code=200)
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"compute": {"status": "notStarted"}}
        client._session = MagicMock()
        client._session.request.return_value = mock_resp

        result = client.schedule_copy("j1", {"input_dirs": "/in", "output_dir": "/out"})
        assert result == {"compute": {"status": "notStarted"}}

    def test_schedule_plugin(self):
        client = self._make_client()
        client._token = "t"
        mock_resp = MagicMock(status_code=200)
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"compute": {"status": "started"}}
        client._session = MagicMock()
        client._session.request.return_value = mock_resp

        result = client.schedule_plugin("j1", {
            "image": "img:latest",
            "entrypoint": ["python", "app.py"],
        })
        assert "compute" in result

    def test_remove_container_success(self):
        client = self._make_client()
        client._token = "t"
        mock_resp = MagicMock(status_code=204)
        client._session = MagicMock()
        client._session.request.return_value = mock_resp

        result = client.remove_container("j1", "plugin")
        assert result is True

    def test_remove_container_not_found(self):
        client = self._make_client()
        client._token = "t"
        mock_resp = MagicMock(status_code=404)
        client._session = MagicMock()
        client._session.request.return_value = mock_resp

        result = client.remove_container("j1", "plugin")
        assert result is False

    def test_remove_container_unknown_type(self):
        client = self._make_client()
        result = client.remove_container("j1", "unknown")
        assert result is False
