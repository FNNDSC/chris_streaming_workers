"""
Synchronous HTTP client for pfcon's REST API.

Used by Celery tasks (which run synchronously) to submit jobs and
remove containers. Handles authentication token caching.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

import requests

logger = logging.getLogger(__name__)

# pfcon job type to endpoint path mapping
_JOB_TYPE_ENDPOINTS = {
    "copy": "copyjobs",
    "plugin": "pluginjobs",
    "upload": "uploadjobs",
    "delete": "deletejobs",
}


class PfconClient:
    """Synchronous pfcon HTTP client with token caching."""

    def __init__(self, base_url: str, username: str, password: str):
        self._base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._token: str | None = None
        self._server_info: dict | None = None
        self._lock = threading.Lock()
        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"

    def get_server_info(self) -> dict:
        """GET pfcon server configuration (cached for process lifetime).

        The payload includes ``requires_copy_job`` and ``requires_upload_job``
        booleans that determine whether the client must schedule those steps
        or can skip them.
        """
        with self._lock:
            if self._server_info is not None:
                return self._server_info
        resp = self._request_with_reauth(
            "GET",
            f"{self._base_url}/api/v1/pluginjobs/",
        )
        resp.raise_for_status()
        info = resp.json()
        with self._lock:
            self._server_info = info
        logger.info(
            "pfcon server info: requires_copy_job=%s requires_upload_job=%s",
            info.get("requires_copy_job"),
            info.get("requires_upload_job"),
        )
        return info

    def _ensure_token(self) -> str:
        """Authenticate with pfcon and cache the JWT token."""
        with self._lock:
            if self._token is not None:
                return self._token
            resp = self._session.post(
                f"{self._base_url}/api/v1/auth-token/",
                json={"pfcon_user": self._username, "pfcon_password": self._password},
            )
            resp.raise_for_status()
            self._token = resp.json()["token"]
            logger.info("Authenticated with pfcon")
            return self._token

    def _auth_headers(self) -> dict[str, str]:
        token = self._ensure_token()
        return {"Authorization": f"Bearer {token}"}

    def _request_with_reauth(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make a request, re-authenticate on 401."""
        resp = self._session.request(method, url, headers=self._auth_headers(), **kwargs)
        if resp.status_code == 401:
            with self._lock:
                self._token = None
            resp = self._session.request(method, url, headers=self._auth_headers(), **kwargs)
        return resp

    def schedule_copy(self, job_id: str, params: dict[str, Any]) -> dict:
        """POST /api/v1/copyjobs/"""
        data = {
            "jid": job_id,
            "input_dirs": params.get("input_dirs", ""),
            "output_dir": params.get("output_dir", ""),
            "cpu_limit": params.get("cpu_limit", 1000),
            "memory_limit": params.get("memory_limit", 300),
        }
        resp = self._request_with_reauth(
            "POST",
            f"{self._base_url}/api/v1/copyjobs/",
            data=data,
        )
        resp.raise_for_status()
        return resp.json()

    def schedule_plugin(self, job_id: str, params: dict[str, Any]) -> dict:
        """POST /api/v1/pluginjobs/"""
        data = {
            "jid": job_id,
            "image": params["image"],
            "entrypoint": params["entrypoint"],
            "args": params.get("args", []),
            "type": params.get("type", "ds"),
            "auid": params.get("auid", "cube"),
            "number_of_workers": params.get("number_of_workers", 1),
            "cpu_limit": params.get("cpu_limit", 1000),
            "memory_limit": params.get("memory_limit", 300),
            "gpu_limit": params.get("gpu_limit", 0),
            "input_dirs": params.get("input_dirs", ""),
            "output_dir": params.get("output_dir", ""),
            "env": params.get("env", []),
        }
        resp = self._request_with_reauth(
            "POST",
            f"{self._base_url}/api/v1/pluginjobs/",
            data=data,
        )
        resp.raise_for_status()
        return resp.json()

    def schedule_upload(self, job_id: str, params: dict[str, Any]) -> dict:
        """POST /api/v1/uploadjobs/"""
        data = {
            "jid": job_id,
            "job_output_path": params.get("output_dir", ""),
            "cpu_limit": params.get("cpu_limit", 1000),
            "memory_limit": params.get("memory_limit", 300),
        }
        resp = self._request_with_reauth(
            "POST",
            f"{self._base_url}/api/v1/uploadjobs/",
            data=data,
        )
        resp.raise_for_status()
        return resp.json()

    def schedule_delete(self, job_id: str, params: dict[str, Any]) -> dict:
        """POST /api/v1/deletejobs/"""
        data = {
            "jid": job_id,
            "cpu_limit": params.get("cpu_limit", 1000),
            "memory_limit": params.get("memory_limit", 300),
        }
        resp = self._request_with_reauth(
            "POST",
            f"{self._base_url}/api/v1/deletejobs/",
            data=data,
        )
        resp.raise_for_status()
        return resp.json()

    def remove_container(self, job_id: str, job_type: str) -> bool:
        """DELETE /api/v1/{job_type}jobs/{job_id}/"""
        endpoint = _JOB_TYPE_ENDPOINTS.get(job_type)
        if not endpoint:
            logger.error("Unknown job_type for removal: %s", job_type)
            return False
        resp = self._request_with_reauth(
            "DELETE",
            f"{self._base_url}/api/v1/{endpoint}/{job_id}/",
        )
        if resp.status_code == 204:
            logger.info("Removed container: job=%s type=%s", job_id, job_type)
            return True
        logger.warning(
            "Container removal returned %d: job=%s type=%s",
            resp.status_code, job_id, job_type,
        )
        return False
