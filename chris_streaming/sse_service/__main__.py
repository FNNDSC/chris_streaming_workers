"""
Entry point for the SSE Service.

    python -m chris_streaming.sse_service

Runs the FastAPI SSE server with uvicorn.
"""

from __future__ import annotations

import uvicorn

from chris_streaming.common.settings import SSEServiceSettings
from .app import create_app

app = create_app()

if __name__ == "__main__":
    settings = SSEServiceSettings()
    uvicorn.run(
        "chris_streaming.sse_service.__main__:app",
        host=settings.host,
        port=settings.port,
        log_level="info",
    )
