"""
FastAPI application for the SSE streaming service.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from chris_streaming.common.redis_stream import create_redis_client
from chris_streaming.common.settings import SSEServiceSettings
from .dispatcher import StreamDispatcher
from .routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = SSEServiceSettings()
    redis = await create_redis_client(settings.redis_url)
    dispatcher = StreamDispatcher(
        redis,
        num_shards=settings.stream_num_shards,
        status_base=settings.stream_status_base,
        logs_base=settings.stream_logs_base,
        workflow_base=settings.stream_workflow_base,
    )
    await dispatcher.start()
    app.state.redis = redis
    app.state.dispatcher = dispatcher
    try:
        yield
    finally:
        await dispatcher.stop()
        await redis.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="ChRIS Streaming SSE Service",
        description="Server-Sent Events for job status and log streaming",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router)
    return app
