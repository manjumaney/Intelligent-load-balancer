import time
import random
import asyncio
from datetime import datetime

from fastapi import FastAPI, Response
from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    server_id: str
    uptime_seconds: float
    active_connections: int
    total_requests: int


def create_backend_app(server_id: str) -> FastAPI:
    """
    Builds one independent backend server instance.
    Each call creates its own isolated state (via closures), so multiple
    instances can run safely side-by-side in the same process.
    """
    app = FastAPI(title=f"Backend ({server_id})")

    state = {
        "start_time": time.time(),
        "active_connections": 0,
        "total_requests": 0,
    }

    @app.middleware("http")
    async def track_connections(request, call_next):
        state["active_connections"] += 1
        state["total_requests"] += 1
        try:
            response = await call_next(request)
        finally:
            state["active_connections"] -= 1
        return response

    @app.get("/")
    async def root():
        delay = random.uniform(0.01, 0.1)
        await asyncio.sleep(delay)
        return {
            "message": "Request handled successfully",
            "server_id": server_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "processing_time_ms": round(delay * 1000, 2),
        }

    @app.get("/health", response_model=HealthResponse)
    async def health():
        return HealthResponse(
            status="healthy",
            server_id=server_id,
            uptime_seconds=round(time.time() - state["start_time"], 2),
            active_connections=state["active_connections"],
            total_requests=state["total_requests"],
        )

    @app.get("/metrics")
    async def metrics():
        lines = [
            f'server_active_connections{{server_id="{server_id}"}} {state["active_connections"]}',
            f'server_total_requests{{server_id="{server_id}"}} {state["total_requests"]}',
            f'server_uptime_seconds{{server_id="{server_id}"}} {round(time.time() - state["start_time"], 2)}',
        ]
        return Response(content="\n".join(lines) + "\n", media_type="text/plain")

    return app
