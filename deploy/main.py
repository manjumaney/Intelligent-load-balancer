import os
import asyncio
import logging

import uvicorn

from backend_app import create_backend_app
from lb_app import app as lb_app

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("main")

# Backend instances run on internal-only ports (never exposed to the internet
# directly — only the load balancer's port is exposed by the hosting platform).
BACKEND_PORTS = [8001, 8002, 8003]


async def run_server(app, host: str, port: int, name: str):
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    logger.info(f"Starting {name} on {host}:{port}")
    await server.serve()


async def main():
    tasks = []

    # Start the 3 internal backend instances
    for i, port in enumerate(BACKEND_PORTS, start=1):
        server_id = f"backend-{i}"
        backend_app = create_backend_app(server_id=server_id)
        tasks.append(run_server(backend_app, "127.0.0.1", port, server_id))

    # Start the load balancer on the port the hosting platform assigns
    # (Render, Railway, etc. inject $PORT; defaults to 9000 for local runs)
    public_port = int(os.getenv("PORT", 9000))
    tasks.append(run_server(lb_app, "0.0.0.0", public_port, "load-balancer"))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
