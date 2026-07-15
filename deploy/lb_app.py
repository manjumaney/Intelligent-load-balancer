import os
import time
import asyncio
import logging
from itertools import count
from collections import deque
from datetime import datetime

import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response

logging.basicConfig(level=logging.INFO, format="%(asctime)s [LB] %(message)s")
logger = logging.getLogger("loadbalancer")

# Rolling in-memory log of recent requests (used for the dashboard's log table + charts).
# Bounded so memory doesn't grow unbounded on a long-running deployment.
REQUEST_LOG = deque(maxlen=300)


class GlobalStats:
    """Lifetime, never-truncated counters — unlike REQUEST_LOG, these never lose
    data no matter how many requests the app has served."""
    total_requests = 0
    total_errors = 0
    total_latency_ms = 0.0

 
# Configuration (all overridable via environment variables / docker-compose)
 

# Format: "http://backend1:8000:1,http://backend2:8000:2,http://backend3:8000:1"
# The trailing number is the weight (used only by weighted_round_robin). Defaults to 1.
RAW_SERVERS = os.getenv(
    "BACKEND_SERVERS",
    "http://localhost:8001:1,http://localhost:8002:1,http://localhost:8003:1",
)

class LBState:
    """Holds mutable runtime state — lets the algorithm be switched live via the API/dashboard."""
    algorithm = os.getenv("ALGORITHM", "round_robin")  # round_robin | least_connections | weighted_round_robin


HEALTH_CHECK_INTERVAL = float(os.getenv("HEALTH_CHECK_INTERVAL", "5"))
HEALTH_CHECK_TIMEOUT = float(os.getenv("HEALTH_CHECK_TIMEOUT", "2"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "5"))
UNHEALTHY_THRESHOLD = int(os.getenv("UNHEALTHY_THRESHOLD", "2"))  # consecutive failures before marking down


class Backend:
    """Represents one backend server and its live state."""

    def __init__(self, url: str, weight: int = 1):
        self.url = url.rstrip("/")
        self.weight = weight
        self.healthy = True
        self.active_connections = 0
        self.consecutive_failures = 0
        # Lifetime counters — never truncated, unlike REQUEST_LOG which is a bounded
        # rolling window used only for the recent-activity chart/table and throughput.
        self.total_requests = 0
        self.total_errors = 0
        self.total_latency_ms = 0.0
        # used internally by the smooth weighted round robin algorithm
        self.current_weight = 0

    def __repr__(self):
        return f"<Backend {self.url} healthy={self.healthy} weight={self.weight} conns={self.active_connections}>"


def parse_servers(raw: str):
    backends = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.rsplit(":", 1)
        # Handle the case where the last ":" is part of "http://host:port" and no weight given
        if len(parts) == 2 and parts[1].isdigit() and entry.count(":") >= 2:
            url, weight = parts[0], int(parts[1])
        else:
            url, weight = entry, 1
        backends.append(Backend(url, weight))
    return backends


BACKENDS = parse_servers(RAW_SERVERS)
client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)

 
# Load balancing algorithms
 

_rr_counter = count()


def pick_round_robin():
    healthy = [b for b in BACKENDS if b.healthy]
    if not healthy:
        return None
    idx = next(_rr_counter) % len(healthy)
    return healthy[idx]


def pick_least_connections():
    healthy = [b for b in BACKENDS if b.healthy]
    if not healthy:
        return None
    return min(healthy, key=lambda b: b.active_connections)


def pick_weighted_round_robin():
    """Smooth weighted round robin (same approach nginx uses)."""
    healthy = [b for b in BACKENDS if b.healthy]
    if not healthy:
        return None

    total_weight = sum(b.weight for b in healthy)
    selected = None
    for b in healthy:
        b.current_weight += b.weight
        if selected is None or b.current_weight > selected.current_weight:
            selected = b
    selected.current_weight -= total_weight
    return selected


ALGORITHMS = {
    "round_robin": pick_round_robin,
    "least_connections": pick_least_connections,
    "weighted_round_robin": pick_weighted_round_robin,
}


def select_backend():
    picker = ALGORITHMS.get(LBState.algorithm, pick_round_robin)
    return picker()


 
# Background health checking (drives failover + recovery)
 

async def health_check_loop():
    while True:
        for backend in BACKENDS:
            try:
                resp = await client.get(f"{backend.url}/health", timeout=HEALTH_CHECK_TIMEOUT)
                if resp.status_code == 200:
                    if not backend.healthy:
                        logger.info(f"Backend recovered: {backend.url}")
                    backend.healthy = True
                    backend.consecutive_failures = 0
                else:
                    raise httpx.HTTPStatusError("bad status", request=resp.request, response=resp)
            except Exception:
                backend.consecutive_failures += 1
                if backend.consecutive_failures >= UNHEALTHY_THRESHOLD and backend.healthy:
                    backend.healthy = False
                    logger.warning(f"Backend marked UNHEALTHY (failover): {backend.url}")
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting load balancer | algorithm={LBState.algorithm} | backends={[b.url for b in BACKENDS]}")
    task = asyncio.create_task(health_check_loop())
    yield
    task.cancel()
    await client.aclose()


app = FastAPI(title="Load Balancer", lifespan=lifespan)


 
# Status / observability endpoint (useful now, and for Phase 3 dashboard)
 

@app.get("/lb/status")
async def status():
    return {
        "algorithm": LBState.algorithm,
        "available_algorithms": list(ALGORITHMS.keys()),
        "backends": [
            {
                "url": b.url,
                "healthy": b.healthy,
                "weight": b.weight,
                "active_connections": b.active_connections,
                "consecutive_failures": b.consecutive_failures,
                "total_requests": b.total_requests,
            }
            for b in BACKENDS
        ],
    }


@app.get("/lb/logs")
async def get_logs(limit: int = 30):
    """Most recent proxied requests, newest first."""
    entries = list(REQUEST_LOG)[-limit:]
    entries.reverse()
    return {"count": len(entries), "logs": entries}


@app.get("/lb/analytics")
async def analytics():
    """
    Lifetime-accurate aggregated stats (total requests, avg latency, error rate,
    per-backend breakdown) sourced from unbounded counters — these never lose
    data no matter how long the app has been running or how many requests it's
    served. Throughput is the one exception: it's a rate, so it's intentionally
    computed from the recent rolling window (REQUEST_LOG) rather than lifetime.
    """
    total = GlobalStats.total_requests

    if total == 0:
        return {
            "total_requests": 0,
            "avg_response_time_ms": 0,
            "error_count": 0,
            "error_rate": 0,
            "throughput_rps": 0,
            "per_backend": [],
        }

    avg_latency = round(GlobalStats.total_latency_ms / total, 2)
    error_count = GlobalStats.total_errors
    error_rate = round((error_count / total) * 100, 1)

    # Throughput: requests observed in the last 10 seconds of wall-clock time.
    # This one has to come from the rolling log, since "requests per second"
    # only means something over a recent window, not a lifetime total.
    now = time.time()
    recent_window = [l for l in REQUEST_LOG if now - l["epoch"] <= 10]
    throughput = round(len(recent_window) / 10, 2)

    per_backend_list = [
        {
            "url": b.url,
            "count": b.total_requests,
            "avg_response_time_ms": round(b.total_latency_ms / b.total_requests, 2) if b.total_requests else 0,
            "errors": b.total_errors,
        }
        for b in BACKENDS
    ]

    return {
        "total_requests": total,
        "avg_response_time_ms": avg_latency,
        "error_count": error_count,
        "error_rate": error_rate,
        "throughput_rps": throughput,
        "per_backend": per_backend_list,
    }


@app.post("/lb/algorithm")
async def set_algorithm(request: Request):
    """Switch the active load-balancing algorithm at runtime. Body: {"algorithm": "round_robin"}"""
    data = await request.json()
    algorithm = data.get("algorithm")
    if algorithm not in ALGORITHMS:
        return Response(
            content=f'{{"error": "Unknown algorithm. Choose from: {list(ALGORITHMS.keys())}"}}',
            status_code=400,
            media_type="application/json",
        )
    old = LBState.algorithm
    LBState.algorithm = algorithm
    logger.info(f"Algorithm switched: {old} -> {algorithm}")
    return {"previous": old, "algorithm": algorithm}


@app.post("/lb/weight")
async def set_weight(request: Request):
    """Update a backend's weight at runtime. Body: {"url": "http://...", "weight": 3}"""
    data = await request.json()
    url = (data.get("url") or "").rstrip("/")
    weight = data.get("weight")

    if not url or not isinstance(weight, int) or weight < 1:
        return Response(
            content='{"error": "Provide a valid url and a positive integer weight"}',
            status_code=400,
            media_type="application/json",
        )

    for b in BACKENDS:
        if b.url == url:
            old_weight = b.weight
            b.weight = weight
            logger.info(f"Weight changed for {url}: {old_weight} -> {weight}")
            return {"url": url, "previous_weight": old_weight, "weight": weight}

    return Response(
        content=f'{{"error": "No backend found with url {url}"}}',
        status_code=404,
        media_type="application/json",
    )


@app.get("/lb/dashboard")
async def dashboard():
    """Built-in control panel — switch algorithms live and run a traffic distribution test."""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Traffic Control — Load Balancer</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,500;9..144,600&family=Inter:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg: #eef1f5;
                --surface: #ffffff;
                --border: #dfe3e0;
                --ink: #1c2530;
                --ink-soft: #5b6472;
                --accent: #2c3e6b;
                --accent-soft: #eef1f7;
                --healthy: #2f8f6b;
                --down: #b5484a;
                --bar-1: #2c3e6b;
                --bar-2: #4f7a6d;
                --bar-3: #a67c3d;
            }
            * { box-sizing: border-box; }
            html { scroll-behavior: smooth; }
            body {
                font-family: 'Inter', sans-serif;
                background: var(--bg);
                color: var(--ink);
                margin: 0;
            }

            .site-header {
                position: sticky;
                top: 0;
                z-index: 10;
                display: flex;
                align-items: center;
                justify-content: space-between;
                background: var(--surface);
                border-bottom: 1px solid var(--border);
                padding: 16px 32px;
            }
            .brand {
                display: flex;
                align-items: baseline;
                gap: 10px;
            }
            .brand-name {
                font-family: 'Fraunces', serif;
                font-weight: 600;
                font-size: 1.25rem;
            }
            .brand-tag {
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.68rem;
                letter-spacing: 0.1em;
                text-transform: uppercase;
                color: var(--ink-soft);
            }
            nav.site-nav { display: flex; gap: 4px; }
            nav.site-nav a {
                font-family: 'Inter', sans-serif;
                font-size: 0.85rem;
                font-weight: 500;
                color: var(--ink-soft);
                text-decoration: none;
                padding: 8px 12px;
                border-radius: 7px;
                transition: background 0.15s ease, color 0.15s ease;
            }
            nav.site-nav a:hover { background: var(--accent-soft); color: var(--ink); }

            main {
                max-width: 1320px;
                margin: 0 auto;
                padding: 40px 32px 80px;
            }

            .two-col {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 28px;
                align-items: start;
                margin-bottom: 28px;
            }
            .two-col .card-section { margin-bottom: 0; }
            @media (max-width: 900px) {
                .two-col { grid-template-columns: 1fr; }
            }

            .table-scroll {
                overflow-x: auto;
                -webkit-overflow-scrolling: touch;
            }

            .page-intro { margin-bottom: 36px; }
            .subhead {
                color: var(--ink-soft);
                font-size: 0.95rem;
                margin: 6px 0 0;
            }
            .subhead strong { color: var(--ink); font-weight: 600; }

            .card-section {
                background: var(--surface);
                border: 1px solid var(--border);
                border-radius: 14px;
                padding: 28px 30px;
                margin-bottom: 28px;
                scroll-margin-top: 84px;
            }
            .plain-section {
                margin-bottom: 40px;
                scroll-margin-top: 84px;
            }
            .card-section h2, .plain-section h2 {
                font-family: 'Fraunces', serif;
                font-weight: 500;
                font-size: 1.3rem;
                margin: 0 0 4px;
            }
            .section-kicker {
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.68rem;
                letter-spacing: 0.1em;
                text-transform: uppercase;
                color: var(--ink-soft);
                margin: 0 0 4px;
            }
            .section-divider {
                border: none;
                border-top: 1px solid var(--border);
                margin: 16px 0 20px;
            }

            .site-footer {
                border-top: 1px solid var(--border);
                padding: 24px 32px 40px;
                max-width: 1320px;
                margin: 0 auto;
            }

            .about-text {
                font-size: 0.92rem;
                line-height: 1.6;
                color: var(--ink);
                margin: 0 0 10px;
            }
            .about-disclaimer {
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.76rem;
                color: var(--ink-soft);
                margin: 0;
                font-style: italic;
            }


            .segmented {
                display: inline-flex;
                border: 1px solid var(--border);
                border-radius: 10px;
                background: var(--surface);
                padding: 4px;
                gap: 4px;
            }
            .segmented button {
                font-family: 'Inter', sans-serif;
                font-size: 0.88rem;
                font-weight: 500;
                border: none;
                background: transparent;
                color: var(--ink-soft);
                padding: 9px 16px;
                border-radius: 7px;
                cursor: pointer;
                transition: background 0.15s ease, color 0.15s ease;
            }
            .segmented button:hover { background: var(--accent-soft); color: var(--ink); }
            .segmented button.active { background: var(--accent); color: #fff; }

            table { width: 100%; border-collapse: collapse; }
            th {
                text-align: left;
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.68rem;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: var(--ink-soft);
                font-weight: 500;
                padding: 0 10px 10px;
            }
            td {
                padding: 12px 10px;
                border-top: 1px solid var(--border);
                font-size: 0.9rem;
                height: 44px;
                vertical-align: middle;
                line-height: 1.3;
            }
            td.mono { font-family: 'IBM Plex Mono', monospace; font-size: 0.85rem; }
            .dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%; margin-right: 7px; }
            .dot.healthy { background: var(--healthy); }
            .dot.down { background: var(--down); }
            .status-healthy { color: var(--healthy); }
            .status-down { color: var(--down); }

            .test-controls {
                display: flex;
                align-items: center;
                gap: 10px;
                margin-bottom: 20px;
            }
            .test-controls input {
                width: 70px;
                font-family: 'IBM Plex Mono', monospace;
                padding: 9px 10px;
                border: 1px solid var(--border);
                border-radius: 7px;
                font-size: 0.9rem;
                background: var(--surface);
                color: var(--ink);
            }
            .run-btn {
                font-family: 'Inter', sans-serif;
                font-weight: 500;
                font-size: 0.88rem;
                background: var(--accent);
                color: #fff;
                border: none;
                padding: 10px 18px;
                border-radius: 7px;
                cursor: pointer;
                transition: opacity 0.15s ease;
            }
            .run-btn:hover { opacity: 0.88; }

            .algo-explainer {
                background: var(--accent-soft);
                border-radius: 10px;
                padding: 16px 18px;
                margin-top: 18px;
            }
            .algo-explainer-label {
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.68rem;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: var(--accent);
                margin: 0 0 6px;
            }
            .algo-explainer p:last-child {
                font-size: 0.88rem;
                line-height: 1.55;
                color: var(--ink);
                margin: 0;
            }
            .run-btn:disabled { opacity: 0.5; cursor: default; }

            .bar-row { margin-bottom: 14px; }
            .bar-meta {
                display: flex;
                justify-content: space-between;
                font-size: 0.84rem;
                margin-bottom: 5px;
            }
            .bar-meta .url { font-family: 'IBM Plex Mono', monospace; color: var(--ink-soft); }
            .bar-meta .count { font-family: 'Fraunces', serif; font-weight: 600; font-size: 0.95rem; }
            .bar-track {
                height: 10px;
                background: var(--accent-soft);
                border-radius: 5px;
                overflow: hidden;
            }
            .bar-fill {
                height: 100%;
                border-radius: 5px;
                transition: width 0.5s cubic-bezier(0.4, 0, 0.2, 1);
            }
            .note {
                font-size: 0.78rem;
                color: var(--ink-soft);
                margin-top: 10px;
            }
            .refresh-note {
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.72rem;
                color: var(--ink-soft);
                margin: -8px 0 18px;
            }
            #switch-msg {
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.78rem;
                color: var(--ink-soft);
                margin-top: 10px;
                min-height: 1em;
            }

            .stat-grid {
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 12px;
                margin-bottom: 24px;
            }
            .stat-card {
                background: var(--surface);
                border: 1px solid var(--border);
                border-radius: 10px;
                padding: 14px 16px;
            }
            .stat-label {
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.65rem;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: var(--ink-soft);
                margin-bottom: 6px;
            }
            .stat-value {
                font-family: 'Fraunces', serif;
                font-size: 1.5rem;
                font-weight: 600;
            }
            .chart-wrap {
                background: var(--surface);
                border: 1px solid var(--border);
                border-radius: 10px;
                padding: 16px;
                margin-bottom: 20px;
            }
            .status-badge {
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.78rem;
                padding: 2px 7px;
                border-radius: 4px;
            }
            .status-badge.ok { background: #e6f4ee; color: var(--healthy); }
            .status-badge.err { background: #fbeaea; color: var(--down); }

            .weight-input {
                width: 52px;
                font-family: 'IBM Plex Mono', monospace;
                font-size: 0.85rem;
                padding: 5px 7px;
                border: 1px solid var(--border);
                border-radius: 6px;
                background: var(--surface);
                color: var(--ink);
            }

            /* Mobile: phones and small tablets. Placed last so it correctly
               overrides the desktop rules above at matching specificity. */
            @media (max-width: 640px) {
                .site-header {
                    flex-wrap: wrap;
                    gap: 10px;
                    padding: 14px 18px;
                }
                nav.site-nav {
                    flex-wrap: wrap;
                    gap: 2px;
                    width: 100%;
                }
                nav.site-nav a { padding: 6px 9px; font-size: 0.8rem; }
                .brand-tag { display: none; }

                main { padding: 28px 16px 60px; }
                .card-section { padding: 20px 18px; }

                .stat-grid { grid-template-columns: 1fr 1fr; }

                .segmented { flex-wrap: wrap; width: 100%; }
                .segmented button { flex: 1 1 auto; text-align: center; }

                .test-controls { flex-wrap: wrap; }
                .test-controls input { width: 90px; }

                table { font-size: 0.82rem; }
                th, td { padding: 8px 8px; white-space: nowrap; }
            }
        </style>
    </head>
    <body>
        <header class="site-header">
            <div class="brand">
                <span class="brand-name">Traffic Control</span>
                <span class="brand-tag">Load Balancer Console</span>
            </div>
            <nav class="site-nav">
                <a href="#about">About</a>
                <a href="#algorithm">Algorithm</a>
                <a href="#servers">Servers</a>
                <a href="#test">Traffic Test</a>
                <a href="#analytics">Analytics</a>
            </nav>
        </header>

        <main>
            <div class="page-intro">
                <p class="subhead">Routing strategy: <strong id="current-algo">—</strong></p>
            </div>

            <section id="about" class="plain-section">
                <p class="section-kicker">About</p>
                <h2>Intelligent Load Balancing &amp; Traffic Monitoring</h2>
                <hr class="section-divider">
                <p class="about-text">
                    This is a demo of an <strong>intelligent load balancing system</strong> that
                    distributes incoming traffic across multiple backend servers using different
                    routing algorithms — Round Robin, Least Connections, and Weighted Round Robin.
                    It includes live health checks with automatic failover, runtime weight adjustment,
                    and request analytics.
                </p>
            </section>

            <div class="two-col">
                <section id="algorithm" class="card-section">
                    <p class="section-kicker">Routing Strategy</p>
                    <h2>Algorithm</h2>
                    <hr class="section-divider">
                    <div class="segmented" id="segmented"></div>
                    <div id="switch-msg"></div>
                    <div class="algo-explainer">
                        <p class="algo-explainer-label">How <span id="algo-explain-name">this algorithm</span> works</p>
                        <p id="algo-explain-text">—</p>
                    </div>
                </section>

                <section id="test" class="card-section">
                    <p class="section-kicker">Verification</p>
                    <h2>Traffic Distribution Test</h2>
                    <hr class="section-divider">
                    <div class="test-controls">
                        <input type="number" id="req-count" value="20" min="1" max="200">
                        <button class="run-btn" id="run-test-btn" onclick="runTest()">Run traffic test</button>
                    </div>
                    <div id="chart"></div>
                    <p class="note" id="test-note"></p>
                </section>
            </div>

            <div class="two-col">
                <section id="servers" class="card-section">
                    <p class="section-kicker">Infrastructure</p>
                    <h2>Backend Servers</h2>
                    <hr class="section-divider">
                    <div class="table-scroll">
                        <table>
                            <thead><tr><th>Address</th><th>Status</th><th>Weight</th><th>Requests Handled</th></tr></thead>
                            <tbody id="backend-rows"></tbody>
                        </table>
                    </div>
                    <div id="backend-msg" class="note"></div>
                </section>

                <section id="analytics" class="card-section">
                    <p class="section-kicker">Observability</p>
                    <h2>Request Analytics</h2>
                    <hr class="section-divider">
                    <p class="refresh-note" id="refresh-note"></p>
                    <div class="stat-grid">
                        <div class="stat-card">
                            <div class="stat-label">Total Requests</div>
                            <div class="stat-value" id="stat-total">—</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Avg Response Time</div>
                            <div class="stat-value" id="stat-latency">—</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Throughput</div>
                            <div class="stat-value" id="stat-throughput">—</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Error Rate</div>
                            <div class="stat-value" id="stat-errors">—</div>
                        </div>
                    </div>

                    <div class="chart-wrap">
                        <canvas id="latency-chart" height="90"></canvas>
                    </div>

                    <div class="table-scroll">
                        <table>
                            <thead><tr><th>Time</th><th>Backend</th><th>Status</th><th>Latency</th></tr></thead>
                            <tbody id="log-rows"></tbody>
                        </table>
                    </div>
                </section>
            </div>
        </main>

        <footer class="site-footer">
            <p class="about-disclaimer">
                Disclaimer: this project was built for academic purposes as part of a University
                networking project. It is not intended for any other use.
            </p>
        </footer>

        <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
        <script>
            const REFRESH_INTERVAL_MS = 5000;
            const algorithms = ["round_robin", "least_connections", "weighted_round_robin"];
            const labels = {round_robin: "Round Robin", least_connections: "Least Connections", weighted_round_robin: "Weighted Round Robin"};
            const descriptions = {
                round_robin: "Requests are handed out in a fixed rotating order — server 1, then 2, then 3, then back to 1 — so every healthy backend gets an equal share over time regardless of how fast or slow it responds.",
                least_connections: "Each request is sent to whichever healthy backend currently has the fewest active connections. This helps route around a server that's mid-way through slower requests, rather than piling more work onto it.",
                weighted_round_robin: "Like Round Robin, but backends can be given different weights. A backend with weight 3 receives roughly three times as many requests as one with weight 1 — useful when servers have different capacities.",
            };
            const barColors = ["#2c3e6b", "#4f7a6d", "#a67c3d"];
            let latestBackends = [];

            async function refreshStatus() {
                const res = await fetch('/lb/status');
                const data = await res.json();
                latestBackends = data.backends;
                document.getElementById('current-algo').textContent = labels[data.algorithm] || data.algorithm;
                document.getElementById('algo-explain-name').textContent = labels[data.algorithm] || data.algorithm;
                document.getElementById('algo-explain-text').textContent = descriptions[data.algorithm] || '';

                const seg = document.getElementById('segmented');
                seg.innerHTML = '';
                algorithms.forEach(algo => {
                    const btn = document.createElement('button');
                    btn.className = algo === data.algorithm ? 'active' : '';
                    btn.textContent = labels[algo];
                    btn.onclick = () => switchAlgorithm(algo);
                    seg.appendChild(btn);
                });

                const rows = document.getElementById('backend-rows');
                rows.innerHTML = data.backends.map(b => `
                    <tr>
                        <td class="mono">${b.url}</td>
                        <td class="${b.healthy ? 'status-healthy' : 'status-down'}"><span class="dot ${b.healthy ? 'healthy' : 'down'}"></span>${b.healthy ? 'Healthy' : 'Down'}</td>
                        <td><input class="weight-input" type="number" min="1" value="${b.weight}"
                                onchange="updateWeight('${b.url}', this.value)"></td>
                        <td>${b.total_requests}</td>
                    </tr>
                `).join('');
            }

            async function updateWeight(url, weight) {
                const res = await fetch('/lb/weight', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({url: url, weight: parseInt(weight) || 1})
                });
                const data = await res.json();
                document.getElementById('backend-msg').textContent = res.ok
                    ? `Weight updated: ${url} → ${data.weight}`
                    : `Error: ${data.error}`;
                refreshStatus();
            }

            async function switchAlgorithm(algo) {
                const res = await fetch('/lb/algorithm', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({algorithm: algo})
                });
                const data = await res.json();
                document.getElementById('switch-msg').textContent = res.ok
                    ? `${labels[data.previous] || data.previous} → ${labels[data.algorithm] || data.algorithm}`
                    : `Error: ${data.error}`;
                refreshStatus();
            }

            async function runTest() {
                const btn = document.getElementById('run-test-btn');
                const n = parseInt(document.getElementById('req-count').value) || 20;
                btn.disabled = true;
                btn.textContent = 'Running...';

                const tally = {};
                for (let i = 0; i < n; i++) {
                    try {
                        const res = await fetch('/');
                        const server = res.headers.get('x-served-by') || 'unknown';
                        tally[server] = (tally[server] || 0) + 1;
                    } catch (e) { /* skip failed request */ }
                }

                renderChart(tally, n);
                btn.disabled = false;
                btn.textContent = 'Run traffic test';
            }

            function renderChart(tally, total) {
                const chart = document.getElementById('chart');
                const urls = latestBackends.map(b => b.url);
                const max = Math.max(...Object.values(tally), 1);

                chart.innerHTML = urls.map((url, i) => {
                    const count = tally[url] || 0;
                    const pct = total ? Math.round((count / total) * 100) : 0;
                    const widthPct = Math.round((count / max) * 100);
                    return `
                        <div class="bar-row">
                            <div class="bar-meta">
                                <span class="url">${url}</span>
                                <span class="count">${count} <span style="color: var(--ink-soft); font-family: 'Inter'; font-weight: 400; font-size: 0.8rem;">(${pct}%)</span></span>
                            </div>
                            <div class="bar-track">
                                <div class="bar-fill" style="width: ${widthPct}%; background: ${barColors[i % barColors.length]};"></div>
                            </div>
                        </div>
                    `;
                }).join('');

                const algo = document.getElementById('current-algo').textContent;
                document.getElementById('test-note').textContent =
                    `Sent ${total} requests under ${algo}. ` +
                    (algo === 'Weighted Round Robin' ? 'Split should roughly track each backend\\'s weight.' :
                     algo === 'Round Robin' ? 'Split should be roughly even across healthy backends.' :
                     'Split favors whichever backend has fewer active connections at request time.');

                // The traffic test itself generates real logged requests, so refresh analytics too.
                refreshAnalytics();
            }

            let latencyChart = null;

            async function refreshAnalytics() {
                const [analyticsRes, logsRes] = await Promise.all([
                    fetch('/lb/analytics'),
                    fetch('/lb/logs?limit=15')
                ]);
                const analytics = await analyticsRes.json();
                const logsData = await logsRes.json();

                document.getElementById('stat-total').textContent = analytics.total_requests;
                document.getElementById('stat-latency').textContent = analytics.avg_response_time_ms + ' ms';
                document.getElementById('stat-throughput').textContent = analytics.throughput_rps + '/s';
                document.getElementById('stat-errors').textContent = analytics.error_rate + '%';

                const formatTime = (iso) => new Date(iso).toLocaleTimeString(undefined, {
                    hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3
                });

                const rows = document.getElementById('log-rows');
                rows.innerHTML = logsData.logs.map(l => {
                    const badge = l.success
                        ? `<span class="status-badge ok">${l.status_code}</span>`
                        : `<span class="status-badge err">error</span>`;
                    return `
                        <tr>
                            <td class="mono">${formatTime(l.timestamp)}</td>
                            <td class="mono">${l.backend}</td>
                            <td>${badge}</td>
                            <td>${l.response_time_ms} ms</td>
                        </tr>
                    `;
                }).join('') || '<tr><td colspan="4" class="note">No requests yet — click "Run traffic test" above or hit the app directly.</td></tr>';

                // Chart wants oldest-to-newest, but /lb/logs returns newest-first.
                const chronological = [...logsData.logs].reverse();
                const chartLabels = chronological.map(l => formatTime(l.timestamp));

                // Color-code by backend so it's visually obvious which server handled which request,
                // matching the same palette used in the traffic distribution bar chart.
                const backendUrls = latestBackends.length
                    ? latestBackends.map(b => b.url)
                    : [...new Set(chronological.map(l => l.backend))];

                const datasets = backendUrls.map((url, i) => ({
                    label: url,
                    data: chronological.map(l => l.backend === url ? l.response_time_ms : null),
                    borderColor: barColors[i % barColors.length],
                    backgroundColor: barColors[i % barColors.length],
                    tension: 0.3,
                    spanGaps: true,
                    pointRadius: 3,
                    fill: false,
                }));

                const ctx = document.getElementById('latency-chart').getContext('2d');
                if (latencyChart) latencyChart.destroy();
                latencyChart = new Chart(ctx, {
                    type: 'line',
                    data: { labels: chartLabels, datasets: datasets },
                    options: {
                        responsive: true,
                        animation: { duration: 250 },
                        plugins: {
                            legend: {
                                display: true,
                                position: 'bottom',
                                labels: { font: { family: 'IBM Plex Mono', size: 10 }, boxWidth: 10 }
                            }
                        },
                        scales: {
                            x: { display: false },
                            y: {
                                beginAtZero: true,
                                title: { display: true, text: 'Response Time (ms)', font: { family: 'Inter', size: 11 } },
                                ticks: { font: { family: 'IBM Plex Mono' } }
                            }
                        }
                    }
                });
            }

            refreshStatus();
            refreshAnalytics();
            document.getElementById('refresh-note').textContent =
                `Auto-refreshes every ${REFRESH_INTERVAL_MS / 1000} seconds`;
            setInterval(refreshStatus, REFRESH_INTERVAL_MS);
            setInterval(refreshAnalytics, REFRESH_INTERVAL_MS);
        </script>
    </body>
    </html>
    """
    return Response(content=html, media_type="text/html")


 
# Main reverse-proxy route: forwards any request to a chosen backend,
# retries once on a different backend if the first choice fails (failover).
 

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(path: str, request: Request):
    body = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)

    tried = set()
    last_error = None

    for _attempt in range(2):  # try selected backend, then one failover retry
        backend = select_backend()
        if backend is None or backend.url in tried:
            break
        tried.add(backend.url)

        backend.active_connections += 1
        start = time.time()
        try:
            resp = await client.request(
                request.method,
                f"{backend.url}/{path}",
                headers=headers,
                content=body,
                params=dict(request.query_params),
            )
            elapsed_ms = round((time.time() - start) * 1000, 2)
            logger.info(f"{request.method} /{path} -> {backend.url} [{resp.status_code}] {elapsed_ms}ms")
            backend.total_requests += 1
            backend.total_latency_ms += elapsed_ms
            GlobalStats.total_requests += 1
            GlobalStats.total_latency_ms += elapsed_ms
            REQUEST_LOG.append({
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "epoch": time.time(),
                "method": request.method,
                "path": "/" + path,
                "backend": backend.url,
                "algorithm": LBState.algorithm,
                "status_code": resp.status_code,
                "response_time_ms": elapsed_ms,
                "success": True,
            })
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers={"X-Served-By": backend.url, "X-LB-Algorithm": LBState.algorithm},
                media_type=resp.headers.get("content-type"),
            )
        except Exception as e:
            elapsed_ms = round((time.time() - start) * 1000, 2)
            last_error = e
            backend.consecutive_failures += 1
            backend.total_errors += 1
            GlobalStats.total_requests += 1
            GlobalStats.total_errors += 1
            GlobalStats.total_latency_ms += elapsed_ms
            if backend.consecutive_failures >= UNHEALTHY_THRESHOLD:
                backend.healthy = False
                logger.warning(f"Backend marked UNHEALTHY mid-request (failover): {backend.url}")
            logger.warning(f"Request to {backend.url} failed, retrying with another backend: {e}")
            REQUEST_LOG.append({
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "epoch": time.time(),
                "method": request.method,
                "path": "/" + path,
                "backend": backend.url,
                "algorithm": LBState.algorithm,
                "status_code": None,
                "response_time_ms": elapsed_ms,
                "success": False,
            })
        finally:
            backend.active_connections -= 1

    return Response(
        content=f'{{"error": "All backends unavailable", "detail": "{last_error}"}}',
        status_code=502,
        media_type="application/json",
    )