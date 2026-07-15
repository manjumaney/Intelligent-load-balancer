FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend_app.py lb_app.py main.py .

# ALGORITHM: round_robin | least_connections | weighted_round_robin
ENV ALGORITHM=round_robin
ENV BACKEND_SERVERS="http://localhost:8001:1,http://localhost:8002:2,http://localhost:8003:1"
ENV HEALTH_CHECK_INTERVAL=5
ENV UNHEALTHY_THRESHOLD=2

# Render (and most PaaS platforms) inject PORT at runtime; default here is for local use
ENV PORT=9000
EXPOSE 9000

CMD ["python", "main.py"]
