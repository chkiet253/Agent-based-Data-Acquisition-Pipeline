FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY agents/storage_agent/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY agents /app/agents

EXPOSE 8003

HEALTHCHECK --interval=30s --timeout=10s \
    CMD curl -f http://localhost:8003/health || exit 1

CMD ["python", "agents/storage_agent/main.py"]
