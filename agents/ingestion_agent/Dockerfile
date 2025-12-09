FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY agents/ingestion_agent/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY agents /app/agents

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=10s \
    CMD curl -f http://localhost:8001/health || exit 1

CMD ["python", "agents/ingestion_agent/main.py"]
