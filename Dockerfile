FROM python:3.11-slim

WORKDIR /app

# system deps (timezone for cron-like scheduling)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata curl \
    && rm -rf /var/lib/apt/lists/*
ENV TZ=Asia/Shanghai

# python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# app code
COPY app.py ./
COPY scripts/ ./scripts/
COPY templates/ ./templates/
COPY static/ ./static/

# create mount points (volumes will overlay these at runtime)
RUN mkdir -p /app/data /app/qlib_data/cn_data /app/qlib_data/csv_tmp/tushare_daily

EXPOSE 5055

HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:5055/api/health || exit 1

CMD ["python", "-u", "app.py"]
