FROM python:3.11-slim

WORKDIR /app

# system deps (timezone + 编译 qlib/lightgbm 所需的 gcc/g++)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata curl build-essential \
    && rm -rf /var/lib/apt/lists/*
ENV TZ=Asia/Shanghai

# python deps (先装 Cython/numpy, 便于 pyqlib 从源码构建时找到)
COPY requirements.txt .
RUN pip install --no-cache-dir Cython "numpy==1.26.4" \
    && pip install --no-cache-dir -r requirements.txt

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
