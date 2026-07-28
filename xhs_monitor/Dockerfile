FROM python:3.12-slim-bookworm

WORKDIR /app

COPY --from=node:20-bookworm-slim /usr/local/bin/node /usr/local/bin/node
COPY --from=node:20-bookworm-slim /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s /usr/local/lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

RUN pip install --no-cache-dir \
    requests==2.33.0 \
    PyYAML==6.0.3 \
    PyExecJS==1.5.1 \
    loguru==0.7.3

COPY trendradar ./trendradar
COPY xhs_monitor ./xhs_monitor

ENV PYTHONUNBUFFERED=1 \
    XHS_MONITOR_CONFIG=/app/config/xhs-monitor.yaml \
    XHS_MONITOR_DATABASE=/app/state/xhs-monitor.db \
    XHS_MONITOR_PORT=8091

EXPOSE 8091

ENTRYPOINT ["python", "-m", "xhs_monitor.service"]
