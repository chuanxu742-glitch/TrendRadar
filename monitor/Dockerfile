FROM projectdiscovery/katana:v1.6.1 AS katana

FROM wantcat/trendradar:latest

USER root

COPY --from=katana /usr/local/bin/katana /usr/local/bin/katana

RUN apt-get update \
    && apt-get install -y --no-install-recommends musl \
    && rm -rf /var/lib/apt/lists/* \
    && /app/.venv/bin/python -m ensurepip \
    && /app/.venv/bin/python -m pip install --no-cache-dir \
        "scrapling[fetchers]==0.4.10" \
        "pypdf==6.14.2" \
        "tzdata==2025.2" \
    && /app/.venv/bin/scrapling install
