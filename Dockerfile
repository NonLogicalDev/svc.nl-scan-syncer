FROM ghcr.io/astral-sh/uv:python3.12-alpine

# Install dependencies (alpine)
RUN apk add --no-cache \
    bash \
    sqlite \
    rsync \
    && rm -rf /var/cache/apk/*

COPY pyproject.toml .
COPY server.py .

VOLUME /app/data
VOLUME /app/syncer/src
VOLUME /app/syncer/dst1
VOLUME /app/syncer/dst2

EXPOSE 5040

# UV Lock & update
RUN uv sync

CMD ["uv", "run", "server.py"]