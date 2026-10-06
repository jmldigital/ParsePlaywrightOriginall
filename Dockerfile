FROM ghcr.io/astral-sh/uv:0.10.0 AS uv
FROM mcr.microsoft.com/playwright/python:v1.58.0-noble
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright PATH="/app/.venv/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY --chown=pwuser:pwuser price_api ./price_api
RUN mkdir -p /app/data && chown pwuser:pwuser /app/data
USER pwuser
EXPOSE 8000
CMD ["uvicorn", "price_api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
