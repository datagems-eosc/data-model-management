# Install uv
FROM python:3.12-slim

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:0.7.4 /uv /bin/uv

# The user the containers run as, with a fixed uid and gid that the
# deployment's securityContext names. Nothing in the image belongs to it, so
# at run time it writes only to the mounted /logs and /s3, and to /tmp
RUN groupadd --gid 10001 dmm \
    && useradd --uid 10001 --gid 10001 --no-log-init --no-create-home \
        --home-dir /home/dmm --shell /usr/sbin/nologin dmm

# Compile bytecode while installing, since the user cannot write `__pycache__`
ENV UV_COMPILE_BYTECODE=1

# Change the working directory to the `app` directory
WORKDIR /app

# Copy the lockfile, `pyproject.toml` and the README into the image
COPY uv.lock /app/uv.lock
COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md

# Install dependencies
RUN uv sync --frozen --no-install-project

# Copy the dmm_api folder into the image
COPY dmm_api /app/dmm_api

# Sync the project
RUN uv sync --frozen

# DuckDB keeps extensions under $HOME. The postgres extension is installed
# here, into a home only root can write, so a query never downloads it
ENV HOME=/home/dmm
RUN mkdir -p /home/dmm \
    && /app/.venv/bin/python -c "import duckdb; duckdb.sql('INSTALL postgres; LOAD postgres')"

ENV PYTHONDONTWRITEBYTECODE=1

USER 10001:10001

# Run the environment built above. `uv run` would check it and sync it, which
# writes to /app and to a cache under $HOME
CMD ["/app/.venv/bin/uvicorn", "dmm_api.main:app", "--host", "0.0.0.0", "--port", "5000"]
