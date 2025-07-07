# Install uv
FROM python:3.12-slim

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Change the working directory to the `app` directory
WORKDIR /app

# Copy the lockfile and `pyproject.toml` into the image
COPY uv.lock /app/uv.lock
COPY pyproject.toml /app/pyproject.toml
#COPY README.md /app/README.md

# Install dependencies
RUN uv sync --frozen --no-install-project

# Copy only the dmm_api folder into the image
#COPY dmm_api /app/dmm_api
COPY . /app

# Sync the project
RUN uv sync --frozen

# Expose the port
EXPOSE 5000

CMD ["uv", "run", "uvicorn", "dmm_api.main:app", "--host", "0.0.0.0", "--port", "5000"]
