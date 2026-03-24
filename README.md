# dmm-api

Data Model & Management Platform API (WP5)

The official documentation is available at: https://datagems-eosc.github.io/data-model-management/latest/

## 1. Getting started with your project

### Set Up Your Development Environment

#### Linux/macOS

If you do not have `uv` installed, you can install it with

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
After executing the command above, you will need to restart your shell.

`uv` is a python package similar to `poetry`.

Then, install the environment and the pre-commit hooks with

```bash
make install
```

This will also generate your `uv.lock` file





### Windows

If you do not have `uv` installed, you can install it with

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
Or following the instructions at docs.astral.sh/uv/getting-started/installation/#installation-methods.

After executing the command above, you will need to restart your shell.

`uv` is a python package similar to `poetry`.

Then, install the environment and the pre-commit hooks with

```bash
uv sync
uv run pre-commit install
```

This will also generate your `uv.lock` file

---


## API Usage Examples
The API is available at:
https://datagems-dev.scayle.es/dmm/api/v1

You can interact with it using curl commands, going into the `tests` folder:

```bash
cd tests
```

For full request/response walkthroughs, see [API_USAGE.md](API_USAGE.md).

Example assets are organized under:
- `tests/register`, `tests/load`, `tests/update`;
- `tests/query`;
- `tests/in-dataset`, `tests/cross-dataset`;
- `tests/data` (sample files)

Primary endpoint URLs (grouped):

- **Dataset lifecycle**
    - `https://datagems-dev.scayle.es/dmm/api/v1/data-workflow` — Uploads dataset files to scratchpad (internal testing flow).
    - `https://datagems-dev.scayle.es/dmm/api/v1/dataset/register` — Registers a new dataset profile/metadata.
    - `https://datagems-dev.scayle.es/dmm/api/v1/dataset/load` — Moves dataset from scratchpad to permanent storage.
    - `https://datagems-dev.scayle.es/dmm/api/v1/dataset/update` — Updates dataset metadata/profiling information.

- **Dataset discovery**
    - `https://datagems-dev.scayle.es/dmm/api/v1/dataset/search` — Lists/filter datasets.
    - `https://datagems-dev.scayle.es/dmm/api/v1/dataset/get/{dataset_id}` — Returns one dataset by ID.

- **Query execution**
    - `https://datagems-dev.scayle.es/dmm/api/v1/polyglot/query` — Executes query workflows and stores results.

- **Authentication test utilities**
    - `https://datagems-dev.scayle.es/dmm/api/v1/authtest` — Validates bearer token and echoes request context.


---

## Running the API Locally
You can run the API either directly via Python or using Docker.

### Terminal

To start the API server, open your terminal and navigate to the dmm_api directory.

Run the api.py script:

```bash
cd dmm_api
python api.py
```

---

### Docker

Alternatively, run the API using Docker.

Use the provided Dockerfile to build the image:
```bash
docker build -t fastapi-image .
```
Start a container from the image and mount the results directory:
```bash
docker run -d -p 5000:5000 -v /path/to/your/local/results:/app/dmm_api/data/results --name fastapi-container fastapi-image
```
Replace `/path/to/your/local/results` with the actual path to your local results directory, e.g., `desktop/repositories/data-model-management/dmm_api/data/results`.

#### Required Environment Variables (No Defaults)

The following environment variables are read by the API **without defaults**.

| Variable | Used by | Required when | Secret |
|---|---|---|---|
| `OIDC_CLIENT_SECRET` | Token exchange in `dmm_api/security/auth.py` | Using `POST /api/v1/authtest/cdd-search` (or any token-exchange flow) | Yes |
| `CDD_URL` | External service mapping in `dmm_api/resources/dataset.py` | Using cross-dataset discovery routes via dataset resource | No |
| `IDD_URL` | External service mapping in `dmm_api/resources/dataset.py` | Using in-dataset discovery routes via dataset resource | No |
| `DATASET_DIR` | Dataset load flow in `dmm_api/resources/dataset.py` | Using `PUT /api/v1/dataset/load` | No |
| `DATAGEMS_POSTGRES_HOST` | Postgres test endpoint | Using `GET /api/v1/test-postgres-duckdb` | No |
| `DATAGEMS_POSTGRES_PORT` | Postgres test endpoint | Using `GET /api/v1/test-postgres-duckdb` | No |
| `DS_READER_USER` | Postgres test endpoint | Using `GET /api/v1/test-postgres-duckdb` | Yes |
| `DS_READER_PS` | Postgres test endpoint | Using `GET /api/v1/test-postgres-duckdb` | Yes |

Notes:
- For `authtest/cdd-search`, also review optional auth variables with defaults in `dmm_api/security/auth.py` (`OIDC_CLIENT_ID`, `OIDC_TOKEN_URL`, etc.).
- `OIDC_CLIENT_SECRET` should never be hardcoded in image layers or committed files.

#### Secrets with Vault (Kubernetes)

If you use Vault in Kubernetes, expose secrets into a Kubernetes Secret (for example via `VaultStaticSecret`) and bind them as env vars in the Deployment.

```yaml
env:
  - name: OIDC_CLIENT_SECRET
    valueFrom:
      secretKeyRef:
        name: dmm-api-secrets
        key: OIDC_CLIENT_SECRET
  - name: DS_READER_USER
    valueFrom:
      secretKeyRef:
        name: dmm-api-secrets
        key: DS_READER_USER
  - name: DS_READER_PS
    valueFrom:
      secretKeyRef:
        name: dmm-api-secrets
        key: DS_READER_PS
```

For local Docker runs, pass these with `-e` or `--env-file` (recommended for non-secret local setup only).

---

## License

See the [LICENSE](LICENSE) file for license rights and limitations (MIT).
