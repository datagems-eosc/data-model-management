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

To find a request in the logs, and for the log format, see [LOGGING.md](LOGGING.md).

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
    - `https://datagems-dev.scayle.es/dmm/api/v1/polyglot/query/result/{dataset_id}` - Returns the file content of a query result

- **Authentication test utilities**
    - `https://datagems-dev.scayle.es/dmm/api/v1/authtest` — Validates bearer token and echoes request context.
---

## Running the API Locally
You can run the API either directly via Python or using Docker.

The API needs the URLs of the services it calls (see
[Required service URLs](#required-service-urls)). `dev.env` holds those of the
DataGEMS dev deployment. `docker compose up` loads it; for any other way of
running the API, load it into the shell first:

```bash
set -a; . ./dev.env; set +a
uv run uvicorn dmm_api.main:app --port 5000
```

### Docker

`docker compose up` builds the image and starts it on port 5050, with Grafeo.
To run the image on its own:

```bash
docker build -t dmm-api .
docker run -d -p 5000:5000 --env-file dev.env --name dmm-api dmm-api
```

The image runs as uid and gid 10001 and writes only to `/logs`, `/s3` and
`/tmp`. Mount writable volumes at `/logs` and `/s3` to keep the log files and
the dataset files; without a writable `/logs`, the API logs to stdout only.
Set `GRAFEO_URL` when Grafeo is not on `localhost:7474` as seen from the
container.

#### Required Service URLs

The API refuses to start, and logs which ones are missing, unless all of these
are set:

| Variable | Service |
|---|---|
| `OIDC_ISSUER_URL` | The login server's realm, used to validate tokens |
| `MOMA_URL` | MoMa |
| `CDD_URL` | Cross-dataset discovery |
| `IDD_URL` | In-dataset discovery |
| `QD_URL` | Query disambiguation |
| `REC_SYS_URL` | Dataset recommendation |

A trailing `/` is optional on all of them.

#### Required Environment Variables (No Defaults)

The following environment variables are read by the API **without defaults**.

| Variable | Used by | Required when | Secret |
|---|---|---|---|
| `DATASET_DIR` | Dataset load flow in `dmm_api/resources/dataset.py` | Using `PUT /api/v1/dataset/load` | No |
| `DATAGEMS_POSTGRES_HOST` | PostgreSQL and mixed queries in `dmm_api/resources/dataset.py` | Using `POST /api/v1/polyglot/query` on PostgreSQL or mixed sources | No |
| `DATAGEMS_POSTGRES_PORT` | PostgreSQL and mixed queries in `dmm_api/resources/dataset.py` | Using `POST /api/v1/polyglot/query` on PostgreSQL or mixed sources | No |
| `DS_READER_USER` | PostgreSQL and mixed queries in `dmm_api/resources/dataset.py` | Using `POST /api/v1/polyglot/query` on PostgreSQL or mixed sources | Yes |
| `DS_READER_PS` | PostgreSQL and mixed queries in `dmm_api/resources/dataset.py` | Using `POST /api/v1/polyglot/query` on PostgreSQL or mixed sources | Yes |

`DS_READER_USER` and `DS_READER_PS` should never be hardcoded in image layers or committed files.

#### Optional Environment Variables

| Variable | Default | Meaning |
|---|---|---|
| `GRAFEO_URL` | `http://localhost:7474` | Grafeo's HTTP endpoint |
| `OIDC_CLIENT_ID` | `data-model-management-api` | The audience (`aud`) a token must contain |
| `OIDC_JWKS_URL` | `<OIDC_ISSUER_URL>/protocol/openid-connect/certs` | The login server's public keys |
| `JWKS_CACHE_TTL_SECONDS` | `300` | How long the public keys are cached, in seconds |
| `SCRATCHPAD_DIR` | `/s3/scratchpad` | Uploaded dataset files, before `PUT /api/v1/dataset/load` |
| `DATASET_STAGING_DIR` | `/s3/dataset_staging` | Dataset files collected by the onboarding, before `PUT /api/v1/dataset/load` |
| `RESULTS_DIR`, `RESULTS_FOLDER` | `/s3/data-model-management`, `results` | Query results, under `RESULTS_DIR/RESULTS_FOLDER` |
| `CATALOGUE_DIR`, `CATALOGUE_FOLDER` | `/s3/data-model-management`, `catalogue` | The catalogue, under `CATALOGUE_DIR/CATALOGUE_FOLDER` |
| `MAX_UPLOAD_MB` | `64` | The largest request body and uploaded file accepted, in MB |
| `ROOT_PATH` | empty | The path prefix when the API is served behind a proxy |

The logging variables are in [LOGGING.md](LOGGING.md#settings).

#### Secrets with Vault (Kubernetes)

If you use Vault in Kubernetes, expose secrets into a Kubernetes Secret (for example via `VaultStaticSecret`) and bind them as env vars in the Deployment.

```yaml
env:
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
