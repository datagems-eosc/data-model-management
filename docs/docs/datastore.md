# Data Stores

The service has no database of its own apart from Grafeo. What it writes goes to the stores below.

| Store | What it holds | How the service reaches it |
| - | - | - |
| MoMa | Dataset metadata and links between datasets | MoMa's HTTP API, with the caller's token |
| Grafeo | The Analytical Pattern logs | Cypher queries over HTTP |
| S3 storage | Uploaded dataset files waiting to be loaded (the scratchpad), loaded datasets, the catalogue, and query results | Files under `/s3` |
| PostgreSQL | Tables of datasets kept in the platform's database | Read-only, for SQL queries |
| Log disk | The service's daily log files, archived weekly to S3 storage | Files under `/logs`; see [Logging](logging.md) |

The folders on S3 storage and the Grafeo address are set by environment variables, listed under [Configuration](configuration.md).
