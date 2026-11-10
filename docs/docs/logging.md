# Logging

The service logs to stdout, one JSON object per line, in the
[json-cf-2](https://datagems-eosc.github.io/dg-logging/latest/logging-formats/)
format of the
[DataGEMS Logging Service](https://datagems-eosc.github.io/dg-logging/latest/),
which collects the entries and makes these fields searchable:

| Field | Content |
| - | - |
| `timestamp` | UTC, ISO 8601 with milliseconds |
| `level` | `debug`, `info`, `warning`, `error` or `critical` |
| `msg` | The message |
| `DGCorrelationId` | The request's correlation identifier |
| `UserId` | The `sub` claim of the caller's token |
| `ClientId` | The `azp` claim of the caller's token |

Each request ends with one summary entry, `request finished`, with the
method, path, status code and duration.

## Correlation identifier

A request is logged under the identifier in its `x-tracking-correlation`
header, when that is 1 to 128 characters of `A-Z a-z 0-9 . _ -`. Otherwise the
service generates a UUID4. The service:

- returns the identifier in the `x-tracking-correlation` response header, and
  in the `correlationId` field of its error bodies;
- passes it on, in the same header, to MoMa and the other DataGEMS services it
  calls, so that one request can be followed across services.

To report a problem, quote that identifier and the approximate time.

## Details

[LOGGING.md](https://github.com/datagems-eosc/data-model-management/blob/main/LOGGING.md),
in the code repository, lists every field and describes the daily log files,
the weekly archive on S3, and how to search them.
