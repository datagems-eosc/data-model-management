# Logging

The API logs to stdout and to files on a disk claim. stdout is what
`kubectl logs` shows and what the platform's ELK stack collects, for a limited
time. The files stay: each finished week is archived to S3.

## Finding a request

Every response carries an `x-tracking-correlation` header, and every error
body a `correlationId`:

```json
{"code": 424, "error": "Authentication service returned an error.", "correlationId": "5b1e0c9a-3f4d-4e2b-9a71-0c2d8e6f1a33"}
```

To look into a failed call, ask whoever made it for that ID and the
approximate time. Days not archived yet are in `LOGS_DIR`, on the disk claim;
earlier weeks are in `LOG_ARCHIVE_DIR`, on S3. `zgrep` and `zcat -f` read plain
and gzipped files alike, so the same commands work in both:

```sh
cd /logs
zgrep -h 5b1e0c9a dmm-api_*
zcat -f dmm-api_2026-11-02_* | jq -c 'select(.outcome == "server_error")'
cd /s3/data-model-management/logs
zcat -f dmm-api_week_2026-10-26*.log.gz | jq -c 'select(.level == "warning")'
zcat -f dmm-api_week_* | jq -c 'select(.user_id == "16a5380f-4787-44dd-a552-c7095f2d12d1")'
```

The ID is taken from the request's `x-tracking-correlation` header when it is
1 to 128 characters of `A-Z a-z 0-9 . _ -`. Otherwise the API generates a
UUID4. A caller that sends its own ID can therefore find its requests in these
logs under the same ID.

FastAPI's own validation errors (422) keep their `{"detail": [...]}` body; the
ID is in their header only. So is it for paths that match no route (404).

## Entries

Each line, on stdout and in the files, is one JSON object. These eight
fields come first, in this order, on every entry:

| Field | Type | Content |
| - | - | - |
| `timestamp` | string | UTC, ISO 8601 with milliseconds: `2026-11-02T10:15:03.412Z` |
| `level` | string | `debug`, `info`, `warning`, `error` or `critical` |
| `event` | string | The message |
| `logger` | string | The module that logged it, e.g. `dmm_api.resources.dataset`; `uvicorn.error` for the server's own messages |
| `app_version` | string | The release, e.g. `0.2.0` |
| `pod` | string | The pod name |
| `correlation_id` | string or `null` | The request's ID; `null` outside a request |
| `user_id` | string or `null` | The `sub` claim of the caller's token; `null` before authentication or without it |

Then come the fields the log call passed, such as `dataset_id` or
`status_code`, and last, only when an exception was logged, `exception` with
the full traceback.

### The request summary

Every request ends with one entry whose `event` is `request finished`, with
`method`, `path` (without the query string), `status_code`, `duration_ms` and
`outcome`:

| `outcome` | When | `level` |
| - | - | - |
| `success` | status below 400 | `info` |
| `client_error` | 400 to 499, except 424 | `warning` |
| `server_error` | 500 and above, 424 (a service the API calls failed), or an unhandled exception | `error` |

An unhandled exception is logged first, at `error` with its traceback, and
answered with `{"code": 500, "error": "Internal server error", "correlationId": "..."}`.

Inside a request, `info` marks a step that completed, `warning` something that
went wrong while the request still succeeded (for example, storing the
Analytical Pattern log), and `error` a failure that fails the request.

### Example

A failed dataset update, as it appears on stdout and in a file:

```json
{"timestamp": "2026-11-02T10:15:02.601Z", "level": "info", "event": "Requesting dataset metadata from MoMa", "logger": "dmm_api.resources.dataset", "app_version": "0.2.0", "pod": "data-model-management-7c9f-x2k4q", "correlation_id": "5b1e0c9a-3f4d-4e2b-9a71-0c2d8e6f1a33", "user_id": "16a5380f-4787-44dd-a552-c7095f2d12d1", "dataset_id": "729f8932-8300-4ac5-a71e-b41904ca7433"}
{"timestamp": "2026-11-02T10:15:03.412Z", "level": "error", "event": "MoMa API HTTP error while fetching dataset metadata", "logger": "dmm_api.resources.dataset", "app_version": "0.2.0", "pod": "data-model-management-7c9f-x2k4q", "correlation_id": "5b1e0c9a-3f4d-4e2b-9a71-0c2d8e6f1a33", "user_id": "16a5380f-4787-44dd-a552-c7095f2d12d1", "dataset_id": "729f8932-8300-4ac5-a71e-b41904ca7433", "status_code": 502}
{"timestamp": "2026-11-02T10:15:03.419Z", "level": "error", "event": "request finished", "logger": "dmm_api.main", "app_version": "0.2.0", "pod": "data-model-management-7c9f-x2k4q", "correlation_id": "5b1e0c9a-3f4d-4e2b-9a71-0c2d8e6f1a33", "user_id": "16a5380f-4787-44dd-a552-c7095f2d12d1", "method": "PUT", "path": "/api/v1/dataset/update", "status_code": 424, "duration_ms": 818.2, "outcome": "server_error"}
```

## Files

```
/logs/                                                     # LOGS_DIR, the disk claim
  dmm-api_2026-11-03_data-model-management-7c9f-x2k4q.log  # one pod, one day
/s3/data-model-management/logs/                            # LOG_ARCHIVE_DIR, on S3
  dmm-api_week_2026-10-26.log.gz                           # one week, all pods
```

| File | Name | Written |
| - | - | - |
| Daily | `dmm-api_<day>_<pod>.log`, under `LOGS_DIR` | by each pod, one entry at a time |
| Weekly archive | `dmm-api_week_<Monday>.log.gz`, under `LOG_ARCHIVE_DIR` | by the CronJob, Mondays 01:00 UTC |

- Days are UTC; weeks run Monday to Sunday.
- Each pod appends to its own file, so the two pods that overlap during a
  rolling update never write to the same one. A container restarted in the
  same pod keeps the pod name and appends to the same file; a line cut short
  by a crash is ended before the next entry.
- Daily files are plain text. A weekly archive holds every pod's entries for
  the week, in timestamp order.
- An archive is written once and never changed. A week that gets a second
  archive (see below) has it named `dmm-api_week_<Monday>-2.log.gz`, then
  `-3`, and so on.

The API pods and the CronJob mount the claim at the same time, so it must be
ReadWriteMany. Nothing deletes an archive: expiring old ones, if wanted, is an
S3 lifecycle rule on `logs/`.

### What can be missing from the files

- Each entry is formatted in the request and handed to a background thread
  that appends it to the file, so a slow disk never slows a request. A pod
  killed without warning loses only the entries still waiting, normally none.
  stdout still had them.
- At most 50,000 entries wait. If the disk falls further behind, or cannot be
  written, new entries reach stdout only. A warning on stdout says so, and a
  second one gives the number missed once writing resumes.
- On a normal stop, the API writes what is waiting during shutdown, for at
  most 10 seconds; a warning on stdout gives the number left unwritten.
  uvicorn's last two messages ("Application shutdown complete", "Finished
  server process") come after that and reach stdout only.
- If `LOGS_DIR` cannot be created or written when the API starts, a warning
  goes to stdout and the API logs to stdout only.

Problems with the log files themselves are reported on stdout only.

## Settings

| Variable | Default | Meaning |
| - | - | - |
| `LOGS_DIR` | `/logs` | Folder of the daily files, on the disk claim |
| `LOG_ARCHIVE_DIR` | `/s3/data-model-management/logs` | Folder of the weekly archives, on the S3 mount; read by the archive job only |
| `LOG_LEVEL` | `INFO` | Lowest level written, to stdout and files: `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` |
| `HOSTNAME` | set by Kubernetes to the pod name | The pod name in entries and file names; characters other than `A-Z a-z 0-9 . -` become `-` |

For a local run, point `LOGS_DIR` at a folder in the working copy, such as
`./logs`, which git ignores.

## The weekly archive job

`python -m dmm_api.tools.log_archive` archives every week that ended before the
current one and still has daily files:

1. It merges all pods' daily files for the week, in timestamp order, into
   `dmm-api_week_<Monday>.log.gz` under `LOG_ARCHIVE_DIR`.
2. It reads the archive back and checks the line count.
3. It deletes the week's daily files.

If writing or checking the archive fails, the job deletes the archive and
keeps the daily files, and the next run tries again. The job exits with status
1 when any week failed, so Kubernetes shows the run as failed. A missed run is
caught up by the next one. The job logs to stdout only.

A failure can leave an entry in two archives, never in none:

- A daily file that could not be deleted, that grew while the job read it, or
  that was written after its week was archived, goes into that week's next
  archive at the next run.
- A failed archive that could not be deleted stays beside the next one. It
  may look complete; the job's error on stdout, "could not delete a failed
  weekly log archive", names it.

Deleting a failed archive needs an S3 mount that allows deletes.

The CronJob runs the same image as the API, at the same tag, so bump both
together. Like the API, it runs as the image's user, uid and gid 10001:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: data-model-management-log-archive
  namespace: univr
  labels:
    app: data-model-management
spec:
  schedule: "0 1 * * 1"
  timeZone: "Etc/UTC"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          restartPolicy: Never
          securityContext:
            runAsNonRoot: true
            runAsUser: 10001
            runAsGroup: 10001
            seccompProfile:
              type: RuntimeDefault
          imagePullSecrets:
            - name: ghcr-pull-secret
          volumes:
            - name: logs-volume
              persistentVolumeClaim:
                claimName: <the log claim>
            - name: s3-volume
              persistentVolumeClaim:
                claimName: dg-s3-dev-pvc-univr
          containers:
            - name: log-archive
              image: ghcr.io/datagems-eosc/datagems-eosc/data-model-management:<same tag as the API>
              command: ["/app/.venv/bin/python", "-m", "dmm_api.tools.log_archive"]
              securityContext:
                allowPrivilegeEscalation: false
                capabilities:
                  drop: ["ALL"]
              env:
                - name: LOGS_DIR
                  value: "/logs"
                - name: LOG_ARCHIVE_DIR
                  value: "/s3/data-model-management/logs"
              volumeMounts:
                - name: logs-volume
                  mountPath: /logs
                - name: s3-volume
                  mountPath: /s3
```

`LOGS_DIR` must match the API's, and the API's Deployment mounts the same
claim there. `backoffLimit: 0` keeps a failed run from being retried at once;
the next week's run picks up what it left.
