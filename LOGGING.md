# Logging

The API logs to stdout and to files on the S3 mount. stdout is what
`kubectl logs` shows, and is gone with the pod. The files stay.

## Finding a request

Every response carries an `x-tracking-correlation` header, and every error
body a `correlationId`:

```json
{"code": 424, "error": "Authentication service returned an error.", "correlationId": "5b1e0c9a-3f4d-4e2b-9a71-0c2d8e6f1a33"}
```

To look into a failed call, ask whoever made it for that ID and the
approximate time. Then search the month's folder. `zgrep` and `zcat -f` read
plain and gzipped files alike, so the same commands work before and after a
week is archived:

```sh
cd /s3/data-model-management/logs/2026-11
zgrep -h 5b1e0c9a dmm-api_*
zcat -f dmm-api_2026-11-02* | jq -c 'select(.outcome == "server_error")'
zcat -f dmm-api_week_2026-10-26.log.gz | jq -c 'select(.level == "warning")'
zcat -f dmm-api_* | jq -c 'select(.user_id == "16a5380f-4787-44dd-a552-c7095f2d12d1")'
```

The ID is taken from the request's `x-tracking-correlation` header when it is
1 to 128 characters of `A-Z a-z 0-9 . _ -`. Otherwise the API generates a
UUID4. A caller that sends its own ID can therefore find its requests in these
logs under the same ID.

FastAPI's own validation errors (422) keep their `{"detail": [...]}` body; the
ID is in their header only. So is it for paths that match no route (404).

## Entries

Each file line is one JSON object. These eight fields come first, in this
order, on every entry:

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

A failed dataset update, as it appears in a file:

```json
{"timestamp": "2026-11-02T10:15:02.601Z", "level": "info", "event": "Requesting dataset metadata from MoMa", "logger": "dmm_api.resources.dataset", "app_version": "0.2.0", "pod": "data-model-management-7c9f-x2k4q", "correlation_id": "5b1e0c9a-3f4d-4e2b-9a71-0c2d8e6f1a33", "user_id": "16a5380f-4787-44dd-a552-c7095f2d12d1", "dataset_id": "729f8932-8300-4ac5-a71e-b41904ca7433"}
{"timestamp": "2026-11-02T10:15:03.412Z", "level": "error", "event": "MoMa API HTTP error while fetching dataset metadata", "logger": "dmm_api.resources.dataset", "app_version": "0.2.0", "pod": "data-model-management-7c9f-x2k4q", "correlation_id": "5b1e0c9a-3f4d-4e2b-9a71-0c2d8e6f1a33", "user_id": "16a5380f-4787-44dd-a552-c7095f2d12d1", "dataset_id": "729f8932-8300-4ac5-a71e-b41904ca7433", "status_code": 502}
{"timestamp": "2026-11-02T10:15:03.419Z", "level": "error", "event": "request finished", "logger": "dmm_api.main", "app_version": "0.2.0", "pod": "data-model-management-7c9f-x2k4q", "correlation_id": "5b1e0c9a-3f4d-4e2b-9a71-0c2d8e6f1a33", "user_id": "16a5380f-4787-44dd-a552-c7095f2d12d1", "method": "PUT", "path": "/api/v1/dataset/update", "status_code": 424, "duration_ms": 818.2, "outcome": "server_error"}
```

stdout carries the same entries as one readable line each, without
`app_version` and `pod`, and without `correlation_id` and `user_id` when they
are empty.

## Files

```
logs/
  2026-11/
    dmm-api_week_2026-10-26.log.gz                                # weekly archive, all pods
    dmm-api_2026-11-02_data-model-management-7c9f-x2k4q.log            # daily, one pod
    dmm-api_2026-11-03T00-00-07Z_data-model-management-7c9f-x2k4q.log  # chunk, one pod
```

| File | Name | Written |
| - | - | - |
| Chunk | `dmm-api_<UTC time of its first entry>_<pod>.log` | by each pod, every `LOG_FLUSH_SECONDS` |
| Daily | `dmm-api_<day>_<pod>.log` | by each pod, at its first flush after midnight UTC |
| Weekly archive | `dmm-api_week_<Monday>.log.gz` | by the CronJob, Mondays 01:00 UTC |

- Every file sits in the folder of its day's month; a weekly archive in the
  folder of its Monday's month. Weeks run Monday to Sunday, UTC.
- Chunks and daily files are plain text; only the weekly archive is gzipped.
  It holds every pod's entries for the week, in timestamp order.
- When two chunks would get the same name, the second gets `-2` after the
  time, the third `-3`, and so on.
- A pod writes, merges and deletes only its own files, so the two pods that
  overlap during a rolling update never touch each other's.

The S3 mount cannot append to a file, so every file is written once, in full,
and never changed. The only other operation is deleting files once they have
been merged or archived. Nothing deletes an archive: expiring old ones, if
wanted, is an S3 lifecycle rule on `logs/`.

### What can be missing from the files

- Entries are buffered in memory between flushes. A pod killed without
  warning (out of memory, node failure) loses up to `LOG_FLUSH_SECONDS` of
  entries from the files. stdout still had them.
- On a normal stop, the API writes its buffer during shutdown. uvicorn's last
  two messages ("Application shutdown complete", "Finished server process")
  come after that and reach stdout only.
- If the mount cannot be written, the entries stay in the buffer and are
  retried at the next flush. The buffer holds at most 50,000 entries; past
  that the oldest are dropped, and a warning on stdout gives the number.
- If `LOGS_DIR` cannot be created or written when the API starts, a warning
  goes to stdout and the API logs to stdout only.

Problems with the log files themselves (a failed write, merge or delete) are
reported on stdout only.

## Settings

| Variable | Default | Meaning |
| - | - | - |
| `LOGS_DIR` | `/s3/data-model-management/logs` | Root folder of the log files |
| `LOG_LEVEL` | `INFO` | Lowest level written, to stdout and files: `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` |
| `LOG_FLUSH_SECONDS` | `180` | How often a pod writes a chunk. A chunk is also written as soon as 10,000 entries are waiting |
| `HOSTNAME` | set by Kubernetes to the pod name | The pod name in entries and file names; characters other than `A-Z a-z 0-9 . -` become `-` |

For a local run, point `LOGS_DIR` at a folder in the working copy, such as
`./logs`, which git ignores.

## The weekly archive job

`python -m dmm_api.tools.log_archive` archives every week that ended before the
current one and still has plain files:

1. It merges all pods' daily files and leftover chunks for the week into
   `dmm-api_week_<Monday>.log.gz`. A chunk whose pod and day also have a daily
   file is left out, since the daily file holds it.
2. It reads the archive back and checks the line count.
3. It deletes the week's plain files.

If a week's archive already exists, the week is left untouched. Any failure
leaves the plain files in place, and the job exits with status 1, so
Kubernetes shows the run as failed. A missed run is caught up by the next one.
The job logs to stdout only.

The mount must allow deletes (for Mountpoint for S3, `--allow-delete`).
Without that, the archive is written, the deletes fail, and the plain files
stay next to it.

The CronJob runs the same image as the API, at the same tag, so bump both
together:

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
          imagePullSecrets:
            - name: ghcr-pull-secret
          volumes:
            - name: s3-volume
              persistentVolumeClaim:
                claimName: dg-s3-dev-pvc-univr
          containers:
            - name: log-archive
              image: ghcr.io/datagems-eosc/datagems-eosc/data-model-management:<same tag as the API>
              command: ["uv", "run", "python", "-m", "dmm_api.tools.log_archive"]
              env:
                - name: LOGS_DIR
                  value: "/s3/data-model-management/logs"
              volumeMounts:
                - name: s3-volume
                  mountPath: /s3
```

`LOGS_DIR` must match the API's. `backoffLimit: 0` keeps a failed run from
being retried at once; the next week's run picks up what it left.
