# Maintenance

## Health

`GET /api/v1` answers without a token and reports the running version, so it shows whether the service is up.

## Versions

The service follows semantic versioning, `MAJOR.MINOR.PATCH`. Each release is a tag `vX.Y.Z` and the image of the same tag. The running version is in the answer of `GET /api/v1`, in the [OpenAPI Reference](openapi.md) and in every log entry.

## Backups

What the service writes is kept in MoMa, in Grafeo and on S3 storage, as [Data Stores](datastore.md) describes. Back up those.

## Troubleshooting

Ask the caller for the correlation ID of the failed request, from the response's `x-tracking-correlation` header or the error body's `correlationId`, and find the request in the logs as [Logging](logging.md) describes.
