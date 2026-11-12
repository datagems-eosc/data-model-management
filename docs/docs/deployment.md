# Deployment

The DataGEMS team runs the service on the platform's Kubernetes cluster, from the container image the repository publishes (see [Automations](automations.md)).

## The image

- It serves the API with uvicorn on port 5000.
- It runs as user and group 10001.
- It writes only to `/logs`, the log disk; `/s3`, the S3 storage; and `/tmp`.

## Configuration

The service is configured with environment variables only; see [Configuration](configuration.md).

## Dependencies

The service needs the components described under [Architecture](architecture.md): MoMa, Grafeo, S3 storage and the AAI service at all times, and PostgreSQL and the other DataGEMS services for the endpoints that use them.
