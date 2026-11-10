# Security

## Authentication

Every endpoint requires a bearer token issued by the [DataGEMS AAI service](https://github.com/datagems-eosc/dg-aai), except `GET /api/v1`, `GET /api/v1/dataset` and `GET /api/v1/grafeo/test`, which return no data:

```
Authorization: Bearer <token>
```

The service accepts a token when:

- its signature verifies with the public keys the AAI service publishes, which the service caches for a few minutes;
- its issuer (`iss`) is the configured AAI realm;
- it has not expired (`exp`);
- its audience (`aud`) contains `data-model-management-api`. `POST /api/v1/authtest` does not check the audience.

It does not check the authorized party (`azp`).

A missing or invalid token gets `401`, a token without the audience `403`. When the public keys cannot be fetched, the answer is `503`.

## Authorization

The service does not check roles or dataset grants: a caller with an accepted token can use every endpoint.

## Calls to other services

When the service calls MoMa or another DataGEMS service while serving a request, it passes on the caller's token unchanged, with the request's correlation ID.

## Secrets

The only secret the service reads is the PostgreSQL reader account, from environment variables that the deployment fills from a secret store. [Configuration](configuration.md) lists them.
