# Changelog

Notable changes to the service, for its API clients and its deployments. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
versions follow [semantic versioning](https://semver.org/). How to add an
entry: "Changelog" in `CONTRIBUTING.md`.

## [Unreleased]

## [0.4.6] - 2026-11-27

### Added

- `GET /dataset/search` can filter datasets by `uploadedBy`. (#98)

## [0.4.5] - 2026-11-24

### Added

- `python -m dmm_api.tools.grafeo_upgrade_check`: checks that a candidate
  Grafeo version can replace the one a deployment runs, on two test instances
  restored from one backup (#87).

### Changed

- A refused mixed query in `POST /polyglot/query` (a CSV source outside
  `s3://dataset/`, or more than one query) answers 422 with the reason,
  instead of 500 (#92).
- The image is built on `python:3.12.16-slim-trixie` instead of
  `python:3.12-slim`, which had moved to Debian 13 without notice (#87).

### Fixed

- `user_id` in `GET /dataset/search` filters by the datasets the user owns.
  It answered 400 for any id longer than one character, and matched no
  dataset the user owned (#91).
- `POST /polyglot/query` reads a CSV file whose `contentUrl` is its mounted
  path, `/s3/dataset/<id>/<file>`, as profiles store it. Refused since 0.3.0
  (#92).

## [0.4.4] - 2026-11-17

Versions 0.4.2 and 0.4.3 were tagged but never published: `v0.4.2` is on a
commit whose version still said 0.4.1, so the image build refused it, and
`0.4.3` has no `v`, so no build ran. Their change is the one below, first
published in 0.4.4.

### Changed

- The service requires `GRAFEO_AUTH_TOKEN`, and sends it to Grafeo as a
  bearer token on every request. It does not start without it.

## [0.4.1] - 2026-11-13

### Added

- `PUT /dataset/load` also moves a dataset from the onboarding's staging
  folder, `s3://dataset_staging/`, set by the new `DATASET_STAGING_DIR`
  (default `/s3/dataset_staging`) (#77).

### Security

- Load errors no longer name server paths (#77).

## [0.4.0] - 2026-11-12

### Changed

- `POST /convert` answers 410 Gone (#72).
- A query's SQL errors reach the caller; the PostgreSQL password never does
  (#72).
- The image leaves out the `dev` dependency group (#72).

### Fixed

- Direct graph queries (`/grafeo/*`) use `GRAFEO_URL` (#72).

### Security

- `PUT /dataset/load` refuses a dataset ID that names a path, and moves only
  a folder from the scratchpad (#74).
- Error bodies no longer carry the text of the called services' errors, 400s
  included (#72).

## [0.3.1] - 2026-11-11

### Added

- Log entries follow the DataGEMS logging service's json-cf-2 format (#70).
- The correlation ID is passed on to the services this one calls, in
  `x-tracking-correlation` (#70).

### Removed

- The unused token exchange, and its settings `CDD_EXCHANGE_SCOPE` and
  `OIDC_CLIENT_SECRET` (#71).

### Security

- `GET /polyglot/query/result/{id}` checks the token, and error bodies no
  longer carry exception text (#71).

## [0.3.0] - 2026-11-07

### Changed

- `POST /data-workflow` requires a login, and answers 400 for a `dataset_id`
  that is not a UUID or a `file_name` with a folder (#67).
- A mixed query in `POST /polyglot/query` is refused when a CSV source is
  outside `s3://dataset/` or its SQL is more than one query (#67).
- `POST /convert` requires a login, and converts uploads in memory (#67).

### Security

- Mixed queries read files only under `/s3/dataset/`, and argument names from
  the request no longer reach DuckDB's SQL (#67).

## Before 0.3.0

Versions up to 0.2.1 have no entries here; their tags and the commit history
record them.

[Unreleased]: https://github.com/datagems-eosc/data-model-management/compare/v0.4.6...HEAD
[0.4.6]: https://github.com/datagems-eosc/data-model-management/compare/v0.4.5...v0.4.6
[0.4.5]: https://github.com/datagems-eosc/data-model-management/compare/v0.4.4...v0.4.5
[0.4.4]: https://github.com/datagems-eosc/data-model-management/compare/v0.4.1...v0.4.4
[0.4.1]: https://github.com/datagems-eosc/data-model-management/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/datagems-eosc/data-model-management/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/datagems-eosc/data-model-management/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/datagems-eosc/data-model-management/compare/v0.2.1...v0.3.0
