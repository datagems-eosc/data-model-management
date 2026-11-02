# dmm-api

Data Model & Management Platform API (WP5)

## How to commit your code

### 1. Run the pre-commit hooks

Firstly, to resolve formatting issues of your code, run:

```bash
uv run pre-commit run -a
```

### 2. Commit the changes

Commit the changes made to your repository.

```bash
git add .
git commit -m 'Commit message'
git push origin main
```

## How to release a new version

The project follows [semantic versioning](https://semver.org) (`MAJOR.MINOR.PATCH`).
The `version` in `pyproject.toml` is the only place the version is written:
the API reads it at runtime and shows it in Swagger, ReDoc and `GET /api/v1`.
Do not hard-code a version anywhere else.

### 1. Bump the version

Pick the part to bump: `patch` for fixes, `minor` for new features or, while
the version is below 1.0.0, for breaking changes, `major` for breaking changes
after 1.0.0. Then run:

```bash
uv version --bump patch
```

This updates both `pyproject.toml` and `uv.lock`. Check the result with
`make check`.

### 2. Commit the bump on its own

The release commit contains only the version change:

```bash
git add pyproject.toml uv.lock
git commit -m 'chore(release): v0.1.26'
```

### 3. Tag the commit and push

Use an annotated tag named after the new version, with a leading `v`:

```bash
git tag -a v0.1.26 -m 'v0.1.26'
git push origin main
git push origin v0.1.26
```

Pushing the tag is the release: the `Build and Push Docker Image` workflow
builds the image and publishes it to the GitHub Container Registry, tagged
with the version. The workflow stops without publishing if the tag does not
match the version in `pyproject.toml`.

Never move or delete a tag once it is pushed. To fix a release, bump the
version again.

---
