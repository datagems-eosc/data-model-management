# Automations

## Container image

The [image workflow](https://github.com/datagems-eosc/data-model-management/blob/main/.github/workflows/docker-image.yml) builds the image from the [Dockerfile](https://github.com/datagems-eosc/data-model-management/blob/main/Dockerfile) and publishes it to the GitHub Container Registry as `ghcr.io/datagems-eosc/datagems-eosc/data-model-management`, tagged with the version. It runs when a tag `vX.Y.Z` is pushed, and stops if the tag does not match the version in `pyproject.toml`.

## Documentation

The [documentation workflow](https://github.com/datagems-eosc/data-model-management/blob/main/.github/workflows/deploy-docs-on-demand.yml) builds this site with [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/), failing on any warning, and publishes it to GitHub Pages with [mike](https://github.com/jimporter/mike) under the version given when it is started, and as `latest`. It is started by hand. The [OpenAPI Reference](openapi.md) is generated from the code at each build.

## Checks before a commit

[pre-commit](https://pre-commit.com/) runs ruff (lint and format) and checks JSON, YAML and TOML files, as [Quality Assurance](qa.md) describes.
