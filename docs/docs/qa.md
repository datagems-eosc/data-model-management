# Quality Assurance

The repository's Makefile runs the checks:

| Command | What it checks |
| - | - |
| `make test` | The pytest suite under `tests/`, with doctests |
| `make check` | That `uv.lock` matches `pyproject.toml`; then pre-commit on every file: ruff (lint and format), and JSON, YAML and TOML checks |
| `make docs-test` | That this site builds without warnings |

`tests/` also holds example Analytical Pattern requests and responses for each feature, and sample data under `tests/data/`.
