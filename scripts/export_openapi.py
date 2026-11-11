"""Write the API's OpenAPI document to a file, for the docs site.

The docs site renders this document on its OpenAPI page. It is generated from
the app at build time rather than kept in the repository, so that it always
matches the routes. Importing the app needs the service URLs, which are read
from `dev.env` as the tests do; variables already set in the environment win.

Usage: python scripts/export_openapi.py <output file>
"""

import json
import os
import sys
import tempfile
from pathlib import Path

for line in (Path(__file__).parent.parent / "dev.env").read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#"):
        name, _, value = line.partition("=")
        os.environ.setdefault(name, value)

# Importing the app sets up the log files; they go to a throwaway folder.
os.environ.setdefault("LOGS_DIR", tempfile.mkdtemp(prefix="dmm-api-openapi-logs-"))

from dmm_api.main import app  # noqa: E402

# A fresh checkout has no `docs/docs/content/`: the document is its only file,
# and it is not committed.
out = Path(sys.argv[1])
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(app.openapi(), indent=2) + "\n")
