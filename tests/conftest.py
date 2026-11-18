"""Load `dev.env` before the app is imported, as `docker compose` does.

The service URLs are required at startup, so without them importing
`dmm_api.main` exits. Variables already set in the environment win.
"""

import os
import tempfile
from pathlib import Path

for line in (Path(__file__).parent.parent / "dev.env").read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#"):
        name, _, value = line.partition("=")
        os.environ.setdefault(name, value)

# The service refuses to start without a Grafeo token. No test reaches a real
# Grafeo, so any value will do.
os.environ.setdefault("GRAFEO_AUTH_TOKEN", "test-token")

# Log files written while the tests run go to a throwaway folder.
os.environ.setdefault("LOGS_DIR", tempfile.mkdtemp(prefix="dmm-api-test-logs-"))
