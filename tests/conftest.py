"""Load `dev.env` before the app is imported, as `docker compose` does.

The service URLs are required at startup, so without them importing
`dmm_api.main` exits. Variables already set in the environment win.
"""

import os
from pathlib import Path

for line in (Path(__file__).parent.parent / "dev.env").read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#"):
        name, _, value = line.partition("=")
        os.environ.setdefault(name, value)
