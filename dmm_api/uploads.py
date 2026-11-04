"""Reading uploaded files with a size limit.

The gateway in front of the cluster caps request bodies at 64 MB, but a direct
or local call has no cap, and every upload endpoint used to read the whole file
into memory.
"""

import os

from fastapi import HTTPException, UploadFile, status

MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "64"))
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024
CHUNK_BYTES = 1024 * 1024


async def read_upload(file: UploadFile) -> bytes:
    """Return the file's content, or raise a 413 once it passes the limit."""
    chunks = []
    size = 0
    while chunk := await file.read(CHUNK_BYTES):
        size += len(chunk)
        if size > MAX_UPLOAD_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File too large: the limit is {MAX_UPLOAD_MB} MB.",
            )
        chunks.append(chunk)
    return b"".join(chunks)
