"""Size limits for request bodies and uploaded files.

The gateway streams request bodies to the service without buffering or
limiting them, so the service limits them itself: a body declared too large is
refused before any of it is read. Upload endpoints read a whole file into
memory, which the same limit bounds.
"""

import os

from fastapi import HTTPException, UploadFile, status
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from dmm_api.logging_config import get_correlation_id

MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "64"))
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024
CHUNK_BYTES = 1024 * 1024


async def read_upload(file: UploadFile) -> bytes:
    """Return the file's content, or raise a 413 once it passes the limit."""
    chunks = []
    size = 0
    while chunk := await file.read(CHUNK_BYTES):
        size += len(chunk)
        # In the app this runs after BodyLimitMiddleware, which has already
        # refused any request body over the same MAX_UPLOAD_BYTES. A file is
        # always smaller than the multipart body that carries it, so over HTTP
        # this check never fires: an oversized upload gets the middleware's
        # "Request too large" 413 instead of this "File too large" one, and
        # the largest file accepted is the limit minus the multipart wrapping
        # and any other form fields, a few hundred bytes for a plain upload.
        # It fires only where read_upload is called without the middleware in
        # front, as the unit tests do, or if the two checks are ever given
        # different limits.
        if size > MAX_UPLOAD_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File too large: the limit is {MAX_UPLOAD_MB} MB.",
            )
        chunks.append(chunk)
    return b"".join(chunks)


class BodyLimitMiddleware:
    """Answer a request whose body passes `MAX_UPLOAD_BYTES` with a 413.

    A body declared too large by its Content-Length is refused before any of
    it is read. A body sent without one is counted as it arrives, and reading
    it raises the 413 once it passes the limit.
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        error = f"Request too large: the limit is {MAX_UPLOAD_MB} MB."
        length = dict(scope["headers"]).get(b"content-length")
        if length is not None and int(length) > MAX_UPLOAD_BYTES:
            response = JSONResponse(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                content={
                    "code": status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    "error": error,
                    "correlationId": get_correlation_id(),
                },
            )
            await response(scope, receive, send)
            return

        received = 0

        async def counting_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=error,
                    )
            return message

        await self.app(scope, counting_receive, send)
