#!/usr/bin/env python3
"""Verified, resumable-by-chunk downloads from immutable remote files."""

from __future__ import annotations

import hashlib
import os
import tempfile
import time
from pathlib import Path
from typing import Callable


class RemoteTransferError(RuntimeError):
    pass


def download_verified_file(
    destination: Path,
    fingerprint: Callable[[], tuple[int, str]],
    read_chunk: Callable[[int, int], bytes],
    *,
    chunk_bytes: int = 2 * 1024 * 1024,
    attempts_per_chunk: int = 5,
    retry_backoff_seconds: float = 0.0,
    max_retry_backoff_seconds: float = 2.0,
) -> dict[str, int | str]:
    """Download a stable file without exposing partial data at ``destination``.

    ``fingerprint`` must return the remote file's byte length and SHA-256.
    ``read_chunk`` reads the exact byte range requested, or raises/returns a
    short range when the transport is interrupted. Each range is retried
    independently. The complete remote fingerprint is checked again after the
    local file has been hashed and before its atomic rename.
    """
    if chunk_bytes <= 0 or attempts_per_chunk <= 0:
        raise ValueError("chunk_bytes and attempts_per_chunk must be positive")
    if retry_backoff_seconds < 0 or max_retry_backoff_seconds < 0:
        raise ValueError("retry backoff values must be nonnegative")
    before_size, before_hash = fingerprint()
    if before_size < 0 or len(before_hash) != 64 or any(c not in "0123456789abcdef" for c in before_hash):
        raise RemoteTransferError("Remote file fingerprint is invalid")

    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".partial",
                                          dir=destination.parent)
    temporary = Path(temporary_name)
    retries = 0
    chunks = 0
    digest = hashlib.sha256()
    try:
        with os.fdopen(fd, "wb") as output:
            for offset in range(0, before_size, chunk_bytes):
                expected = min(chunk_bytes, before_size - offset)
                received = None
                for attempt in range(attempts_per_chunk):
                    try:
                        candidate = read_chunk(offset, expected)
                    except Exception:
                        candidate = b""
                    if len(candidate) == expected:
                        received = candidate
                        break
                    if attempt + 1 < attempts_per_chunk:
                        retries += 1
                        delay = min(retry_backoff_seconds * (2 ** attempt), max_retry_backoff_seconds)
                        if delay:
                            time.sleep(delay)
                if received is None:
                    raise RemoteTransferError(
                        f"Could not read remote file range at byte {offset} after {attempts_per_chunk} attempts"
                    )
                output.write(received)
                digest.update(received)
                chunks += 1
            output.flush()
            os.fsync(output.fileno())

        after_size, after_hash = fingerprint()
        local_hash = digest.hexdigest()
        if (before_size, before_hash) != (after_size, after_hash):
            raise RemoteTransferError("Remote file changed during chunked download")
        if local_hash != before_hash or temporary.stat().st_size != before_size:
            raise RemoteTransferError("Downloaded bytes do not match the remote file fingerprint")
        os.replace(temporary, destination)
        return {
            "bytes": before_size,
            "sha256": before_hash,
            "chunks": chunks,
            "chunkBytes": chunk_bytes,
            "retries": retries,
        }
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
