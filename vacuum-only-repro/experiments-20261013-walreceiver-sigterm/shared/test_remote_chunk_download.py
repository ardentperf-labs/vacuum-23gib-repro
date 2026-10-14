#!/usr/bin/env python3
"""Offline coverage for the WebSocket-safe harvest file transfer."""

import hashlib
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from remote_chunk_download import RemoteTransferError, download_verified_file


class RemoteChunkDownloadTests(unittest.TestCase):
    def test_retries_short_range_and_atomically_verifies_complete_file(self):
        content = bytes((index * 31) % 256 for index in range(53))
        observed_ranges = []
        attempts = {}

        def fingerprint():
            return len(content), hashlib.sha256(content).hexdigest()

        def read_chunk(offset, length):
            observed_ranges.append((offset, length))
            attempts[offset] = attempts.get(offset, 0) + 1
            if offset == 16 and attempts[offset] == 1:
                return content[offset:offset + 5]  # Simulate a severed exec stream.
            return content[offset:offset + length]

        with tempfile.TemporaryDirectory() as temp:
            destination = Path(temp) / 'controller-logs.tar.gz'
            destination.write_bytes(b'previous-complete-file')
            receipt = download_verified_file(destination, fingerprint, read_chunk,
                                              chunk_bytes=16, attempts_per_chunk=3)
            self.assertEqual(destination.read_bytes(), content)
            self.assertEqual(receipt['bytes'], len(content))
            self.assertEqual(receipt['sha256'], hashlib.sha256(content).hexdigest())
            self.assertEqual(receipt['chunks'], 4)
            self.assertEqual(receipt['retries'], 1)
            self.assertEqual(observed_ranges.count((16, 16)), 2)

    def test_remote_change_refuses_to_replace_existing_destination(self):
        original = b'original remote bytes'
        changed = b'changed remote bytes!'
        calls = 0

        def fingerprint():
            nonlocal calls
            calls += 1
            data = original if calls == 1 else changed
            return len(data), hashlib.sha256(data).hexdigest()

        with tempfile.TemporaryDirectory() as temp:
            destination = Path(temp) / 'logs.tar.gz'
            destination.write_bytes(b'keep this file')
            with self.assertRaises(RemoteTransferError):
                download_verified_file(destination, fingerprint, lambda offset, length: original[offset:offset + length],
                                       chunk_bytes=8)
            self.assertEqual(destination.read_bytes(), b'keep this file')
            self.assertEqual(sorted(path.name for path in Path(temp).iterdir()), ['logs.tar.gz'])

    def test_exhausted_partial_retries_preserve_old_file(self):
        content = b'x' * 40
        fingerprint = lambda: (len(content), hashlib.sha256(content).hexdigest())
        with tempfile.TemporaryDirectory() as temp:
            destination = Path(temp) / 'logs.tar.gz'
            destination.write_bytes(b'old archive')
            with self.assertRaises(RemoteTransferError):
                download_verified_file(destination, fingerprint, lambda offset, length: b'x',
                                       chunk_bytes=16, attempts_per_chunk=2)
            self.assertEqual(destination.read_bytes(), b'old archive')
            self.assertEqual(sorted(path.name for path in Path(temp).iterdir()), ['logs.tar.gz'])

    def test_configured_retry_backoff_grows_and_caps(self):
        content = b'complete range'
        attempts = 0
        delays = []

        def read_chunk(offset, length):
            nonlocal attempts
            attempts += 1
            if attempts <= 3:
                return b''
            return content[offset:offset + length]

        with tempfile.TemporaryDirectory() as temp, mock.patch('remote_chunk_download.time.sleep',
                                                                  side_effect=delays.append):
            result = download_verified_file(Path(temp) / 'bundle.gz',
                lambda: (len(content), hashlib.sha256(content).hexdigest()), read_chunk,
                chunk_bytes=len(content), attempts_per_chunk=4,
                retry_backoff_seconds=0.5, max_retry_backoff_seconds=2.0)
        self.assertEqual(result['retries'], 3)
        self.assertEqual(delays, [0.5, 1.0, 2.0])


if __name__ == '__main__':
    unittest.main()
