"""tools/csv_utils
Minimal, clean CSV utilities for the scanner.

This module provides safe, file-locked helpers used by the scanner to
atomically append or overwrite the canonical `data/liquidations_master.csv`.
"""

import csv
import os
import time
import tempfile
from shutil import copy2
import portalocker


def safe_append_row(csv_path: str, row: dict, fieldnames: list):
    """Append a single row to CSV with file locking. Creates header if file empty."""
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, 'a+', encoding='utf-8', newline='') as f:
        portalocker.lock(f, portalocker.LockFlags.EXCLUSIVE)
        try:
            f.seek(0)
            first = f.read(1)
            if first == '':
                # File is empty, write header
                f.seek(0)
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
            else:
                # File has content, seek to end
                f.seek(0, os.SEEK_END)
                writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        finally:
            try:
                portalocker.unlock(f)
            except Exception:
                pass


def safe_overwrite_rows(csv_path: str, rows: list, fieldnames: list):
    """Safely overwrite CSV with rows using atomic tmp-replace."""
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    dirn = os.path.dirname(csv_path)
    fd, tmp_path = tempfile.mkstemp(prefix='csv_tmp_', suffix='.csv', dir=dirn)
    os.close(fd)
    try:
        with open(tmp_path, 'w', encoding='utf-8', newline='') as tf:
            writer = csv.DictWriter(tf, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        lock_path = csv_path + '.lock'
        with open(lock_path, 'w', encoding='utf-8') as lf:
            portalocker.lock(lf, portalocker.LockFlags.EXCLUSIVE)
            try:
                os.replace(tmp_path, csv_path)
            finally:
                try:
                    portalocker.unlock(lf)
                except Exception:
                    pass
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def append_row_if_tx_missing(csv_path: str, row: dict, fieldnames: list, tx_field: str = 'tx') -> bool:
    """Append row only if tx not already present. Returns True if appended."""
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    open(csv_path, 'a', encoding='utf-8').close()
    with open(csv_path, 'r+', encoding='utf-8', newline='') as f:
        portalocker.lock(f, portalocker.LockFlags.EXCLUSIVE)
        try:
            f.seek(0)
            reader = csv.DictReader(f)
            existing = set((r.get(tx_field) or '').lower() for r in reader if r.get(tx_field))

            txval = (row.get(tx_field) or '').lower()
            if not txval:
                f.seek(0, os.SEEK_END)
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow(row)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
                return True

            if txval in existing:
                return False

            f.seek(0, os.SEEK_END)
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(row)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
            return True
        finally:
            try:
                portalocker.unlock(f)
            except Exception:
                pass


def backup_file(path: str, suffix: str = None):
    """Create timestamped backup of file."""
    try:
        if not os.path.exists(path):
            return None
        ts = int(time.time())
        dirname = os.path.dirname(path)
        base = os.path.basename(path)
        suffix = suffix or 'bak'
        backup = os.path.join(dirname, f"{base}.{suffix}.{ts}")
        copy2(path, backup)
        return backup
    except Exception:
        return None
