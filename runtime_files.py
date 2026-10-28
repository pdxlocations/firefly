"""Private, atomic storage for runtime identity and session secrets."""
from contextlib import contextmanager
import json
import sqlite3
import os
import secrets
import tempfile
from pathlib import Path


def atomic_private_write(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix=f'.{path.name}.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def read_identity_payload(path, *, allow_generate=False):
    from vnode import NodeConfig, SecurityConfig
    from vnode.crypto import b64_encode, generate_keypair
    path = Path(path)
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(payload, dict) or not isinstance(payload.get('security', {}), dict):
            raise ValueError('Invalid identity configuration')
    except FileNotFoundError:
        if not allow_generate:
            raise ValueError(f'Missing identity file: {path}; restore it from backup')
        payload = {}
    security = payload.get('security', {})
    key = security.get('private_key', '')
    if not key:
        if not allow_generate:
            raise ValueError(f'Missing private key in {path}; restore it from backup')
        _, private = generate_keypair()
        key = b64_encode(private)
    # Derive with vnode's public config API; invalid keys must never be replaced.
    config = NodeConfig(security=SecurityConfig(private_key=key))
    node_id = config.node_id
    payload['security'] = {'private_key': key}
    payload.pop('node_id', None)
    return payload, node_id


def session_secret(root):
    configured = os.getenv('FIREFLY_SECRET_KEY', '').strip()
    if configured and configured not in {'change-me', 'dev-secret-key-change-in-production'}:
        return configured
    path = Path(root) / 'session.secret'
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with exclusive_file_lock(path.with_suffix('.lock')):
        if path.exists():
            secret = path.read_text(encoding='utf-8').strip()
            if not secret:
                raise ValueError(f'Empty session secret: {path}')
            return secret
        secret = secrets.token_hex(32)
        atomic_private_write(path, secret)
        return secret


@contextmanager
def database_connection(path):
    """SQLite's transaction context does not close its connection."""
    connection = sqlite3.connect(path)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


@contextmanager
def exclusive_file_lock(path):
    with Path(path).open('a+b') as lock:
        if os.name == 'nt':
            import msvcrt
            lock.seek(0)
            lock.write(b'\0')
            lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            if os.name == 'nt':
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock, fcntl.LOCK_UN)
