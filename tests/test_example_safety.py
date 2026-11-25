"""Safety checks for public examples; existing data must never be opened."""
from pathlib import Path
import importlib
import sqlite3
import subprocess
import sys

import pytest


@pytest.fixture
def demo(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / 'examples'))
    return importlib.import_module('threaded_dispatcher')


@pytest.mark.parametrize('existing', ['empty', 'arbitrary', 'database', 'directory', 'wal', 'shm', 'journal'])
def test_threaded_demo_refuses_existing_paths_before_store_open(tmp_path, monkeypatch, demo, existing):
    path = tmp_path / 'ops.db'
    target = path
    if existing in {'wal', 'shm', 'journal'}:
        target = Path(str(path) + '-' + existing)
        target.write_bytes(b'SYNTHETIC existing sidecar')
    elif existing == 'directory':
        target.mkdir()
    elif existing == 'database':
        with sqlite3.connect(path) as connection:
            connection.execute('CREATE TABLE unrelated(value TEXT)')
            connection.execute("INSERT INTO unrelated VALUES('SYNTHETIC existing row')")
    else:
        target.write_bytes(b'' if existing == 'empty' else b'SYNTHETIC existing data')
    before = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.iterdir() if p.is_file()}

    def forbidden_open(*args, **kwargs):
        pytest.fail('example constructed the store before refusing an existing path')

    monkeypatch.setattr(demo, 'ChatOperationStore', forbidden_open)
    with pytest.raises(FileExistsError, match='[Rr]efusing existing'):
        demo.main(path, workers=2, lanes=2)
    after = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.iterdir() if p.is_file()}
    assert after == before
    assert target.exists()


def test_demo_cli_preserves_a_real_existing_queue_with_inflight_work(tmp_path):
    from durable_chat_queue import ChatOperationStore

    path = tmp_path / 'existing.db'
    store = ChatOperationStore(path)
    for op in ('running', 'queued'):
        store.submit(client_operation_id=op, request_id=op,
                     conversation_id='synthetic-conversation', device_id='synthetic-device',
                     payload={'text': 'synthetic existing work'})
    assert store.claim('running') is not None
    before_rows = store.list(conversation_id='synthetic-conversation')
    before_files = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.iterdir() if p.is_file()}
    example = Path(__file__).resolve().parents[1] / 'examples/threaded_dispatcher.py'
    result = subprocess.run([sys.executable, '-B', str(example), '--db', str(path)],
                            cwd=tmp_path, capture_output=True, text=True, timeout=15)
    assert result.returncode == 2
    assert 'Refusing existing' in result.stderr
    assert 'previous process' not in result.stdout
    after_files = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.iterdir() if p.is_file()}
    assert after_files == before_files
    assert store.list(conversation_id='synthetic-conversation') == before_rows
    assert store.counts() == {'QUEUED': 1, 'RUNNING': 1}
