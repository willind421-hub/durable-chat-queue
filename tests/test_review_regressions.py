"""Regression tests for the 2026-11-25 review; all data is synthetic."""

import pytest
from typing import Any
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

from durable_chat_queue import ChatOperationStore, canonical_payload


def submission(**changes) -> dict[str, Any]:
    values: dict[str, Any] = dict(client_operation_id="op-original", request_id="request-original",
                  conversation_id="conversation-owner", device_id="device-owner",
                  payload={"text": "private synthetic draft", "workspace_id": "workspace-owner"})
    values.update(changes)
    return values


@pytest.mark.parametrize("scope", ["conversation", "device", "workspace", "device-none", "workspace-none"])
@pytest.mark.parametrize("identity", ["operation", "request"])
@pytest.mark.parametrize("terminal", [False, True])
def test_submit_foreign_scope_conflicts_without_disclosing_any_existing_fields(tmp_path, scope, identity, terminal):
    store = ChatOperationStore(tmp_path / "ops.db")
    original = submission()
    store.submit(**original)
    if terminal:
        store.claim_next()
        store.finish(original["client_operation_id"], state="COMPLETED", message_id="private-receipt")
    before = store.get(original["client_operation_id"])
    incoming = submission()
    if scope == "conversation":
        incoming["conversation_id"] = "foreign-conversation"
    elif scope.startswith("device"):
        incoming["device_id"] = None if scope.endswith("none") else "foreign-device"
    else:
        incoming["payload"] = {**incoming["payload"], "workspace_id": None if scope.endswith("none") else "foreign-workspace"}
    if identity == "request":
        incoming["client_operation_id"] = "op-other"
    assert store.submit(**incoming) == ("CONFLICT", {})
    assert store.get(original["client_operation_id"]) == before
    assert sum(store.counts().values()) == 1


@pytest.mark.parametrize("identity", ["operation", "request"])
def test_same_scope_content_conflict_is_generic(tmp_path, identity):
    store = ChatOperationStore(tmp_path / "ops.db")
    store.submit(**submission())
    incoming = submission(payload={"text": "changed", "workspace_id": "workspace-owner"})
    if identity == "request":
        incoming["client_operation_id"] = "op-other"
    assert store.submit(**incoming) == ("CONFLICT", {})


@pytest.mark.parametrize("scope", ["conversation", "device", "workspace"])
@pytest.mark.parametrize("identity", ["operation", "request"])
@pytest.mark.parametrize("sealed", [False, True])
def test_confirm_missing_cannot_disclose_foreign_existing_rows(tmp_path, scope, identity, sealed):
    store = ChatOperationStore(tmp_path / "ops.db")
    original = submission()
    digest = canonical_payload(original.pop("payload"))[1]
    original.update(workspace_id="workspace-owner", payload_sha256=digest)
    if sealed:
        store.confirm_missing(**original)
    else:
        store.submit(**submission())
    before = store.get("op-original")
    incoming = dict(original)
    incoming[scope + "_id"] = "foreign-" + scope
    if identity == "request":
        incoming["client_operation_id"] = "op-other"
    assert store.confirm_missing(**incoming) == ("CONFLICT", {})
    assert store.get("op-original") == before
    assert sum(store.counts().values()) == 1


def test_confirm_missing_request_collision_even_in_same_scope_is_generic(tmp_path):
    store = ChatOperationStore(tmp_path / "ops.db")
    store.submit(**submission())
    assert store.confirm_missing(client_operation_id="op-other", request_id="request-original",
                                 conversation_id="conversation-owner", device_id="device-owner",
                                 workspace_id="workspace-owner", payload_sha256="0" * 64) == ("CONFLICT", {})


@pytest.mark.parametrize("method", ["submit", "confirm_missing"])
def test_existing_operation_cannot_bypass_another_request_identity(tmp_path, method):
    store = ChatOperationStore(tmp_path / "ops.db")
    store.submit(**submission())
    store.submit(**submission(client_operation_id="op-second", request_id="request-second"))
    incoming = submission(request_id="request-second")
    if method == "confirm_missing":
        incoming["payload_sha256"] = canonical_payload(incoming.pop("payload"))[1]
        incoming["workspace_id"] = "workspace-owner"
    assert getattr(store, method)(**incoming) == ("CONFLICT", {})


@pytest.mark.parametrize("state", ["QUEUED", "COMPLETED", "CANCELLED", "FAILED", "UNKNOWN", "CONFIRMED_MISSING"])
def test_global_idempotency_keeps_original_receipt_for_matching_scope(tmp_path, state):
    store = ChatOperationStore(tmp_path / "ops.db")
    original = submission()
    missing = {key: value for key, value in original.items() if key != "payload"}
    missing.update(workspace_id="workspace-owner", payload_sha256=canonical_payload(original["payload"])[1])
    if state == "CONFIRMED_MISSING":
        store.confirm_missing(**missing)
    else:
        store.submit(**original)
        if state != "QUEUED":
            store.claim_next()
            store.finish("op-original", state=state)
    before = store.get("op-original")
    assert store.submit(**original) == ("REPLAY", before)
    # An unused transport request id still reconciles to the same operation.
    assert store.submit(**submission(request_id="unused-request")) == ("REPLAY", before)
    assert store.confirm_missing(**missing) == ("EXISTING", before)
    assert sum(store.counts().values()) == 1


def test_workspace_backfill_runs_once_including_unbound_and_invalid_payloads(tmp_path, monkeypatch):
    path = tmp_path / "ops.db"
    store = ChatOperationStore(path)
    for index in range(3):
        store.submit(**submission(client_operation_id=f"op-{index}", request_id=f"req-{index}"))
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version=0")
        for index, payload in enumerate(['{"workspace_id":"legacy-workspace"}', '{"text":"unbound"}', 'invalid-json']):
            connection.execute("UPDATE chat_operations SET workspace_id=NULL, payload_json=? WHERE client_operation_id=?", (payload, f"op-{index}"))
    loads = json.loads
    parsed = []

    def record_loads(value, *args, **kwargs):
        parsed.append(value)
        return loads(value, *args, **kwargs)

    monkeypatch.setattr(json, "loads", record_loads)
    assert store.counts() == {"QUEUED": 3}
    assert len(parsed) == 3  # legacy rows are considered once, even when unbound
    for _ in range(3):
        assert ChatOperationStore(path).counts() == {"QUEUED": 3}
    assert len(parsed) == 3
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert connection.execute("SELECT workspace_id FROM chat_operations ORDER BY sequence").fetchall() == [("legacy-workspace",), (None,), (None,)]


def test_concurrent_legacy_initialization_serializes_workspace_migration(tmp_path):
    # Legacy schema has no workspace column; all callers use separate connections.
    from test_store import LEGACY_SCHEMA
    path = tmp_path / "ops.db"
    with sqlite3.connect(path) as connection:
        connection.execute(LEGACY_SCHEMA)
    import threading
    barrier = threading.Barrier(6)

    def initialize(_):
        barrier.wait(timeout=10)
        return ChatOperationStore(path).counts()

    with ThreadPoolExecutor(max_workers=6) as pool:
        assert list(pool.map(initialize, range(6))) == [{}] * 6


@pytest.mark.parametrize("terminal", ["COMPLETED", "CANCELLED"])
@pytest.mark.parametrize("size", [200, 5000, 20000])
def test_cleared_payload_not_present_in_checkpointed_main_db(tmp_path, monkeypatch, terminal, size):
    # Explicitly emulate a SQLite build whose default secure_delete is OFF.
    raw_connect = sqlite3.connect

    def default_off(*args, **kwargs):
        connection = raw_connect(*args, **kwargs)
        connection.execute("PRAGMA secure_delete=OFF")
        return connection

    monkeypatch.setattr(sqlite3, "connect", default_off)
    path = tmp_path / "ops.db"
    marker = "SYNTHETIC-REMOVAL-MARKER-" * (size // 25 + 1)
    store = ChatOperationStore(path)
    store.submit(**submission(payload={"text": marker}))
    store.claim_next()
    store.finish("op-original", state=terminal)
    receipt = store.get("op-original")
    assert receipt is not None and receipt["payload"] is None
    with raw_connect(path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert b"SYNTHETIC-REMOVAL-MARKER-" not in path.read_bytes()


def test_secure_delete_is_enabled_on_every_store_connection(tmp_path, monkeypatch):
    raw_connect = sqlite3.connect

    def default_off(*args, **kwargs):
        connection = raw_connect(*args, **kwargs)
        connection.execute("PRAGMA secure_delete=OFF")
        return connection

    monkeypatch.setattr(sqlite3, "connect", default_off)
    store = ChatOperationStore(tmp_path / "ops.db")
    for _ in range(2):
        with store._session() as connection:
            assert connection.execute("PRAGMA secure_delete").fetchone()[0] == 1


@pytest.mark.parametrize("number", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_json_is_rejected_before_opening_database(tmp_path, number):
    path = tmp_path / "ops.db"
    payload = {"nested": [number]}
    with pytest.raises(ValueError, match="Out of range float"):
        canonical_payload(payload)
    with pytest.raises(ValueError, match="Out of range float"):
        ChatOperationStore(path).submit(**submission(payload=payload))
    assert not path.exists()


def test_legacy_migration_waits_for_writer_lock_and_preserves_receipts(tmp_path, monkeypatch):
    from test_store import LEGACY_SCHEMA
    import threading

    path = tmp_path / "legacy.db"
    raw_connect = sqlite3.connect
    with raw_connect(path) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(LEGACY_SCHEMA)
        connection.execute(
            "INSERT INTO chat_operations(client_operation_id, request_id, conversation_id, "
            "device_id, payload_sha256, payload_json, state, created_at, updated_at) "
            "VALUES('legacy', 'legacy', 'conversation', 'device', ?, ?, 'FAILED', 'then', 'then')",
            ("a" * 64, '{"workspace_id":"workspace-legacy","text":"synthetic"}'),
        )
    waiting = threading.Event()
    statements = []

    def traced_connect(*args, **kwargs):
        connection = raw_connect(*args, **kwargs)

        def trace(statement):
            statements.append(statement)
            if statement == "BEGIN IMMEDIATE":
                waiting.set()

        connection.set_trace_callback(trace)
        return connection

    monkeypatch.setattr(sqlite3, "connect", traced_connect)
    blocker = raw_connect(path)
    try:
        blocker.execute("BEGIN IMMEDIATE")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(ChatOperationStore(path).counts)
            try:
                assert waiting.wait(2), "migration did not attempt the writer lock"
                assert not future.done(), "migration bypassed the held writer lock"
                assert "workspace_id" not in {r[1] for r in blocker.execute("PRAGMA table_info(chat_operations)")}
            finally:
                blocker.rollback()
            assert future.result(timeout=5) == {"FAILED": 1}
    finally:
        blocker.close()
    row = ChatOperationStore(path).get("legacy")
    assert row is not None
    assert (row["sequence"], row["state"], row["payload_sha256"], row["created_at"]) == (1, "FAILED", "a" * 64, "then")
    assert row["workspace_id"] == "workspace-legacy"
    ChatOperationStore(path).counts()
    assert sum(s.startswith("ALTER TABLE") for s in statements) == 1
    assert sum(s.startswith("SELECT sequence, payload_json") for s in statements) == 1


def test_secure_delete_does_not_promise_erasure_of_live_wal_snapshots(tmp_path):
    path = tmp_path / "ops.db"
    store = ChatOperationStore(path)
    marker = "SYNTHETIC-LIVE-WAL-MARKER-" * 50
    store.submit(**submission(payload={"text": marker}))
    keeper = sqlite3.connect(path)
    try:
        keeper.execute("BEGIN")
        assert marker in keeper.execute("SELECT payload_json FROM chat_operations").fetchone()[0]
        store.claim_next()
        store.finish("op-original", state="COMPLETED")
        row = store.get("op-original")
        assert row is not None and row["payload"] is None
        # An older reader still holds a snapshot and old WAL frames remain.
        assert marker.encode() in path.with_name(path.name + "-wal").read_bytes()
        keeper.rollback()
        assert keeper.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0
    finally:
        keeper.close()
    assert marker.encode() not in path.read_bytes()


def test_python_payload_encoding_is_not_jcs_or_unicode_normalization():
    assert canonical_payload({"n": 1})[1] != canonical_payload({"n": 1.0})[1]
    assert canonical_payload({"s": "\u00e9"})[1] != canonical_payload({"s": "e\u0301"})[1]
