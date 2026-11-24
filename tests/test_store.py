"""Store contract tests ported from the original project.

Payload texts and identifiers are synthetic. Test names are unchanged from the
original suite so they can be traced back.
"""

import hashlib
import json
import sqlite3

from durable_chat_queue import ChatOperationStore


LEGACY_SCHEMA = (
    "CREATE TABLE chat_operations (sequence INTEGER PRIMARY KEY AUTOINCREMENT, "
    "client_operation_id TEXT NOT NULL UNIQUE, request_id TEXT NOT NULL UNIQUE, "
    "conversation_id TEXT NOT NULL, device_id TEXT, payload_sha256 TEXT NOT NULL, "
    "payload_json TEXT, state TEXT NOT NULL, message_id TEXT, error_code TEXT, "
    "created_at TEXT NOT NULL, updated_at TEXT NOT NULL, started_at TEXT, completed_at TEXT)"
)


def _payload(text: str) -> dict:
    return {
        "text": text,
        "thread": "main",
        "workspace_id": None,
        "client_context": None,
    }


def test_store_orders_devices_and_fences_exact_replay(tmp_path):
    store = ChatOperationStore(tmp_path / "execution.db")
    state_a, a = store.submit(
        client_operation_id="op_desktop_001",
        request_id="op_desktop_001",
        conversation_id="main",
        device_id="device_desktop",
        payload=_payload("request from desktop"),
    )
    state_b, b = store.submit(
        client_operation_id="op_mobile_001",
        request_id="op_mobile_001",
        conversation_id="main",
        device_id="device_mobile",
        payload=_payload("request from mobile"),
    )
    assert state_a == state_b == "ACCEPTED"
    assert a["sequence"] < b["sequence"]

    replay_state, replay = store.submit(
        client_operation_id="op_mobile_001",
        request_id="op_mobile_001",
        conversation_id="main",
        device_id="device_mobile",
        payload=_payload("request from mobile"),
    )
    assert replay_state == "REPLAY"
    assert replay["sequence"] == b["sequence"]

    conflict_state, _ = store.submit(
        client_operation_id="op_mobile_001",
        request_id="op_mobile_001",
        conversation_id="main",
        device_id="device_mobile",
        payload=_payload("same id, different content"),
    )
    assert conflict_state == "CONFLICT"

    first = store.claim_next()
    assert first["client_operation_id"] == "op_desktop_001"
    assert store.claim_next() is None
    terminal = store.finish(
        "op_desktop_001", state="COMPLETED", message_id="message_a"
    )
    assert terminal["payload"] is None
    second = store.claim_next()
    assert second["client_operation_id"] == "op_mobile_001"


def test_terminal_operation_keeps_workspace_authority_after_payload_compaction(tmp_path):
    store = ChatOperationStore(tmp_path / "execution.db")
    scoped = _payload("project-scoped phone request")
    scoped["workspace_id"] = "project:mobile"
    store.submit(
        client_operation_id="op_mobile_terminal_001",
        request_id="op_mobile_terminal_001",
        conversation_id="main",
        device_id="device_mobile",
        payload=scoped,
    )
    assert store.claim_next()["workspace_id"] == "project:mobile"
    terminal = store.finish("op_mobile_terminal_001", state="COMPLETED")
    assert terminal["payload"] is None
    assert terminal["workspace_id"] == "project:mobile"


def test_failed_operation_retains_exact_source_as_durable_recovery_inbox(tmp_path):
    store = ChatOperationStore(tmp_path / "execution.db")
    source = {**_payload("retain failed source"), "workspace_id": "project:mobile"}
    store.submit(
        client_operation_id="op_failed_source_001",
        request_id="op_failed_source_001",
        conversation_id="main",
        device_id="device_mobile",
        payload=source,
    )
    assert store.claim_next()["state"] == "RUNNING"
    failed = store.finish(
        "op_failed_source_001",
        state="FAILED",
        error_code="input_persistence_failed",
    )
    assert failed["payload"] == source
    assert failed["workspace_id"] == "project:mobile"
    assert store.claim_next() is None


def test_legacy_operation_table_migrates_workspace_column_before_transaction(tmp_path):
    path = tmp_path / "execution.db"
    with sqlite3.connect(path) as connection:
        connection.execute(LEGACY_SCHEMA)
    store = ChatOperationStore(path)
    state, row = store.submit(
        client_operation_id="op_legacy_schema_001",
        request_id="op_legacy_schema_001",
        conversation_id="main",
        device_id="device_mobile",
        payload={**_payload("legacy migration"), "workspace_id": "project:migrated"},
    )
    assert state == "ACCEPTED"
    assert row["workspace_id"] == "project:migrated"


def test_legacy_nonterminal_payload_workspace_is_backfilled_atomically(tmp_path):
    path = tmp_path / "execution.db"
    payload = {**_payload("legacy unknown"), "workspace_id": "project:legacy"}
    payload_json = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    with sqlite3.connect(path) as connection:
        connection.execute(LEGACY_SCHEMA)
        connection.execute(
            "INSERT INTO chat_operations(client_operation_id, request_id, conversation_id, "
            "device_id, payload_sha256, payload_json, state, created_at, updated_at) "
            "VALUES(?,?,?,?,?,?, 'UNKNOWN', 'now', 'now')",
            ("op_legacy_unknown", "op_legacy_unknown", "main", "old_phone", payload_sha256, payload_json),
        )
    row = ChatOperationStore(path).get("op_legacy_unknown")
    assert row["state"] == "UNKNOWN"
    assert row["workspace_id"] == "project:legacy"


def test_legacy_compacted_terminal_row_stays_unbound_instead_of_guessing(tmp_path):
    path = tmp_path / "execution.db"
    with sqlite3.connect(path) as connection:
        connection.execute(LEGACY_SCHEMA)
        connection.execute(
            "INSERT INTO chat_operations(client_operation_id, request_id, conversation_id, "
            "device_id, payload_sha256, payload_json, state, created_at, updated_at, completed_at) "
            "VALUES(?,?,?,?,?,NULL, 'COMPLETED', 'now', 'now', 'now')",
            ("op_legacy_terminal", "op_legacy_terminal", "main", "old_phone", "0" * 64),
        )
    row = ChatOperationStore(path).get("op_legacy_terminal")
    assert row["state"] == "COMPLETED"
    assert row["workspace_id"] is None
    assert row["payload"] is None


def test_restart_marks_running_unknown_but_keeps_never_started_queue(tmp_path):
    store = ChatOperationStore(tmp_path / "execution.db")
    for suffix in ("running", "queued"):
        store.submit(
            client_operation_id=f"op_{suffix}_001",
            request_id=f"op_{suffix}_001",
            conversation_id="main",
            device_id="device_mobile",
            payload=_payload(suffix),
        )
    assert store.claim_next()["client_operation_id"] == "op_running_001"
    assert store.recover_inflight() == 1
    assert store.get("op_running_001")["state"] == "UNKNOWN"
    assert store.claim_next()["client_operation_id"] == "op_queued_001"


# --- Per-conversation lanes ---------------------------------------------------


def _store(tmp_path):
    return ChatOperationStore(tmp_path / "ops.db")


def _submit(store, op, conv, text="body"):
    store.submit(client_operation_id=op, request_id="rq_" + op,
                 conversation_id=conv, device_id="device_t",
                 payload={"text": text, "thread": conv})


def test_claim_next_skips_running_conversation_with_two_lanes(tmp_path):
    store = _store(tmp_path)
    _submit(store, "op_a1_00000001", "main")
    _submit(store, "op_a2_00000002", "main")
    # Non-ASCII conversation id on purpose (identifiers are stored verbatim).
    _submit(store, "op_b1_00000003", "대화-b")
    first = store.claim_next(lanes=2)
    assert first["client_operation_id"] == "op_a1_00000001"
    # "main" is RUNNING: skip its op_a2 and take the oldest op of another conversation.
    second = store.claim_next(lanes=2)
    assert second["client_operation_id"] == "op_b1_00000003"
    # Both conversations are RUNNING: the 2-lane limit is reached.
    assert store.claim_next(lanes=2) is None


def test_claim_next_single_lane_keeps_old_serial_behavior(tmp_path):
    store = _store(tmp_path)
    _submit(store, "op_a1_00000001", "main")
    _submit(store, "op_b1_00000002", "대화-b")
    assert store.claim_next(lanes=1)["client_operation_id"] == "op_a1_00000001"
    assert store.claim_next(lanes=1) is None   # one lane = fully serial, as before


def test_claim_exact_enforces_per_conversation_fifo(tmp_path):
    store = _store(tmp_path)
    _submit(store, "op_a1_00000001", "main")
    _submit(store, "op_a2_00000002", "main")
    # An exact claim of a later op is refused while an older one in the same
    # conversation is still queued.
    assert store.claim(client_operation_id="op_a2_00000002", lanes=2) is None
    assert store.claim(client_operation_id="op_a1_00000001",
                       lanes=2)["client_operation_id"] == "op_a1_00000001"
