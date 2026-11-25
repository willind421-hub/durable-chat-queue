"""Additional contract tests written for the standalone release.

They cover store behaviour that the original suite exercised only through an
HTTP router (which is not part of this package): request_id rebinding,
negative receipts, terminal-state validation, requeue, listing, and
concurrent claims from several threads.
"""

import threading
import time

import pytest

from durable_chat_queue import ChatOperationStore, canonical_payload


def _submit(store, op, conv="main", text="body", request_id=None, workspace_id=None):
    return store.submit(
        client_operation_id=op,
        request_id=request_id or op,
        conversation_id=conv,
        device_id="device_test",
        payload={"text": text, "thread": conv, "workspace_id": workspace_id},
    )


def test_canonical_payload_ignores_key_order_and_keeps_unicode():
    a_json, a_sha = canonical_payload({"b": 1, "a": "héllo"})
    b_json, b_sha = canonical_payload({"a": "héllo", "b": 1})
    assert a_json == b_json == '{"a":"héllo","b":1}'
    assert a_sha == b_sha and len(a_sha) == 64


def test_request_id_cannot_be_rebound_to_another_operation(tmp_path):
    store = ChatOperationStore(tmp_path / "ops.db")
    assert _submit(store, "op_first", request_id="rq_shared")[0] == "ACCEPTED"
    state, row = _submit(store, "op_second", request_id="rq_shared")
    assert state == "CONFLICT"
    assert row == {}  # conflicting callers must not receive the existing row
    assert store.get("op_second") is None
    assert store.counts() == {"QUEUED": 1}


def test_unknown_outcome_keeps_source_and_is_never_claimed(tmp_path):
    # Store-level part of the original router test
    # test_exact_operation_reconciliation_is_read_only_and_preserves_unknown.
    store = ChatOperationStore(tmp_path / "ops.db")
    _submit(store, "op_unknown_001", text="ambiguous request")
    assert store.claim("op_unknown_001")["state"] == "RUNNING"
    row = store.finish(
        "op_unknown_001", state="UNKNOWN", error_code="transport_outcome_unknown"
    )
    assert row["state"] == "UNKNOWN"
    assert row["payload"]["text"] == "ambiguous request"
    assert len(row["payload_sha256"]) == 64
    assert store.claim_next() is None
    assert store.claim("op_unknown_001") is None
    assert store.get("op_unknown_001")["state"] == "UNKNOWN"
    assert store.get("op_missing_001") is None


def test_confirm_missing_seals_identity_so_late_submit_never_runs(tmp_path):
    store = ChatOperationStore(tmp_path / "ops.db")
    payload = {"text": "lost in transit", "thread": "main", "workspace_id": "ws:1"}
    _, digest = canonical_payload(payload)
    state, sealed = store.confirm_missing(
        client_operation_id="op_lost_001",
        request_id="op_lost_001",
        conversation_id="main",
        device_id="device_test",
        workspace_id="ws:1",
        payload_sha256=digest,
    )
    assert state == "CONFIRMED_MISSING"
    assert sealed["state"] == "CONFIRMED_MISSING"
    assert sealed["error_code"] == "client_confirmed_missing"
    assert sealed["payload"] == {"workspace_id": "ws:1", "sealed_missing": True}

    # The original request finally arrives: it is recognised, but never queued.
    late_state, late = store.submit(
        client_operation_id="op_lost_001",
        request_id="op_lost_001",
        conversation_id="main",
        device_id="device_test",
        payload=payload,
    )
    assert late_state == "REPLAY"
    assert late["state"] == "CONFIRMED_MISSING"
    assert store.claim_next() is None
    assert store.claim("op_lost_001") is None


def test_confirm_missing_reports_existing_operation(tmp_path):
    store = ChatOperationStore(tmp_path / "ops.db")
    _submit(store, "op_present_001", workspace_id="ws:1")
    state, row = store.confirm_missing(
        client_operation_id="op_present_001",
        request_id="op_present_001",
        conversation_id="main",
        device_id="device_test",
        workspace_id="ws:1",
        payload_sha256="0" * 64,
    )
    assert state == "EXISTING"
    assert row["state"] == "QUEUED"


def test_finish_rejects_invalid_state_and_operations_not_running(tmp_path):
    store = ChatOperationStore(tmp_path / "ops.db")
    _submit(store, "op_001")
    with pytest.raises(ValueError):
        store.finish("op_001", state="QUEUED")
    with pytest.raises(KeyError):
        store.finish("op_001", state="COMPLETED")  # still QUEUED, never claimed
    with pytest.raises(KeyError):
        store.finish("op_does_not_exist", state="COMPLETED")
    store.claim_next()
    cancelled = store.finish("op_001", state="CANCELLED", error_code="cancelled")
    assert cancelled["payload"] is None
    with pytest.raises(KeyError):
        store.finish("op_001", state="COMPLETED")  # terminal receipts are final


def test_requeue_keeps_identity_and_position(tmp_path):
    store = ChatOperationStore(tmp_path / "ops.db")
    _, first = _submit(store, "op_001")
    _submit(store, "op_002")
    assert store.claim_next()["client_operation_id"] == "op_001"
    store.requeue("op_001", error_code="lane_busy")
    row = store.get("op_001")
    assert row["state"] == "QUEUED"
    assert row["started_at"] is None
    assert row["error_code"] == "lane_busy"
    again = store.claim_next()
    assert again["client_operation_id"] == "op_001"
    assert again["sequence"] == first["sequence"]


def test_list_and_counts(tmp_path):
    store = ChatOperationStore(tmp_path / "ops.db")
    for index in range(3):
        _submit(store, f"op_main_{index}", conv="main")
    _submit(store, "op_other_0", conv="other")
    rows = store.list(conversation_id="main")
    assert [r["client_operation_id"] for r in rows] == ["op_main_0", "op_main_1", "op_main_2"]
    after_first = store.list(conversation_id="main", after_sequence=rows[0]["sequence"])
    assert [r["client_operation_id"] for r in after_first] == ["op_main_1", "op_main_2"]
    assert len(store.list(conversation_id="main", limit=1)) == 1
    assert store.counts() == {"QUEUED": 4}
    store.claim_next(lanes=2)
    assert store.counts() == {"QUEUED": 3, "RUNNING": 1}


def test_concurrent_workers_never_overlap_a_conversation(tmp_path):
    store = ChatOperationStore(tmp_path / "ops.db")
    expected: dict[str, list[str]] = {}
    for conv in ("conv-a", "conv-b", "conv-c"):
        for index in range(4):
            op = f"op_{conv}_{index}"
            _submit(store, op, conv=conv)
            expected.setdefault(conv, []).append(op)

    lock = threading.Lock()
    active: set[str] = set()
    executed: dict[str, list[str]] = {}
    errors: list[str] = []
    lanes = 2
    max_parallel = 0

    def worker() -> None:
        nonlocal max_parallel
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            row = store.claim_next(lanes=lanes)
            if row is None:
                if store.counts().get("QUEUED", 0) == 0:
                    return
                time.sleep(0.005)  # lanes busy or only busy conversations queued
                continue
            conv = row["conversation_id"]
            with lock:
                if conv in active:
                    errors.append(f"overlap in {conv}")
                active.add(conv)
                max_parallel = max(max_parallel, len(active))
                executed.setdefault(conv, []).append(row["client_operation_id"])
            time.sleep(0.01)
            with lock:
                active.discard(conv)
            store.finish(row["client_operation_id"], state="COMPLETED")

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    assert executed == expected
    assert max_parallel <= lanes
    assert store.counts() == {"COMPLETED": 12}
