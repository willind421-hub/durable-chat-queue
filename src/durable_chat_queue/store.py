"""Durable, idempotent command inbox for chat turns submitted from many devices.

The conversation history (kept elsewhere) remains the message authority.  This
store owns only submission identity, server acceptance order, transient queued
payloads, and terminal execution receipts.  Payloads are cleared after a known
terminal state (COMPLETED or CANCELLED); FAILED and UNKNOWN rows keep their
exact source as a recovery inbox.  An operation that was RUNNING when the
service stopped becomes UNKNOWN and is never retried automatically.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any


TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def canonical_payload(payload: dict[str, Any]) -> tuple[str, str]:
    """Return ``(canonical_json, sha256_hex)`` for a payload.

    Keys are sorted and separators are compact, so payloads with the same
    content always produce the same bytes and digest regardless of key order.
    """

    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class ChatOperationStore:
    """SQLite command inbox with exact-operation replay protection."""

    def __init__(self, path: Path):
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS chat_operations (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                client_operation_id TEXT NOT NULL UNIQUE,
                request_id TEXT NOT NULL UNIQUE,
                conversation_id TEXT NOT NULL,
                device_id TEXT,
                workspace_id TEXT,
                payload_sha256 TEXT NOT NULL,
                payload_json TEXT,
                state TEXT NOT NULL,
                message_id TEXT,
                error_code TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_chat_operations_queue
                ON chat_operations(state, sequence);
            CREATE INDEX IF NOT EXISTS idx_chat_operations_conversation
                ON chat_operations(conversation_id, sequence);
            """
        )
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(chat_operations)").fetchall()
        }
        if "workspace_id" not in columns:
            connection.execute("ALTER TABLE chat_operations ADD COLUMN workspace_id TEXT")
        # Older rows carried their workspace scope only inside the transient
        # payload. Recover that scope while the payload still exists; terminal
        # rows whose payload was already compacted remain explicitly unbound
        # and must never be guessed or rebound to another device.
        legacy_rows = connection.execute(
            "SELECT sequence, payload_json FROM chat_operations "
            "WHERE workspace_id IS NULL AND payload_json IS NOT NULL"
        ).fetchall()
        for legacy_row in legacy_rows:
            try:
                payload = json.loads(str(legacy_row["payload_json"]))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            workspace_id = (
                str(payload.get("workspace_id") or "").strip()
                if isinstance(payload, dict)
                else ""
            )
            if not workspace_id or len(workspace_id) > 80:
                continue
            connection.execute(
                "UPDATE chat_operations SET workspace_id=? "
                "WHERE sequence=? AND workspace_id IS NULL",
                (workspace_id, int(legacy_row["sequence"])),
            )
        connection.commit()
        return connection

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        """Open a connection, commit (or roll back) on exit, then close it.

        Closing explicitly releases the file handle immediately instead of
        relying on garbage collection (Python 3.13+ warns about unclosed
        connections).
        """

        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    @staticmethod
    def _decode(row: sqlite3.Row | None, *, include_payload: bool = True) -> dict | None:
        if row is None:
            return None
        result = dict(row)
        raw = result.pop("payload_json", None)
        payload = None
        if include_payload and raw:
            try:
                decoded = json.loads(str(raw))
                payload = decoded if isinstance(decoded, dict) else None
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = None
        if include_payload:
            result["payload"] = payload
        return result

    def submit(
        self,
        *,
        client_operation_id: str,
        request_id: str,
        conversation_id: str,
        device_id: str | None,
        payload: dict[str, Any],
    ) -> tuple[str, dict]:
        """Insert once; replay same bytes and reject identity reuse with new bytes."""

        payload_json, payload_sha256 = canonical_payload(payload)
        workspace_id = str(payload.get("workspace_id") or "") or None
        now = _now()
        with self._session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM chat_operations WHERE client_operation_id=?",
                (client_operation_id,),
            ).fetchone()
            if row is not None:
                decoded = self._decode(row)
                if str(row["payload_sha256"]) != payload_sha256:
                    return "CONFLICT", decoded
                return "REPLAY", decoded
            try:
                connection.execute(
                    """
                    INSERT INTO chat_operations(
                        client_operation_id, request_id, conversation_id,
                        device_id, workspace_id, payload_sha256, payload_json, state,
                        created_at, updated_at
                    ) VALUES(?,?,?,?,?,?,?, 'QUEUED', ?,?)
                    """,
                    (
                        client_operation_id,
                        request_id,
                        conversation_id,
                        device_id,
                        workspace_id,
                        payload_sha256,
                        payload_json,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                # A request_id may not be rebound to a second operation either.
                row = connection.execute(
                    "SELECT * FROM chat_operations WHERE request_id=?",
                    (request_id,),
                ).fetchone()
                return "CONFLICT", self._decode(row) or {
                    "client_operation_id": client_operation_id,
                    "request_id": request_id,
                }
            row = connection.execute(
                "SELECT * FROM chat_operations WHERE client_operation_id=?",
                (client_operation_id,),
            ).fetchone()
        return "ACCEPTED", self._decode(row)

    def confirm_missing(
        self,
        *,
        client_operation_id: str,
        request_id: str,
        conversation_id: str,
        device_id: str,
        workspace_id: str,
        payload_sha256: str,
    ) -> tuple[str, dict]:
        """Atomically tombstone an absent identity so a late POST cannot run."""

        now = _now()
        sealed_payload = json.dumps(
            {"workspace_id": workspace_id, "sealed_missing": True},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )
        with self._session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM chat_operations WHERE client_operation_id=?",
                (client_operation_id,),
            ).fetchone()
            if row is not None:
                return "EXISTING", self._decode(row)
            try:
                connection.execute(
                    """
                    INSERT INTO chat_operations(
                        client_operation_id, request_id, conversation_id,
                        device_id, workspace_id, payload_sha256, payload_json, state,
                        error_code, created_at, updated_at, completed_at
                    ) VALUES(?,?,?,?,?,?,?, 'CONFIRMED_MISSING',
                             'client_confirmed_missing', ?,?,?)
                    """,
                    (
                        client_operation_id, request_id, conversation_id,
                        device_id, workspace_id, payload_sha256, sealed_payload,
                        now, now, now,
                    ),
                )
            except sqlite3.IntegrityError:
                row = connection.execute(
                    "SELECT * FROM chat_operations WHERE client_operation_id=? "
                    "OR request_id=? LIMIT 1",
                    (client_operation_id, request_id),
                ).fetchone()
                return "EXISTING", self._decode(row) or {}
            row = connection.execute(
                "SELECT * FROM chat_operations WHERE client_operation_id=?",
                (client_operation_id,),
            ).fetchone()
        return "CONFIRMED_MISSING", self._decode(row)

    def claim(self, client_operation_id: str, *, lanes: int = 1) -> dict | None:
        """Claim one exact queued operation if its conversation lane is free.

        Serialisation is enforced only *within* a conversation. Another
        conversation being RUNNING does not block this one as long as the
        number of active conversations stays below ``lanes``. The target must
        also be the oldest QUEUED operation of its conversation.
        """

        now = _now()
        with self._session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            running_rows = connection.execute(
                "SELECT DISTINCT conversation_id FROM chat_operations "
                "WHERE state='RUNNING'"
            ).fetchall()
            running = {str(r["conversation_id"]) for r in running_rows}
            target = connection.execute(
                "SELECT conversation_id, sequence FROM chat_operations "
                "WHERE client_operation_id=? AND state='QUEUED'",
                (client_operation_id,),
            ).fetchone()
            if target is None:
                return None
            conversation = str(target["conversation_id"])
            if conversation in running or len(running) >= max(1, int(lanes)):
                return None
            oldest = connection.execute(
                "SELECT MIN(sequence) AS s FROM chat_operations "
                "WHERE state='QUEUED' AND conversation_id=?",
                (conversation,),
            ).fetchone()
            if oldest is None or int(target["sequence"]) != int(oldest["s"]):
                return None   # an older submission in this conversation goes first (FIFO)
            changed = connection.execute(
                """
                UPDATE chat_operations
                SET state='RUNNING', started_at=?, updated_at=?
                WHERE client_operation_id=? AND state='QUEUED'
                """,
                (now, now, client_operation_id),
            ).rowcount
            if changed != 1:
                return None
            row = connection.execute(
                "SELECT * FROM chat_operations WHERE client_operation_id=?",
                (client_operation_id,),
            ).fetchone()
        return self._decode(row)

    def claim_next(self, *, lanes: int = 1) -> dict | None:
        """Claim the oldest queued operation whose conversation lane is free.

        Order within a conversation strictly follows ``sequence`` (serial per
        conversation) and conversations that already have a RUNNING operation
        are skipped. When the number of active conversations reaches
        ``lanes``, nothing is claimed.
        """

        now = _now()
        with self._session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            running_rows = connection.execute(
                "SELECT DISTINCT conversation_id FROM chat_operations "
                "WHERE state='RUNNING'"
            ).fetchall()
            running = {str(r["conversation_id"]) for r in running_rows}
            if len(running) >= max(1, int(lanes)):
                return None
            if running:
                marks = ",".join("?" for _ in running)
                row = connection.execute(
                    f"""
                    SELECT client_operation_id FROM chat_operations
                    WHERE state='QUEUED' AND conversation_id NOT IN ({marks})
                    ORDER BY sequence ASC LIMIT 1
                    """,
                    tuple(sorted(running)),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT client_operation_id FROM chat_operations
                    WHERE state='QUEUED' ORDER BY sequence ASC LIMIT 1
                    """
                ).fetchone()
            if row is None:
                return None
            operation_id = str(row["client_operation_id"])
            connection.execute(
                """
                UPDATE chat_operations
                SET state='RUNNING', started_at=?, updated_at=?
                WHERE client_operation_id=? AND state='QUEUED'
                """,
                (now, now, operation_id),
            )
            claimed = connection.execute(
                "SELECT * FROM chat_operations WHERE client_operation_id=?",
                (operation_id,),
            ).fetchone()
        return self._decode(claimed)

    def finish(
        self,
        client_operation_id: str,
        *,
        state: str,
        message_id: str | None = None,
        error_code: str | None = None,
    ) -> dict:
        """Record the terminal receipt of a RUNNING operation.

        ``state`` must be COMPLETED, FAILED, CANCELLED or UNKNOWN; any other
        value raises ``ValueError``. Raises ``KeyError`` if the operation is
        not currently RUNNING.
        """

        if state not in TERMINAL_STATES and state != "UNKNOWN":
            raise ValueError(f"invalid terminal operation state: {state}")
        now = _now()
        # FAILED is a durable recovery inbox, not proof that no useful source
        # remains. Preserve its exact payload. UNKNOWN likewise stays fenced
        # with its source and is never claimed automatically.
        clear_payload = state in {"COMPLETED", "CANCELLED"}
        with self._session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """
                UPDATE chat_operations
                SET state=?, message_id=?, error_code=?, updated_at=?,
                    completed_at=?, payload_json=CASE WHEN ? THEN NULL ELSE payload_json END
                WHERE client_operation_id=? AND state='RUNNING'
                """,
                (
                    state,
                    message_id,
                    error_code,
                    now,
                    now,
                    1 if clear_payload else 0,
                    client_operation_id,
                ),
            ).rowcount
            if changed != 1:
                raise KeyError(client_operation_id)
            row = connection.execute(
                "SELECT * FROM chat_operations WHERE client_operation_id=?",
                (client_operation_id,),
            ).fetchone()
        return self._decode(row)

    def requeue(self, client_operation_id: str, *, error_code: str | None = None) -> None:
        """Return a RUNNING operation to QUEUED under the same identity.

        Intended only for work that provably never started (for example, the
        executor refused it because its lane was busy). The original sequence
        is kept, so the operation does not lose its place in the queue.
        """

        now = _now()
        with self._session() as connection:
            connection.execute(
                """
                UPDATE chat_operations
                SET state='QUEUED', started_at=NULL, updated_at=?, error_code=?
                WHERE client_operation_id=? AND state='RUNNING'
                """,
                (now, error_code, client_operation_id),
            )

    def recover_inflight(self) -> int:
        """Fence crash-ambiguous work; only never-started QUEUED rows may drain."""

        now = _now()
        with self._session() as connection:
            return connection.execute(
                """
                UPDATE chat_operations
                SET state='UNKNOWN', error_code='service_restart_inflight_unknown',
                    updated_at=?, completed_at=?
                WHERE state='RUNNING'
                """,
                (now, now),
            ).rowcount

    def get(self, client_operation_id: str) -> dict | None:
        """Return one operation (with its payload, if retained) or ``None``."""

        with self._session() as connection:
            row = connection.execute(
                "SELECT * FROM chat_operations WHERE client_operation_id=?",
                (client_operation_id,),
            ).fetchone()
        return self._decode(row)

    def list(
        self,
        *,
        conversation_id: str,
        after_sequence: int = 0,
        limit: int = 50,
    ) -> list[dict]:
        """List one conversation's operations after ``after_sequence`` (1..100 rows)."""

        bounded = max(1, min(int(limit), 100))
        with self._session() as connection:
            rows = connection.execute(
                """
                SELECT * FROM chat_operations
                WHERE conversation_id=? AND sequence>?
                ORDER BY sequence ASC LIMIT ?
                """,
                (conversation_id, max(0, int(after_sequence)), bounded),
            ).fetchall()
        return [self._decode(row) for row in rows]

    def counts(self) -> dict[str, int]:
        """Return the number of operations per state."""

        with self._session() as connection:
            rows = connection.execute(
                "SELECT state, COUNT(*) AS count FROM chat_operations GROUP BY state"
            ).fetchall()
        return {str(row["state"]): int(row["count"]) for row in rows}
