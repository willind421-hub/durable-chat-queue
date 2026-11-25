"""A small threaded dispatcher on top of ChatOperationStore (synthetic data only).

It walks through the situations a chat/agent backend has to survive:

1. A previous process crashed while a turn was RUNNING. On restart the turn is
   fenced as UNKNOWN (never silently re-run); never-started turns still drain.
2. Clients retry and race: identical retries are REPLAYs, a reused id with
   different content is a CONFLICT. Neither creates a second turn.
3. Several workers drain the queue with ``lanes=2``: different conversations run
   in parallel, turns of the same conversation run strictly one after another
   in submission order, and a new message never interrupts a running turn.
4. Failures are recorded honestly: a failure proven to happen before any side
   effect is FAILED, anything ambiguous is UNKNOWN. Both keep their source.

Run from the repository root::

    python examples/threaded_dispatcher.py
    python examples/threaded_dispatcher.py --db ./demo.db --workers 4 --lanes 2
"""

from __future__ import annotations

import argparse
from pathlib import Path
import tempfile
import threading
import time

from durable_chat_queue import ChatOperationStore


def reserve_new_database(path: Path) -> None:
    """Reserve a fresh demo file; never open an existing DB or its sidecars.

    Use a trusted directory with no other process writing this demo path.
    Exclusive creation also rejects a competing creator and existing symlinks.
    """
    path = Path(path)
    for candidate in [path, *(Path(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal"))]:
        if candidate.exists() or candidate.is_symlink():
            raise FileExistsError(f"Refusing existing demo database or sidecar: {candidate}")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb"):
            pass
    except FileExistsError:
        raise FileExistsError(f"Refusing existing demo database: {path}") from None


class ProvenPreSideEffectError(Exception):
    """The turn was rejected before anything observable happened."""


def fake_agent_turn(payload: dict) -> str:
    """Stand-in for a model/tool call. Returns a synthetic message id."""

    text = str(payload["text"])
    if text.startswith("!reject"):
        raise ProvenPreSideEffectError("input rejected before the model was called")
    if text.startswith("!crash"):
        time.sleep(0.02)
        raise RuntimeError("connection lost after the model call started")
    time.sleep(0.05)
    return "msg-" + text.split()[0]


def submit(store: ChatOperationStore, op: str, conversation: str, text: str) -> str:
    disposition, _row = store.submit(
        client_operation_id=op,
        request_id=op,
        conversation_id=conversation,
        device_id="device-demo",
        payload={"text": text, "thread": conversation},
    )
    return disposition


def run_workers(
    store: ChatOperationStore, *, workers: int, lanes: int,
    stop: threading.Event | None = None,
) -> list[tuple]:
    """Return (op, conv, start, end) spans from synthetic worker threads.

    Without ``stop``, drain a finite preloaded batch (30-second safety bound).
    With ``stop``, stay resident even when idle; poll until it is set. This
    also picks up queued tails after an inline executor releases its lane,
    without relying on a notification that could race with an empty scan.
    Stop admission before setting the event; current executions finish, but
    remaining QUEUED work is left durable for the next startup.
    """

    spans: list[tuple] = []
    lock = threading.Lock()
    origin = time.monotonic()

    def execute(row: dict) -> None:
        op = row["client_operation_id"]
        start = time.monotonic() - origin
        message_id = error_code = None
        try:
            message_id = fake_agent_turn(row["payload"])
            state = "COMPLETED"
        except ProvenPreSideEffectError as exc:
            state, error_code = "FAILED", type(exc).__name__
        except Exception as exc:  # outcome unknown: never retry automatically
            state, error_code = "UNKNOWN", f"{type(exc).__name__}:outcome_unknown"
        end = time.monotonic() - origin
        with lock:
            spans.append((op, row["conversation_id"], start, end))
        # The receipt is written last: until then the conversation stays RUNNING,
        # so no other worker can start the next turn of this conversation.
        store.finish(op, state=state, message_id=message_id, error_code=error_code)

    def worker() -> None:
        deadline = time.monotonic() + 30
        while stop is None or not stop.is_set():
            if stop is None and time.monotonic() >= deadline:
                return
            row = store.claim_next(lanes=lanes)
            if row is None:
                if stop is None and store.counts().get("QUEUED", 0) == 0:
                    return
                if stop is None:
                    time.sleep(0.005)  # finite batch: wait for a busy lane
                else:
                    stop.wait(0.005)  # resident: also wait when the queue is empty
                continue
            execute(row)

    threads = [threading.Thread(target=worker, name=f"worker-{i}") for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return spans


def check_spans(spans: list[tuple], submitted: dict[str, list[str]], lanes: int) -> None:
    by_conv: dict[str, list[tuple]] = {}
    for span in sorted(spans, key=lambda s: s[2]):
        by_conv.setdefault(span[1], []).append(span)
    for conv, items in by_conv.items():
        order = [op for op, *_ in items]
        assert order == submitted[conv], (conv, order, submitted[conv])
        for earlier, later in zip(items, items[1:]):
            assert earlier[3] <= later[2], f"overlap inside {conv}"
    events = sorted([(s[2], 1) for s in spans] + [(s[3], -1) for s in spans])
    active = peak = 0
    for _, delta in events:
        active += delta
        peak = max(peak, active)
    assert peak <= lanes, peak
    print(f"  checked: per-conversation FIFO, no overlap inside a conversation, "
          f"peak parallel turns = {peak} (lanes = {lanes})")


def main(db_path: Path, *, workers: int, lanes: int) -> None:
    if workers < 1 or lanes < 1:
        raise ValueError("workers and lanes must be positive")
    reserve_new_database(db_path)
    print(f"database: {db_path}")

    print("\n[1] previous process: one turn RUNNING, one still QUEUED, then a crash")
    before_crash = ChatOperationStore(db_path)
    submit(before_crash, "op-0001", "alice", "a1 summarise the meeting notes")
    submit(before_crash, "op-0002", "alice", "a2 and draft a reply")
    before_crash.claim_next(lanes=lanes)  # op-0001 starts ... and the process dies

    print("\n[2] restart: fence uncertain work, keep never-started work")
    store = ChatOperationStore(db_path)
    print(f"  recover_inflight() fenced {store.recover_inflight()} operation(s)")
    for op in ("op-0001", "op-0002"):
        row = store.get(op)
        print(f"  {op}: {row['state']:<8} error_code={row['error_code']}")

    print("\n[3] new submissions, retries and an id collision")
    plan = [
        ("op-0003", "alice", "a3 thanks, send it"),
        ("op-0004", "bob", "b1 plan a trip"),
        ("op-0005", "bob", "b2 add a museum day"),
        ("op-0006", "carol", "c1 translate this paragraph"),
        ("op-0007", "bob", "b3 what about the budget?"),
        ("op-0008", "carol", "!reject c2 empty attachment"),
        ("op-0009", "carol", "!crash c3 call the external tool"),
    ]
    for op, conv, text in plan:
        print(f"  submit {op} ({conv}): {submit(store, op, conv, text)}")
    print(f"  retry  op-0004 with the same bytes:      {submit(store, 'op-0004', 'bob', 'b1 plan a trip')}")
    print(f"  reuse  op-0004 with different content:   {submit(store, 'op-0004', 'bob', 'b1 something else')}")

    print(f"\n[4] drain with {workers} worker thread(s), lanes={lanes}")
    spans = run_workers(store, workers=workers, lanes=lanes)
    for op, conv, start, end in sorted(spans, key=lambda s: s[2]):
        print(f"  {start:6.3f}s -> {end:6.3f}s  {conv:<6} {op}")
    submitted: dict[str, list[str]] = {}
    for op, conv, _text in [("op-0002", "alice", "")] + plan:
        submitted.setdefault(conv, []).append(op)
    check_spans(spans, submitted, lanes)

    print("\n[5] receipts")
    for conv in ("alice", "bob", "carol"):
        for row in store.list(conversation_id=conv):
            kept = "payload kept" if row["payload"] else "payload cleared"
            print(f"  {row['client_operation_id']} {conv:<6} {row['state']:<9} "
                  f"message_id={row['message_id']} error_code={row['error_code']} ({kept})")
    print(f"  counts: {dict(sorted(store.counts().items()))}")
    print("  FAILED/UNKNOWN rows stay out of the queue with their exact source;"
          " a person or client decides what to do with them.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", type=Path, help="NEW SQLite file only; existing DBs/sidecars are refused (default: temporary)")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--lanes", type=int, default=2)
    args = parser.parse_args()
    try:
        if args.db:
            main(args.db, workers=args.workers, lanes=args.lanes)
        else:
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
                main(Path(tmp) / "operations.db", workers=args.workers, lanes=args.lanes)
    except (FileExistsError, ValueError) as exc:
        parser.error(str(exc))
