"""A resident drainer must survive idle periods and inline execution."""
from concurrent.futures import ThreadPoolExecutor
import importlib
from pathlib import Path
import threading

from durable_chat_queue import ChatOperationStore


def test_resident_worker_dispatches_queued_tail_after_inline_completion(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / 'examples'))
    demo = importlib.import_module('threaded_dispatcher')
    store = ChatOperationStore(tmp_path / 'ops.db')
    stop = threading.Event()
    empty_scan = threading.Event()
    allow_scan = threading.Event()
    blocked_scan = threading.Event()
    tail_done = threading.Event()
    original_claim = store.claim_next
    original_finish = store.finish

    def claim_next(**kwargs):
        row = original_claim(**kwargs)
        if row is None:
            if not empty_scan.is_set():
                empty_scan.set()
                assert allow_scan.wait(3), 'test did not release the initial idle scan'
            if store.counts().get('QUEUED'):
                blocked_scan.set()
        return row

    def finish(op, **kwargs):
        row = original_finish(op, **kwargs)
        if op == 'op-tail':
            tail_done.set()
        return row

    monkeypatch.setattr(store, 'claim_next', claim_next)
    monkeypatch.setattr(store, 'finish', finish)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(demo.run_workers, store, workers=1, lanes=1, stop=stop)
        try:
            assert empty_scan.wait(2), 'resident worker did not scan the initially empty queue'
            assert demo.submit(store, 'op-inline', 'conversation', 'inline synthetic') == 'ACCEPTED'
            # Exact inline claims and drainer claims share the same store protocol.
            inline = store.claim('op-inline', lanes=1)
            assert inline is not None
            demo.submit(store, 'op-tail', 'conversation', 'tail synthetic')
            allow_scan.set()
            assert blocked_scan.wait(2), 'worker did not observe the blocked queued tail'
            store.finish('op-inline', state='COMPLETED', message_id='inline-reply')
            assert tail_done.wait(2), 'queued tail slept forever without another submission'
            assert store.get('op-tail')['state'] == 'COMPLETED'
        finally:
            allow_scan.set()
            stop.set()
        spans = future.result(timeout=3)
    assert any(span[0] == 'op-tail' for span in spans)


def test_submission_at_empty_scan_boundary_is_not_a_lost_wakeup(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / 'examples'))
    demo = importlib.import_module('threaded_dispatcher')
    store = ChatOperationStore(tmp_path / 'ops.db')
    stop = threading.Event()
    original_claim = store.claim_next
    original_finish = store.finish
    injected = False

    def claim_next(**kwargs):
        nonlocal injected
        row = original_claim(**kwargs)
        if row is None and not injected:
            injected = True
            demo.submit(store, 'op-boundary', 'conversation', 'boundary synthetic')
        return row

    def finish(op, **kwargs):
        row = original_finish(op, **kwargs)
        stop.set()
        return row

    monkeypatch.setattr(store, 'claim_next', claim_next)
    monkeypatch.setattr(store, 'finish', finish)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(demo.run_workers, store, workers=1, lanes=1, stop=stop)
        try:
            spans = future.result(timeout=3)
        finally:
            stop.set()
    assert injected
    assert store.get('op-boundary')['state'] == 'COMPLETED'
    assert [span[0] for span in spans] == ['op-boundary']
