"""Execute every Python snippet and test the documented dispatcher with synthetic data."""
import asyncio
from pathlib import Path
import re
from types import SimpleNamespace
from typing import Any

import pytest

from durable_chat_queue import ChatOperationStore


ROOT = Path(__file__).resolve().parents[1]
READMES = ['README.md', 'README.ko.md', 'README.ja.md']


def python_blocks(text):
    return re.findall(r'```python\s*\n(.*?)\n```', text, re.S)


def dispatcher_namespace(text):
    blocks = python_blocks(text)
    assert len(blocks) == 2
    namespace: dict[str, Any] = {'__name__': 'readme_dispatcher'}
    exec(compile(blocks[0], '<readme-dispatcher>', 'exec'), namespace)
    return namespace


async def exercise_documented_dispatcher(text, path):
    namespace = dispatcher_namespace(text)
    store = ChatOperationStore(path)
    idle = asyncio.Event()
    blocked = asyncio.Event()
    done = asyncio.Event()
    stop = asyncio.Event()
    original_claim = store.claim_next

    def claim_next(**kwargs):
        row = original_claim(**kwargs)
        if row is None:
            idle.set()
            if store.counts().get('QUEUED'):
                blocked.set()
        return row

    store.claim_next = claim_next

    async def run_turn(payload):
        done.set()
        return SimpleNamespace(message_id='synthetic-tail-reply')

    namespace.update(run_turn=run_turn, ProvenPreSideEffectError=type('ProvenPreSideEffectError', (Exception,), {}))
    if 'start_drainers' in namespace:
        stop, tasks = await namespace['start_drainers'](store, 1)
    else:
        # Also exercise earlier drain-once documentation to reproduce its bug.
        tasks = [asyncio.create_task(namespace['drain'](store, 1))]
    try:
        await asyncio.wait_for(idle.wait(), timeout=2)
        # Yield until the initial empty scan has finished, before any submission.
        await asyncio.sleep(0)
        for op in ('inline', 'tail'):
            assert store.submit(client_operation_id=op, request_id=op,
                                conversation_id='synthetic-conversation', device_id='synthetic-device',
                                payload={'text': op})[0] == 'ACCEPTED'
            if op == 'inline':
                assert store.claim(op, lanes=1) is not None
        # Observe the blocked tail if the worker is resident. A drain-once task
        # will already have exited and must fail on the final QUEUED assertion.
        if not all(task.done() for task in tasks):
            await asyncio.wait_for(blocked.wait(), timeout=2)
        store.finish('inline', state='COMPLETED', message_id='synthetic-inline-reply')
        if not all(task.done() for task in tasks):
            await asyncio.wait_for(done.wait(), timeout=2)
            # execute() records completion before yielding control back here.
        tail = store.get('tail')
        assert tail is not None
        assert tail['state'] == 'COMPLETED', 'documented worker left the tail QUEUED without a new request'
    finally:
        stop.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=3)


@pytest.mark.parametrize('name', READMES)
def test_documented_dispatcher_does_not_leave_an_inline_tail_dormant(tmp_path, name):
    asyncio.run(exercise_documented_dispatcher((ROOT / name).read_text(encoding='utf-8'), tmp_path / 'docs.db'))


@pytest.mark.parametrize('name', READMES)
def test_readme_quickstart_python_block_runs(tmp_path, monkeypatch, name):
    monkeypatch.chdir(tmp_path)
    blocks = python_blocks((ROOT / name).read_text(encoding='utf-8'))
    namespace: dict[str, Any] = {'run_turn': lambda payload: 'synthetic-quickstart-reply'}
    exec(compile(blocks[1], name, 'exec'), namespace)
    assert namespace['disposition'] == 'ACCEPTED'
    assert namespace['store'].get('op-7f3a')['state'] == 'COMPLETED'


def test_readme_dispatcher_code_is_identical_in_all_languages():
    blocks = [python_blocks((ROOT / name).read_text(encoding='utf-8'))[0] for name in READMES]
    assert blocks[0] == blocks[1] == blocks[2]
