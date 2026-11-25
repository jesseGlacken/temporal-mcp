"""Black-box fixtures: a real Temporal dev server, a real worker, and an in-process MCP client.

Tests in this package talk to the server only through MCP (``list_tools`` / ``call_tool``)
and assert only on tool results. Nothing here mocks Temporal.
"""

import asyncio
import json
import os
import time
import uuid
from typing import Any, Awaitable, Callable, Sequence

import pytest
import pytest_asyncio
from mcp.client import Client
from temporalio import workflow
from temporalio.common import RawValue
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from temporal_mcp.server import TemporalMCPServer

DEFAULT_NAMESPACE = "default"
OTHER_NAMESPACE = "mcp-bb-other"
FORBIDDEN_NAMESPACE = "mcp-bb-forbidden"
TASK_QUEUE = "mcp-bb-workflows"
# No worker polls this queue, so standalone activities scheduled on it stay pending.
IDLE_TASK_QUEUE = "mcp-bb-idle"

CallFn = Callable[..., Awaitable[tuple[bool, Any]]]


@workflow.defn(name="BlackboxWaiter")
class BlackboxWaiter:
    """Records every signal it receives and waits until signalled 'finish'."""

    def __init__(self) -> None:
        self._signals: list[dict[str, Any]] = []
        self._done = False

    @workflow.run
    async def run(self, _arg: Any = None) -> int:
        await workflow.wait_condition(lambda: self._done)
        return len(self._signals)

    @workflow.signal(dynamic=True)
    async def any_signal(self, name: str, args: Sequence[RawValue]) -> None:
        values = [workflow.payload_converter().from_payload(a.payload) for a in args]
        self._signals.append({"name": name, "args": values})
        if name == "finish":
            self._done = True

    @workflow.query
    def signals(self) -> list[dict[str, Any]]:
        return self._signals


def pytest_collection_modifyitems(items):
    for item in items:
        if "/blackbox/" in str(item.fspath):
            item.add_marker(pytest.mark.blackbox)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def temporal_env():
    """A local Temporal dev server with standalone activities enabled and two extra namespaces."""
    # The SDK downloads into this directory but does not create it (CI's cache dir is absent on a cold cache).
    download_dir = os.environ.get("TEMPORAL_DEV_SERVER_DIR")
    if download_dir:
        os.makedirs(download_dir, exist_ok=True)
    env = await WorkflowEnvironment.start_local(
        download_dest_dir=download_dir,
        dev_server_extra_args=[
            "--namespace",
            OTHER_NAMESPACE,
            "--namespace",
            FORBIDDEN_NAMESPACE,
            "--dynamic-config-value",
            "activity.enableStandalone=true",
            "--dynamic-config-value",
            "history.enableChasm=true",
            # The server default allows one running batch job per namespace; tests run several.
            "--dynamic-config-value",
            "frontend.MaxConcurrentBatchOperationPerNamespace=100",
        ],
    )
    try:
        yield env
    finally:
        await env.shutdown()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def worker(temporal_env):
    """A real worker for the test workflow in each namespace the tests start workflows in."""
    base = temporal_env.client
    workers = []
    for namespace in (DEFAULT_NAMESPACE, OTHER_NAMESPACE):
        client = base if namespace == DEFAULT_NAMESPACE else type(base)(base.service_client, namespace=namespace)
        workers.append(Worker(client, task_queue=TASK_QUEUE, workflows=[BlackboxWaiter], workflow_runner=UnsandboxedWorkflowRunner()))
    tasks = [asyncio.create_task(w.run()) for w in workers]
    try:
        yield
    finally:
        for w in workers:
            await w.shutdown()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def mcp_client(temporal_env, worker):
    """An MCP client connected in-process to a TemporalMCPServer pointed at the dev server."""
    server = TemporalMCPServer(
        temporal_host=temporal_env.client.service_client.config.target_host,
        namespace=DEFAULT_NAMESPACE,
        tls_enabled=False,
        allowed_namespaces=[DEFAULT_NAMESPACE, OTHER_NAMESPACE],
    )
    # The MCP client's task group must be entered and exited in the same task, but
    # pytest-asyncio runs fixture setup and teardown in different tasks. Hold the
    # client open in a dedicated task instead.
    ready: asyncio.Future = asyncio.get_running_loop().create_future()
    stop = asyncio.Event()

    async def hold_open() -> None:
        try:
            async with Client(server.server) as client:
                ready.set_result(client)
                await stop.wait()
        except BaseException as e:
            if not ready.done():
                ready.set_exception(e)
            raise

    holder = asyncio.create_task(hold_open())
    try:
        yield await ready
    finally:
        stop.set()
        await asyncio.gather(holder, return_exceptions=True)
        await server.client_manager.disconnect()


@pytest.fixture
def call(mcp_client) -> CallFn:
    """call(name, args) -> (is_error, parsed JSON payload)."""

    async def _call(name: str, args: dict | None = None) -> tuple[bool, Any]:
        result = await mcp_client.call_tool(name, args or {})
        text = "".join(block.text for block in result.content if getattr(block, "type", None) == "text")
        try:
            payload = json.loads(text)
        except ValueError:
            payload = text
        return bool(result.is_error), payload

    return _call


async def wait_until(call: CallFn, name: str, args: dict, predicate: Callable[[bool, Any], bool], timeout: float = 30, interval: float = 0.25) -> tuple[bool, Any]:
    """Poll an MCP tool until predicate(is_error, payload) holds; fail with the last result on timeout."""
    deadline = time.monotonic() + timeout
    while True:
        is_error, payload = await call(name, args)
        if predicate(is_error, payload):
            return is_error, payload
        if time.monotonic() > deadline:
            raise AssertionError(f"Timed out after {timeout}s waiting on {name}({args}); last result: is_error={is_error} payload={payload}")
        await asyncio.sleep(interval)


def new_prefix() -> str:
    return f"t-{uuid.uuid4()}"


async def start_waiters(call: CallFn, prefix: str, count: int, namespace: str | None = None) -> list[str]:
    """Start `count` BlackboxWaiter workflows via MCP and wait until visibility lists them all."""
    ids = [f"{prefix}-{i}" for i in range(count)]
    extra = {"namespace": namespace} if namespace else {}
    for wid in ids:
        is_error, payload = await call("start_workflow", {"workflow_name": "BlackboxWaiter", "workflow_id": wid, "task_queue": TASK_QUEUE, **extra})
        assert not is_error, payload
    await wait_for_visible(call, prefix, count, namespace=namespace)
    return ids


async def wait_for_visible(call: CallFn, prefix: str, count: int, namespace: str | None = None, extra_query: str = "") -> None:
    extra = {"namespace": namespace} if namespace else {}
    query = f'WorkflowId STARTS_WITH "{prefix}"{extra_query}'
    await wait_until(call, "list_workflows", {"query": query, "limit": count + 10, **extra}, lambda e, p: not e and p["count"] == count)


async def wait_job_done(call: CallFn, job_id: str, namespace: str | None = None, timeout: float = 30) -> dict:
    extra = {"namespace": namespace} if namespace else {}
    _, payload = await wait_until(call, "describe_batch_job", {"job_id": job_id, **extra}, lambda e, p: not e and p.get("state") != "BATCH_OPERATION_STATE_RUNNING", timeout=timeout)
    return payload


@pytest.fixture
def helpers():
    """Expose module-level helpers to tests without import path gymnastics."""

    class _Helpers:
        wait_until = staticmethod(wait_until)
        new_prefix = staticmethod(new_prefix)
        start_waiters = staticmethod(start_waiters)
        wait_for_visible = staticmethod(wait_for_visible)
        wait_job_done = staticmethod(wait_job_done)

    return _Helpers
