"""Black-box tests for the server-side batch job tools.

Every test drives TemporalMCPServer through an MCP client against a real dev server and
asserts only on tool results, checking side effects through other MCP tools.
"""

import re
import uuid

import pytest

from .conftest import FORBIDDEN_NAMESPACE, IDLE_TASK_QUEUE, OTHER_NAMESPACE

pytestmark = [pytest.mark.blackbox, pytest.mark.asyncio(loop_scope="session")]

RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
BATCH_TOOLS = {"start_batch_job", "describe_batch_job", "list_batch_jobs", "stop_batch_job"}


def _q(prefix: str) -> str:
    return f'WorkflowId STARTS_WITH "{prefix}"'


async def _status(call, workflow_id: str, **extra) -> str:
    is_error, payload = await call("describe_workflow", {"workflow_id": workflow_id, **extra})
    assert not is_error, payload
    return payload["status"].removeprefix("WORKFLOW_EXECUTION_STATUS_")


async def _event_types(call, workflow_id: str, run_id: str | None = None) -> list[str]:
    args = {"workflow_id": workflow_id}
    if run_id:
        args["run_id"] = run_id
    is_error, payload = await call("get_workflow_history", args)
    assert not is_error, payload
    return [e["event_type_name"] for e in payload["events"]]


async def _all_job_ids(call, **extra) -> list[str]:
    ids, token = [], None
    while True:
        args = {"page_size": 100, **extra}
        if token:
            args["next_page_token"] = token
        is_error, payload = await call("list_batch_jobs", args)
        assert not is_error, payload
        ids.extend(j["job_id"] for j in payload["jobs"])
        token = payload["next_page_token"]
        if not token:
            return ids


async def _assert_rejected_without_job(call, args: dict, expect: str | None = None) -> dict:
    """The call must fail and must not create a job (checked by ID and by job count)."""
    job_id = f"mcp-bb-rejected-{uuid.uuid4()}"
    before = len(await _all_job_ids(call))
    is_error, payload = await call("start_batch_job", {"job_id": job_id, **args})
    assert is_error, payload
    if expect:
        assert expect in payload["error"], payload
    not_found, describe = await call("describe_batch_job", {"job_id": job_id})
    assert not_found and describe["type"] == "not_found", describe
    assert len(await _all_job_ids(call)) == before
    return payload


# --- Positive cases -----------------------------------------------------------


async def test_p1_tools_listed_with_schema(mcp_client):
    tools = {t.name: t for t in (await mcp_client.list_tools()).tools}
    assert BATCH_TOOLS <= tools.keys()
    schema = tools["start_batch_job"].input_schema
    assert schema["properties"]["operation"]["enum"] == ["terminate", "cancel", "signal", "delete", "reset", "cancel_activities", "terminate_activities"]
    assert {"operation", "reason"} <= set(schema["required"])
    assert set(tools["stop_batch_job"].input_schema["required"]) == {"job_id", "reason"}
    assert "server" in tools["start_batch_job"].description and "client-side" in tools["start_batch_job"].description


async def test_p2_terminate_by_query(call, helpers):
    prefix = helpers.new_prefix()
    ids = await helpers.start_waiters(call, prefix, 3)

    is_error, payload = await call("start_batch_job", {"operation": "terminate", "query": _q(prefix), "reason": "bb p2"})
    assert not is_error, payload
    job = await helpers.wait_job_done(call, payload["job_id"])
    assert job["state"] == "BATCH_OPERATION_STATE_COMPLETED"
    assert job["total_operation_count"] == 3 and job["failure_operation_count"] == 0
    for wid in ids:
        assert await _status(call, wid) == "TERMINATED"


async def test_p3_cancel_by_workflow_ids(call, helpers):
    prefix = helpers.new_prefix()
    ids = await helpers.start_waiters(call, prefix, 3)
    targets, untouched = ids[:2], ids[2]

    is_error, payload = await call("start_batch_job", {"operation": "cancel", "workflow_ids": targets, "reason": "bb p3"})
    assert not is_error, payload
    job = await helpers.wait_job_done(call, payload["job_id"])
    assert job["state"] == "BATCH_OPERATION_STATE_COMPLETED" and job["complete_operation_count"] == 2
    for wid in targets:
        assert "EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED" in await _event_types(call, wid)
    assert "EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED" not in await _event_types(call, untouched)
    assert await _status(call, untouched) == "RUNNING"


async def test_p4_signal_args_round_trip(call, helpers):
    prefix = helpers.new_prefix()
    ids = await helpers.start_waiters(call, prefix, 2)
    signal_args = {"note": "hello", "n": 7, "nested": {"ok": True}}

    is_error, payload = await call("start_batch_job", {"operation": "signal", "query": _q(prefix), "reason": "bb p4", "signal_name": "poke", "signal_args": signal_args})
    assert not is_error, payload
    job = await helpers.wait_job_done(call, payload["job_id"])
    assert job["state"] == "BATCH_OPERATION_STATE_COMPLETED" and job["complete_operation_count"] == 2
    for wid in ids:
        is_error, result = await helpers.wait_until(call, "query_workflow", {"workflow_id": wid, "query_name": "signals"}, lambda e, p: not e and p["query_result"])
        assert result["query_result"] == [{"name": "poke", "args": [signal_args]}]


async def test_p5_delete_closed_workflows(call, helpers):
    prefix = helpers.new_prefix()
    ids = await helpers.start_waiters(call, prefix, 2)
    for wid in ids:
        assert not (await call("terminate_workflow", {"workflow_id": wid, "reason": "close for p5"}))[0]
    await helpers.wait_for_visible(call, prefix, 2, extra_query=' AND ExecutionStatus = "Terminated"')

    is_error, payload = await call("start_batch_job", {"operation": "delete", "query": _q(prefix), "reason": "bb p5"})
    assert not is_error, payload
    job = await helpers.wait_job_done(call, payload["job_id"])
    assert job["state"] == "BATCH_OPERATION_STATE_COMPLETED" and job["complete_operation_count"] == 2, job
    for wid in ids:
        # The server deletes closed executions from a background task (~30s on the dev server).
        is_error, result = await helpers.wait_until(call, "describe_workflow", {"workflow_id": wid}, lambda e, p: e, timeout=90)
        assert result["type"] == "not_found", result


async def test_p6_reset_first_workflow_task(call, helpers):
    prefix = helpers.new_prefix()
    ids = await helpers.start_waiters(call, prefix, 2)
    original_runs = {}
    for wid in ids:
        # Make sure a workflow task has completed so there is a reset point.
        _, desc = await helpers.wait_until(
            call, "get_workflow_history", {"workflow_id": wid}, lambda e, p: not e and any(ev["event_type_name"] == "EVENT_TYPE_WORKFLOW_TASK_COMPLETED" for ev in p["events"])
        )
        original_runs[wid] = (await call("describe_workflow", {"workflow_id": wid}))[1]["run_id"]

    is_error, payload = await call("start_batch_job", {"operation": "reset", "query": _q(prefix), "reason": "bb p6", "reset_type": "first_workflow_task"})
    assert not is_error, payload
    job = await helpers.wait_job_done(call, payload["job_id"])
    assert job["state"] == "BATCH_OPERATION_STATE_COMPLETED" and job["failure_operation_count"] == 0, job
    for wid in ids:
        _, desc = await call("describe_workflow", {"workflow_id": wid})
        assert desc["run_id"] != original_runs[wid]
        assert (await _event_types(call, wid, run_id=original_runs[wid]))[-1] == "EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED"
        events = await _event_types(call, wid, run_id=desc["run_id"])
        assert "EVENT_TYPE_WORKFLOW_TASK_FAILED" in events  # the reset marker in the new run


@pytest.mark.parametrize("operation,expected", [("cancel_activities", "CANCELED"), ("terminate_activities", "TERMINATED")])
async def test_p7_activity_operations(call, helpers, operation, expected):
    prefix = helpers.new_prefix()
    ids = [f"{prefix}-{i}" for i in range(2)]
    for aid in ids:
        is_error, payload = await call("start_activity", {"activity": "NeverRuns", "activity_id": aid, "task_queue": IDLE_TASK_QUEUE, "schedule_to_close_timeout_seconds": 600})
        assert not is_error, payload
    query = f'ActivityId STARTS_WITH "{prefix}"'
    await helpers.wait_until(call, "count_activities", {"query": query}, lambda e, p: not e and p.get("count") == 2)

    is_error, payload = await call("start_batch_job", {"operation": operation, "query": query, "reason": f"bb p7 {operation}"})
    assert not is_error, payload
    job = await helpers.wait_job_done(call, payload["job_id"])
    assert job["state"] == "BATCH_OPERATION_STATE_COMPLETED" and job["complete_operation_count"] == 2, job
    for aid in ids:
        await helpers.wait_until(call, "describe_activity", {"activity_id": aid}, lambda e, p: not e and p["status"].endswith(expected))


async def test_p8_default_job_ids_are_unique(call, helpers):
    prefix = helpers.new_prefix()  # matches nothing; jobs are valid and finish immediately
    first = await call("start_batch_job", {"operation": "terminate", "query": _q(prefix), "reason": "bb p8"})
    second = await call("start_batch_job", {"operation": "terminate", "query": _q(prefix), "reason": "bb p8"})
    assert not first[0] and not second[0]
    assert first[1]["job_id"].startswith("mcp-batch-") and second[1]["job_id"].startswith("mcp-batch-")
    assert first[1]["job_id"] != second[1]["job_id"]


async def test_p9_custom_job_id_echoes_reason(call, helpers):
    job_id = f"mcp-bb-custom-{uuid.uuid4()}"
    is_error, payload = await call("start_batch_job", {"operation": "terminate", "query": _q(helpers.new_prefix()), "reason": "bb p9 custom", "job_id": job_id})
    assert not is_error and payload["job_id"] == job_id, payload
    is_error, desc = await call("describe_batch_job", {"job_id": job_id})
    assert not is_error, desc
    assert desc["job_id"] == job_id and desc["reason"] == "bb p9 custom"


async def test_p10_list_and_paginate(call, helpers):
    prefix = helpers.new_prefix()
    ids = await helpers.start_waiters(call, prefix, 3)
    _, by_query = await call("start_batch_job", {"operation": "terminate", "query": _q(prefix), "reason": "bb p10 query"})
    _, by_ids = await call("start_batch_job", {"operation": "cancel", "workflow_ids": ids[:1], "reason": "bb p10 ids"})
    mine = {by_query["job_id"], by_ids["job_id"]}
    for job_id in mine:
        await helpers.wait_job_done(call, job_id)

    seen, token, pages = [], None, 0
    while True:
        args = {"page_size": 1}
        if token:
            args["next_page_token"] = token
        is_error, page = await call("list_batch_jobs", args)
        assert not is_error, page
        assert page["count"] <= 1
        seen.extend(j["job_id"] for j in page["jobs"])
        pages += 1
        token = page["next_page_token"]
        if not token:
            break
    assert pages >= 2
    assert len(seen) == len(set(seen)), "a job repeated across pages"
    assert mine <= set(seen)


async def test_p11_stop_slow_job(call, helpers):
    prefix = helpers.new_prefix()
    await helpers.start_waiters(call, prefix, 12)
    is_error, payload = await call("start_batch_job", {"operation": "signal", "signal_name": "slow", "query": _q(prefix), "reason": "bb p11", "max_operations_per_second": 1})
    assert not is_error, payload
    job_id = payload["job_id"]

    is_error, stopped = await call("stop_batch_job", {"job_id": job_id, "reason": "bb p11 stop"})
    assert not is_error, stopped
    job = await helpers.wait_job_done(call, job_id)
    assert job["state"] == "BATCH_OPERATION_STATE_FAILED", job
    assert job["complete_operation_count"] < 12, job


async def test_p12_output_format(call, helpers):
    _, started = await call("start_batch_job", {"operation": "terminate", "query": _q(helpers.new_prefix()), "reason": "bb p12"})
    job = await helpers.wait_job_done(call, started["job_id"])
    assert RFC3339.match(job["start_time"]) and RFC3339.match(job["close_time"]), job
    assert job["state"] == "BATCH_OPERATION_STATE_COMPLETED"
    assert job["operation_type"].startswith("BATCH_OPERATION_TYPE_TERMINATE")
    assert all(isinstance(job[k], int) for k in ("total_operation_count", "complete_operation_count", "failure_operation_count"))

    is_error, listed = await call("list_batch_jobs", {"page_size": 50})
    assert not is_error and listed["jobs"], listed
    for entry in listed["jobs"]:
        assert RFC3339.match(entry["start_time"]), entry
        assert entry["state"].startswith("BATCH_OPERATION_STATE_") and entry["operation_type"].startswith("BATCH_OPERATION_TYPE_"), entry


async def test_p13_allowed_namespace(call, helpers):
    prefix = helpers.new_prefix()
    ids = await helpers.start_waiters(call, prefix, 2, namespace=OTHER_NAMESPACE)
    is_error, payload = await call("start_batch_job", {"operation": "terminate", "query": _q(prefix), "reason": "bb p13", "namespace": OTHER_NAMESPACE})
    assert not is_error and payload["namespace"] == OTHER_NAMESPACE, payload
    job = await helpers.wait_job_done(call, payload["job_id"], namespace=OTHER_NAMESPACE)
    assert job["total_operation_count"] == 2
    assert payload["job_id"] in await _all_job_ids(call, namespace=OTHER_NAMESPACE)
    assert payload["job_id"] not in await _all_job_ids(call)
    for wid in ids:
        assert await _status(call, wid, namespace=OTHER_NAMESPACE) == "TERMINATED"


# --- Negative cases -----------------------------------------------------------


async def test_n1_missing_filter(call):
    payload = await _assert_rejected_without_job(call, {"operation": "terminate", "reason": "bb n1"})
    assert "query" in payload["error"] and "workflow_ids" in payload["error"]


@pytest.mark.parametrize("query", ["", "   "])
async def test_n2_empty_query(call, query):
    await _assert_rejected_without_job(call, {"operation": "terminate", "query": query, "reason": "bb n2"}, expect="query")


async def test_n3_empty_workflow_ids(call):
    await _assert_rejected_without_job(call, {"operation": "terminate", "workflow_ids": [], "reason": "bb n3"}, expect="workflow_ids")


async def test_n4_both_filters(call, helpers):
    await _assert_rejected_without_job(call, {"operation": "terminate", "query": _q(helpers.new_prefix()), "workflow_ids": ["x"], "reason": "bb n4"}, expect="not both")


@pytest.mark.parametrize("reason", [None, "", "  "])
async def test_n5_missing_reason(call, helpers, reason):
    args = {"operation": "terminate", "query": _q(helpers.new_prefix())}
    if reason is not None:
        args["reason"] = reason
    await _assert_rejected_without_job(call, args, expect="reason")


async def test_n6_unknown_operation(call, helpers):
    await _assert_rejected_without_job(call, {"operation": "explode", "query": _q(helpers.new_prefix()), "reason": "bb n6"}, expect="Unknown operation")


async def test_n7_signal_without_name(call, helpers):
    await _assert_rejected_without_job(call, {"operation": "signal", "query": _q(helpers.new_prefix()), "reason": "bb n7"}, expect="signal_name")


@pytest.mark.parametrize("reset_type", [None, "event_id", 3])
async def test_n8_reset_type(call, helpers, reset_type):
    args = {"operation": "reset", "query": _q(helpers.new_prefix()), "reason": "bb n8"}
    if reset_type is not None:
        args["reset_type"] = reset_type
    await _assert_rejected_without_job(call, args, expect="reset_type")


@pytest.mark.parametrize(
    "operation,extra",
    [
        ("terminate", {"signal_name": "poke"}),
        ("cancel", {"signal_args": {"a": 1}}),
        ("signal", {"signal_name": "poke", "reset_type": "first_workflow_task"}),
        ("delete", {"reset_type": "last_workflow_task"}),
    ],
)
async def test_n9_arg_for_wrong_operation(call, helpers, operation, extra):
    await _assert_rejected_without_job(call, {"operation": operation, "query": _q(helpers.new_prefix()), "reason": "bb n9", **extra}, expect="applies only to")


async def test_n9b_workflow_ids_on_activity_operation(call):
    await _assert_rejected_without_job(call, {"operation": "cancel_activities", "workflow_ids": ["x"], "reason": "bb n9b"}, expect="only 'query'")


async def test_n10_malformed_query(call):
    is_error, payload = await call("start_batch_job", {"operation": "terminate", "query": "WorkflowId ===== AND (", "reason": "bb n10"})
    assert is_error, payload
    assert payload["type"] == "rpc_error" and "query" in payload["error"].lower(), payload


async def test_n11_reused_job_id(call, helpers):
    prefix = helpers.new_prefix()
    await helpers.start_waiters(call, prefix, 3)
    job_id = f"mcp-bb-dup-{uuid.uuid4()}"
    args = {"operation": "signal", "signal_name": "slow", "query": _q(prefix), "reason": "bb n11", "job_id": job_id, "max_operations_per_second": 1}
    assert not (await call("start_batch_job", args))[0]
    is_error, payload = await call("start_batch_job", args)
    assert is_error and payload["type"] == "already_exists", payload
    await call("stop_batch_job", {"job_id": job_id, "reason": "cleanup"})


@pytest.mark.parametrize("tool,extra", [("describe_batch_job", {}), ("stop_batch_job", {"reason": "bb n12"})])
async def test_n12_unknown_job(call, tool, extra):
    is_error, payload = await call(tool, {"job_id": f"mcp-bb-missing-{uuid.uuid4()}", **extra})
    assert is_error and payload["type"] == "not_found", payload


async def test_n13_namespace_outside_allowlist(call, helpers):
    job_id = f"mcp-bb-forbidden-{uuid.uuid4()}"
    default_before = len(await _all_job_ids(call))
    other_before = len(await _all_job_ids(call, namespace=OTHER_NAMESPACE))
    is_error, payload = await call("start_batch_job", {"operation": "terminate", "query": _q(helpers.new_prefix()), "reason": "bb n13", "job_id": job_id, "namespace": FORBIDDEN_NAMESPACE})
    assert is_error, payload
    assert FORBIDDEN_NAMESPACE in payload["error"], payload
    assert len(await _all_job_ids(call)) == default_before
    assert len(await _all_job_ids(call, namespace=OTHER_NAMESPACE)) == other_before
    # The forbidden namespace can't be inspected through MCP either: the allowlist blocks reads too.
    is_error, _ = await call("list_batch_jobs", {"namespace": FORBIDDEN_NAMESPACE})
    assert is_error


async def test_n14_query_matching_nothing(call, helpers):
    is_error, payload = await call("start_batch_job", {"operation": "terminate", "query": _q(helpers.new_prefix()), "reason": "bb n14"})
    assert not is_error, payload
    job = await helpers.wait_job_done(call, payload["job_id"])
    assert job["state"] == "BATCH_OPERATION_STATE_COMPLETED" and job["total_operation_count"] == 0, job


@pytest.mark.parametrize("value", [0, -1, "fast", True, None])
async def test_n15_bad_rate(call, helpers, value):
    await _assert_rejected_without_job(call, {"operation": "terminate", "query": _q(helpers.new_prefix()), "reason": "bb n15", "max_operations_per_second": value}, expect="max_operations_per_second")
