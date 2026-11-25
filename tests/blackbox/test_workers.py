"""Black-box tests for the worker, task queue and worker deployment tools.

Every test drives TemporalMCPServer through an MCP client against a real dev server with
real SDK workers, and asserts only on tool results. Poller and heartbeat data lag behind
the workers, so assertions that depend on them poll through wait_until.
"""

import re

import pytest

from .conftest import FORBIDDEN_NAMESPACE, OTHER_NAMESPACE

pytestmark = [pytest.mark.blackbox, pytest.mark.asyncio(loop_scope="session")]

RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
WORKER_TOOLS = {
    "list_workers",
    "count_workers",
    "describe_worker",
    "describe_task_queue",
    "list_worker_deployments",
    "describe_worker_deployment",
    "describe_worker_deployment_version",
}
REQUIRED = {
    "describe_task_queue": {"task_queue"},
    "describe_worker": {"worker_instance_key"},
    "describe_worker_deployment": {"deployment_name"},
    "describe_worker_deployment_version": {"deployment_name", "build_id"},
}
NO_POLLER_WARNING = "No worker has polled this task queue recently"


def _has_pollers(expected: bool):
    return lambda e, p: not e and p["has_pollers"] is expected


def _tq_filter(*queues: str) -> str:
    return " OR ".join(f'TaskQueue = "{q}"' for q in queues)


async def _wait_listed(call, helpers, query: str, count: int, **extra) -> list[dict]:
    _, payload = await helpers.wait_until(call, "list_workers", {"query": query, **extra}, lambda e, p: not e and p["count"] == count)
    return payload["workers"]


async def _all_deployment_names(call, **extra) -> list[str]:
    names, token = [], None
    while True:
        args = {"page_size": 100, **extra}
        if token:
            args["next_page_token"] = token
        is_error, payload = await call("list_worker_deployments", args)
        assert not is_error, payload
        names.extend(d["name"] for d in payload["deployments"])
        token = payload["next_page_token"]
        if not token:
            return names


# --- Positive cases -----------------------------------------------------------


async def test_p1_tools_listed_with_schema(mcp_client):
    tools = {t.name: t for t in (await mcp_client.list_tools()).tools}
    assert WORKER_TOOLS <= tools.keys()
    for name in WORKER_TOOLS:
        assert set(tools[name].input_schema.get("required", [])) == REQUIRED.get(name, set()), name
    assert tools["describe_task_queue"].input_schema["properties"]["task_queue_type"]["enum"] == ["workflow", "activity", "nexus", "all"]
    for name in ("list_workers", "count_workers", "describe_worker"):
        assert "describe_task_queue" in tools[name].description


async def test_p2_p3_pollers_appear_then_age_out(call, helpers, start_worker):
    tq = helpers.new_prefix()
    stop = await start_worker(tq, identity=f"{tq}-identity")

    _, payload = await helpers.wait_until(call, "describe_task_queue", {"task_queue": tq}, lambda e, p: not e and p["types"]["workflow"]["poller_count"] > 0)
    assert payload["has_pollers"] is True
    assert "warning" not in payload
    pollers = payload["types"]["workflow"]["pollers"]
    assert f"{tq}-identity" in [p["identity"] for p in pollers]
    assert all(RFC3339.match(p["last_access_time"]) for p in pollers), pollers

    await stop()
    _, payload = await helpers.wait_until(call, "describe_task_queue", {"task_queue": tq}, _has_pollers(False))
    assert NO_POLLER_WARNING in payload["warning"]


async def test_p4_backlog_without_worker(call, helpers):
    tq = helpers.new_prefix()
    ids = [f"{tq}-{i}" for i in range(2)]
    for wid in ids:
        is_error, payload = await call("start_workflow", {"workflow_name": "BlackboxWaiter", "workflow_id": wid, "task_queue": tq})
        assert not is_error, payload
    try:
        _, payload = await helpers.wait_until(
            call,
            "describe_task_queue",
            {"task_queue": tq, "task_queue_type": "workflow"},
            lambda e, p: not e and p["types"]["workflow"]["stats"]["approximate_backlog_count"] >= 2,
        )
        assert payload["has_pollers"] is False
        assert NO_POLLER_WARNING in payload["warning"]
    finally:
        for wid in ids:
            await call("terminate_workflow", {"workflow_id": wid, "reason": "test cleanup"})


async def test_p5_task_queue_type_selection(call, helpers, start_worker):
    tq = helpers.new_prefix()
    await start_worker(tq)
    await helpers.wait_until(call, "describe_task_queue", {"task_queue": tq, "task_queue_type": "activity"}, _has_pollers(True))

    for queue_type in ("workflow", "activity", "nexus"):
        is_error, payload = await call("describe_task_queue", {"task_queue": tq, "task_queue_type": queue_type})
        assert not is_error, payload
        assert set(payload["types"]) == {queue_type}

    is_error, payload = await call("describe_task_queue", {"task_queue": tq, "task_queue_type": "all"})
    assert not is_error, payload
    assert {"workflow", "activity"} <= set(payload["types"])
    assert payload["types"]["workflow"]["poller_count"] >= 1 and payload["types"]["activity"]["poller_count"] >= 1
    assert "stats" in payload["types"]["workflow"]


async def test_p6_report_stats_false(call, helpers, start_worker):
    tq = helpers.new_prefix()
    await start_worker(tq)
    _, payload = await helpers.wait_until(call, "describe_task_queue", {"task_queue": tq, "report_stats": False}, _has_pollers(True))
    for section in payload["types"].values():
        assert "stats" not in section
        assert "pollers" in section


async def test_p7_to_p11_list_count_paginate_describe(call, helpers, start_worker):
    tq_a, tq_b = helpers.new_prefix(), helpers.new_prefix()
    await start_worker(tq_a)
    await start_worker(tq_b)
    query = _tq_filter(tq_a, tq_b)

    # P7: both workers, with the summary fields.
    workers = await _wait_listed(call, helpers, query, 2)
    assert {w["task_queue"] for w in workers} == {tq_a, tq_b}
    assert len({w["worker_instance_key"] for w in workers}) == 2
    for w in workers:
        assert w["worker_identity"] == f"{w['task_queue']}-worker"
        assert w["sdk_name"] and w["sdk_version"]
        assert RFC3339.match(w["heartbeat_time"]), w

    # P8: query scoped to one task queue.
    only_a = await _wait_listed(call, helpers, _tq_filter(tq_a), 1)
    assert only_a[0]["task_queue"] == tq_a

    # P9: page_size 1 walks both workers once each; the last page has no token.
    is_error, page1 = await call("list_workers", {"query": query, "page_size": 1})
    assert not is_error, page1
    assert page1["count"] == 1 and page1["next_page_token"]
    is_error, page2 = await call("list_workers", {"query": query, "page_size": 1, "next_page_token": page1["next_page_token"]})
    assert not is_error, page2
    keys = [w["worker_instance_key"] for w in page1["workers"] + page2["workers"]]
    assert sorted(keys) == sorted(w["worker_instance_key"] for w in workers)
    if page2["next_page_token"]:
        # A server may return one extra, empty page before it knows the list ended.
        is_error, page3 = await call("list_workers", {"query": query, "page_size": 1, "next_page_token": page2["next_page_token"]})
        assert not is_error and page3["workers"] == [] and page3["next_page_token"] is None, page3

    # P10: counts agree with listing, with and without a query.
    is_error, counted = await call("count_workers", {"query": query})
    assert not is_error and counted["count"] == 2, counted

    async def list_all() -> int:
        total, token = 0, None
        while True:
            args = {"page_size": 100, **({"next_page_token": token} if token else {})}
            is_error, page = await call("list_workers", args)
            assert not is_error, page
            total += page["count"]
            token = page["next_page_token"]
            if not token:
                return total

    # Other tests' workers can come and go between the two calls; retry until a stable read agrees.
    for _ in range(20):
        listed = await list_all()
        is_error, counted = await call("count_workers")
        assert not is_error, counted
        if counted["count"] == listed:
            break
    else:
        raise AssertionError(f"count_workers {counted['count']} never matched list_workers {listed}")
    assert counted["count"] >= 2

    # P11: describe_worker agrees with the list entry.
    entry = only_a[0]
    is_error, described = await call("describe_worker", {"worker_instance_key": entry["worker_instance_key"]})
    assert not is_error, described
    assert described["worker_identity"] == entry["worker_identity"]
    assert described["task_queue"] == tq_a
    assert "host_info" in described


async def test_p12_to_p14_worker_deployments(call, helpers, start_worker):
    tq, name, build_id = helpers.new_prefix(), helpers.new_prefix(), helpers.new_prefix()
    await start_worker(tq, deployment=(name, build_id))

    # P12
    await helpers.wait_until(call, "list_worker_deployments", {"page_size": 100}, lambda e, p: not e and name in [d["name"] for d in p["deployments"]])
    is_error, payload = await call("list_worker_deployments", {"page_size": 100})
    assert not is_error, payload
    entry = next(d for d in payload["deployments"] if d["name"] == name)
    assert RFC3339.match(entry["create_time"]), entry
    assert name in await _all_deployment_names(call)

    # P13
    is_error, deployment = await call("describe_worker_deployment", {"deployment_name": name})
    assert not is_error, deployment
    assert build_id in [v["deployment_version"]["build_id"] for v in deployment["version_summaries"]]
    assert "routing_config" in deployment

    # P14
    _, version = await helpers.wait_until(
        call,
        "describe_worker_deployment_version",
        {"deployment_name": name, "build_id": build_id, "report_task_queue_stats": True},
        lambda e, p: not e and tq in [t["name"] for t in p.get("task_queues", [])],
    )
    assert version["status"].startswith("WORKER_DEPLOYMENT_VERSION_STATUS_")
    queue = next(t for t in version["task_queues"] if t["name"] == tq)
    assert "stats" in queue


async def test_p15_namespace_scoping(call, helpers, start_worker):
    tq = helpers.new_prefix()
    await start_worker(tq, namespace=OTHER_NAMESPACE)

    await _wait_listed(call, helpers, _tq_filter(tq), 1, namespace=OTHER_NAMESPACE)
    await helpers.wait_until(call, "describe_task_queue", {"task_queue": tq, "namespace": OTHER_NAMESPACE}, _has_pollers(True))

    is_error, payload = await call("list_workers", {"query": _tq_filter(tq)})
    assert not is_error and payload["workers"] == [], payload
    is_error, payload = await call("describe_task_queue", {"task_queue": tq})
    assert not is_error and payload["has_pollers"] is False and payload["namespace"] == "default", payload


# --- Negative cases -----------------------------------------------------------


@pytest.mark.parametrize(
    "tool,args,missing",
    [
        ("describe_task_queue", {}, "task_queue"),
        ("describe_worker", {}, "worker_instance_key"),
        ("describe_worker_deployment", {}, "deployment_name"),
        ("describe_worker_deployment_version", {"build_id": "b"}, "deployment_name"),
        ("describe_worker_deployment_version", {"deployment_name": "d"}, "build_id"),
    ],
)
async def test_n1_missing_required_argument(call, tool, args, missing):
    is_error, payload = await call(tool, args)
    assert is_error, payload
    assert f"'{missing}'" in payload["error"]


async def test_n2_invalid_task_queue_type(call):
    is_error, payload = await call("describe_task_queue", {"task_queue": "x", "task_queue_type": "sticky"})
    assert is_error and "task_queue_type" in payload["error"], payload


async def test_n3_never_used_queue(call, helpers):
    # Observed on Server 1.32: a never-used queue describes successfully with no pollers and zero backlog.
    is_error, payload = await call("describe_task_queue", {"task_queue": helpers.new_prefix()})
    assert not is_error, payload
    assert payload["has_pollers"] is False
    assert NO_POLLER_WARNING in payload["warning"]
    for section in payload["types"].values():
        assert section["pollers"] == []
        assert section["stats"]["approximate_backlog_count"] == 0


async def test_n4_describe_unknown_worker(call, helpers):
    is_error, payload = await call("describe_worker", {"worker_instance_key": helpers.new_prefix()})
    assert is_error and payload["type"] == "not_found", payload


async def test_n5_describe_unknown_deployment(call, helpers):
    is_error, payload = await call("describe_worker_deployment", {"deployment_name": helpers.new_prefix()})
    assert is_error and payload["type"] == "not_found", payload


async def test_n6_describe_unknown_version(call, helpers, start_worker):
    tq, name = helpers.new_prefix(), helpers.new_prefix()
    await start_worker(tq, deployment=(name, "real-build"))
    await helpers.wait_until(call, "describe_worker_deployment", {"deployment_name": name}, lambda e, p: not e)
    is_error, payload = await call("describe_worker_deployment_version", {"deployment_name": name, "build_id": "no-such-build"})
    assert is_error and payload["type"] == "not_found", payload


@pytest.mark.parametrize("tool", ["list_workers", "count_workers"])
async def test_n7_malformed_query(call, tool):
    is_error, payload = await call(tool, {"query": "TaskQueue ==== "})
    assert is_error, payload
    assert "malformed query" in payload["error"]


@pytest.mark.parametrize("tool", ["list_workers", "list_worker_deployments"])
async def test_n8_invalid_page_token(call, tool):
    is_error, payload = await call(tool, {"next_page_token": "not base64!!"})
    assert is_error and "next_page_token" in payload["error"], payload


@pytest.mark.parametrize("tool", ["list_workers", "list_worker_deployments"])
async def test_n8_tampered_page_token(call, tool):
    # Valid base64 that the server did not issue.
    is_error, payload = await call(tool, {"next_page_token": "Z2FyYmFnZQ=="})
    assert is_error and "next_page_token" in payload["error"], payload


@pytest.mark.parametrize(
    "tool,args",
    [("list_workers", {}), ("count_workers", {}), ("describe_worker", {"worker_instance_key": "any"})],
)
async def test_n9_heartbeats_disabled(call_heartbeats_off, tool, args):
    is_error, payload = await call_heartbeats_off(tool, args)
    assert is_error, payload
    assert payload["type"] == "not_supported"
    assert "describe_task_queue" in payload["error"]


async def test_n9_describe_task_queue_still_works_without_heartbeats(call_heartbeats_off):
    is_error, payload = await call_heartbeats_off("describe_task_queue", {"task_queue": "anything"})
    assert not is_error and payload["has_pollers"] is False, payload


async def test_n10_no_deployments(call_heartbeats_off):
    is_error, payload = await call_heartbeats_off("list_worker_deployments", {"namespace": OTHER_NAMESPACE})
    assert not is_error, payload
    assert payload["deployments"] == [] and payload["count"] == 0 and payload["next_page_token"] is None


@pytest.mark.parametrize(
    "tool,args",
    [
        ("list_workers", {}),
        ("count_workers", {}),
        ("describe_worker", {"worker_instance_key": "k"}),
        ("describe_task_queue", {"task_queue": "q"}),
        ("list_worker_deployments", {}),
        ("describe_worker_deployment", {"deployment_name": "d"}),
        ("describe_worker_deployment_version", {"deployment_name": "d", "build_id": "b"}),
    ],
)
async def test_n11_namespace_outside_allowlist(call, tool, args):
    is_error, payload = await call(tool, {**args, "namespace": FORBIDDEN_NAMESPACE})
    assert is_error and "not allowed" in payload["error"], payload


@pytest.mark.parametrize("tool", ["list_workers", "list_worker_deployments"])
@pytest.mark.parametrize("page_size", [0, -1, "ten", 1.5])
async def test_n12_bad_page_size(call, tool, page_size):
    is_error, payload = await call(tool, {"page_size": page_size})
    assert is_error and "page_size" in payload["error"], payload
