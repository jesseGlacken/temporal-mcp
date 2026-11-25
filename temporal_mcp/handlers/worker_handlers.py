"""Read-only handlers for workers, task queues and worker deployments.

These tools give an agent the same view as the Temporal Web UI Workers page. The
``describe_task_queue`` tool works on every server and is the main way to tell why a
workflow or activity is not progressing (for example, no worker polls its queue).

``list_workers``, ``count_workers`` and ``describe_worker`` depend on worker
heartbeating, which needs a recent server with the feature enabled. When the
namespace does not advertise the ``worker_heartbeats`` capability, or the server
rejects the RPC as unimplemented, these tools return a clear error that points to
``describe_task_queue`` instead of a raw RPC failure.
"""

import base64
import binascii
import json
from typing import Any

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
from mcp.types import CallToolResult, TextContent
from temporalio.api.deployment.v1 import WorkerDeploymentVersion
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    CountWorkersRequest,
    DescribeNamespaceRequest,
    DescribeTaskQueueRequest,
    DescribeWorkerDeploymentRequest,
    DescribeWorkerDeploymentVersionRequest,
    DescribeWorkerRequest,
    ListWorkerDeploymentsRequest,
    ListWorkersRequest,
)
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

TASK_QUEUE_TYPES = {
    "workflow": TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
    "activity": TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY,
    "nexus": TaskQueueType.TASK_QUEUE_TYPE_NEXUS,
}
TASK_QUEUE_TYPE_CHOICES = tuple(TASK_QUEUE_TYPES) + ("all",)

HEARTBEAT_UNSUPPORTED = (
    "Worker listing is not supported or not enabled on this server/namespace (it needs worker heartbeating: "
    "a recent Temporal server with frontend.WorkerHeartbeatsEnabled and frontend.ListWorkersEnabled, and SDK workers that send heartbeats). "
    "Use describe_task_queue to see which workers poll a task queue."
)
NO_POLLER_WARNING = "No worker has polled this task queue recently. Workflows and activities scheduled on it will not make progress " "until a worker for this task queue (and namespace) is running."

# Summary fields kept from a worker heartbeat in list_workers; describe_worker returns everything.
_WORKER_SUMMARY_FIELDS = (
    "worker_instance_key",
    "worker_identity",
    "task_queue",
    "sdk_name",
    "sdk_version",
    "deployment_version",
    "status",
    "start_time",
    "heartbeat_time",
    "elapsed_since_last_heartbeat",
)


class _InvalidArgument(ValueError):
    """A caller-supplied argument failed validation."""


def _error(tool: str, message: str, error_type: str = "invalid_argument") -> CallToolResult:
    payload = {"error": message, "type": error_type, "tool": tool}
    return CallToolResult(content=[TextContent(type="text", text=json.dumps(payload, indent=2))], is_error=True)


def _ok(payload: dict[str, Any]) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(payload, indent=2))]


def _to_dict(message: Message) -> dict[str, Any]:
    """Render a protobuf as JSON-ready data with RFC 3339 times and enum names."""
    return MessageToDict(message, preserving_proto_field_name=True)


def _require_text(args: dict, name: str) -> str:
    value = args.get(name)
    if value is None:
        raise _InvalidArgument(f"Missing required parameter: '{name}'")
    if not isinstance(value, str) or not value.strip():
        raise _InvalidArgument(f"'{name}' must be a non-empty string")
    return value


def _optional_bool(args: dict, name: str, default: bool) -> bool:
    if name not in args:
        return default
    value = args[name]
    if not isinstance(value, bool):
        raise _InvalidArgument(f"'{name}' must be a boolean")
    return value


def _optional_query(args: dict) -> str | None:
    query = args.get("query")
    if query is None:
        return None
    if not isinstance(query, str):
        raise _InvalidArgument("'query' must be a string")
    return query


def _apply_paging(args: dict, request: Message, tool: str) -> None:
    """Validate page_size / next_page_token and copy them onto a list request."""
    if "page_size" in args:
        page_size = args["page_size"]
        if isinstance(page_size, bool) or not isinstance(page_size, (int, float)) or page_size != int(page_size) or page_size <= 0:
            raise _InvalidArgument("'page_size' must be a positive integer")
        request.page_size = int(page_size)
    token = args.get("next_page_token")
    if token:
        if not isinstance(token, str):
            raise _InvalidArgument(f"'next_page_token' must be the string returned by a previous {tool} call")
        try:
            request.next_page_token = base64.b64decode(token, validate=True)
        except (binascii.Error, ValueError):
            raise _InvalidArgument("'next_page_token' is not a valid page token")


def _encode_token(token: bytes) -> str | None:
    return base64.b64encode(token).decode() if token else None


async def _heartbeat_error(client: Client, tool: str) -> CallToolResult | None:
    """Return an error result when the namespace does not support worker heartbeats."""
    response = await client.workflow_service.describe_namespace(DescribeNamespaceRequest(namespace=client.namespace))
    if not response.namespace_info.capabilities.worker_heartbeats:
        return _error(tool, HEARTBEAT_UNSUPPORTED, "not_supported")
    return None


def _unsupported_rpc(e: RPCError, tool: str) -> CallToolResult | None:
    if e.status in (RPCStatusCode.UNIMPLEMENTED, RPCStatusCode.FAILED_PRECONDITION):
        return _error(tool, f"{HEARTBEAT_UNSUPPORTED} Server said: {e.message}", "not_supported")
    return None


def _rpc_error(e: RPCError, tool: str) -> CallToolResult:
    error_type = "not_found" if e.status == RPCStatusCode.NOT_FOUND else "invalid_argument" if e.status == RPCStatusCode.INVALID_ARGUMENT else "rpc_error"
    return _error(tool, e.message or str(e), error_type)


# --- worker heartbeat tools ---------------------------------------------------


def _worker_summary(heartbeat: Message) -> dict[str, Any]:
    data = _to_dict(heartbeat)
    summary = {key: data[key] for key in _WORKER_SUMMARY_FIELDS if key in data}
    host = data.get("host_info", {})
    for key in ("host_name", "process_id"):
        if key in host:
            summary[key] = host[key]
    return summary


async def list_workers(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """List workers that heartbeat to the namespace, one page at a time."""
    tool = "list_workers"
    request = ListWorkersRequest(namespace=client.namespace)
    try:
        _apply_paging(args, request, tool)
        query = _optional_query(args)
        if query:
            request.query = query
        request.include_system_workers = _optional_bool(args, "include_system_workers", False)
    except _InvalidArgument as e:
        return _error(tool, str(e))

    if unsupported := await _heartbeat_error(client, tool):
        return unsupported
    try:
        response = await client.workflow_service.list_workers(request)
    except RPCError as e:
        return _unsupported_rpc(e, tool) or _rpc_error(e, tool)

    workers = [_worker_summary(info.worker_heartbeat) for info in response.workers_info]
    return _ok({"workers": workers, "count": len(workers), "next_page_token": _encode_token(response.next_page_token)})


async def count_workers(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """Count workers that heartbeat to the namespace, optionally filtered by a query."""
    tool = "count_workers"
    request = CountWorkersRequest(namespace=client.namespace)
    try:
        query = _optional_query(args)
        if query:
            request.query = query
        request.include_system_workers = _optional_bool(args, "include_system_workers", False)
    except _InvalidArgument as e:
        return _error(tool, str(e))

    if unsupported := await _heartbeat_error(client, tool):
        return unsupported
    try:
        response = await client.workflow_service.count_workers(request)
    except RPCError as e:
        return _unsupported_rpc(e, tool) or _rpc_error(e, tool)

    result: dict[str, Any] = {"count": int(response.count)}
    if query:
        result["query"] = query
    return _ok(result)


async def describe_worker(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """Return the latest heartbeat of one worker: host, slots, pollers, deployment version."""
    tool = "describe_worker"
    try:
        key = _require_text(args, "worker_instance_key")
    except _InvalidArgument as e:
        return _error(tool, str(e))

    if unsupported := await _heartbeat_error(client, tool):
        return unsupported
    try:
        response = await client.workflow_service.describe_worker(DescribeWorkerRequest(namespace=client.namespace, worker_instance_key=key))
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            # The server's message can name an internal namespace ID; give a stable one instead.
            return _error(tool, f"Worker '{key}' not found. It may have stopped heartbeating; list current workers with list_workers.", "not_found")
        return _unsupported_rpc(e, tool) or _rpc_error(e, tool)

    return _ok(_to_dict(response.worker_info.worker_heartbeat))


# --- task queues ---------------------------------------------------------------


def _task_queue_section(response: Message, report_stats: bool) -> dict[str, Any]:
    pollers = [_to_dict(p) for p in response.pollers]
    section: dict[str, Any] = {"pollers": pollers, "poller_count": len(pollers)}
    if report_stats:
        stats = response.stats
        # MessageToDict renders int64 as a string and drops zero values; make the backlog explicit.
        section["stats"] = {
            "approximate_backlog_count": int(stats.approximate_backlog_count),
            "approximate_backlog_age": MessageToDict(stats.approximate_backlog_age) if stats.HasField("approximate_backlog_age") else "0s",
            "tasks_add_rate": stats.tasks_add_rate,
            "tasks_dispatch_rate": stats.tasks_dispatch_rate,
        }
    if response.HasField("versioning_info"):
        section["versioning_info"] = _to_dict(response.versioning_info)
    if response.HasField("effective_rate_limit"):
        section["effective_rate_limit"] = _to_dict(response.effective_rate_limit)
    return section


async def describe_task_queue(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """Describe a task queue: pollers (workers) per type plus backlog and rates."""
    tool = "describe_task_queue"
    try:
        task_queue = _require_text(args, "task_queue")
        queue_type = args.get("task_queue_type", "all")
        if queue_type not in TASK_QUEUE_TYPE_CHOICES:
            raise _InvalidArgument(f"Invalid task_queue_type {queue_type!r}; expected one of {', '.join(TASK_QUEUE_TYPE_CHOICES)}")
        report_stats = _optional_bool(args, "report_stats", True)
    except _InvalidArgument as e:
        return _error(tool, str(e))

    types = list(TASK_QUEUE_TYPES) if queue_type == "all" else [queue_type]
    sections: dict[str, Any] = {}
    for name in types:
        request = DescribeTaskQueueRequest(
            namespace=client.namespace,
            task_queue=TaskQueue(name=task_queue),
            task_queue_type=TASK_QUEUE_TYPES[name],
            report_stats=report_stats,
        )
        try:
            response = await client.workflow_service.describe_task_queue(request)
        except RPCError as e:
            if queue_type == "all" and name == "nexus" and e.status in (RPCStatusCode.UNIMPLEMENTED, RPCStatusCode.INVALID_ARGUMENT):
                continue  # Older servers have no nexus task queues; the other types still answer.
            return _rpc_error(e, tool)
        sections[name] = _task_queue_section(response, report_stats)

    has_pollers = any(section["poller_count"] for section in sections.values())
    result: dict[str, Any] = {"task_queue": task_queue, "namespace": client.namespace, "has_pollers": has_pollers}
    if not has_pollers:
        result["warning"] = NO_POLLER_WARNING
    result["types"] = sections
    return _ok(result)


# --- worker deployments --------------------------------------------------------


async def list_worker_deployments(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """List worker deployments in the namespace with their routing summary."""
    tool = "list_worker_deployments"
    request = ListWorkerDeploymentsRequest(namespace=client.namespace)
    try:
        _apply_paging(args, request, tool)
    except _InvalidArgument as e:
        return _error(tool, str(e))
    try:
        response = await client.workflow_service.list_worker_deployments(request)
    except RPCError as e:
        if request.next_page_token and e.status in (RPCStatusCode.UNKNOWN, RPCStatusCode.INVALID_ARGUMENT, RPCStatusCode.INTERNAL):
            return _error(tool, f"The server rejected 'next_page_token' ({e.message}); pass a token from a previous list_worker_deployments call")
        return _rpc_error(e, tool)

    deployments = [_to_dict(d) for d in response.worker_deployments]
    return _ok({"deployments": deployments, "count": len(deployments), "next_page_token": _encode_token(response.next_page_token)})


async def describe_worker_deployment(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """Describe a worker deployment: its versions and routing config (current and ramping)."""
    tool = "describe_worker_deployment"
    try:
        name = _require_text(args, "deployment_name")
    except _InvalidArgument as e:
        return _error(tool, str(e))
    try:
        response = await client.workflow_service.describe_worker_deployment(DescribeWorkerDeploymentRequest(namespace=client.namespace, deployment_name=name))
    except RPCError as e:
        return _rpc_error(e, tool)
    # conflict_token only matters to mutating calls, which this server does not offer.
    return _ok(_to_dict(response.worker_deployment_info))


async def describe_worker_deployment_version(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """Describe one deployment version: status, drainage and task queues (with stats if asked)."""
    tool = "describe_worker_deployment_version"
    try:
        name = _require_text(args, "deployment_name")
        build_id = _require_text(args, "build_id")
        report_stats = _optional_bool(args, "report_task_queue_stats", False)
    except _InvalidArgument as e:
        return _error(tool, str(e))

    request = DescribeWorkerDeploymentVersionRequest(
        namespace=client.namespace,
        deployment_version=WorkerDeploymentVersion(deployment_name=name, build_id=build_id),
        report_task_queue_stats=report_stats,
    )
    try:
        response = await client.workflow_service.describe_worker_deployment_version(request)
    except RPCError as e:
        return _rpc_error(e, tool)

    result = _to_dict(response.worker_deployment_version_info)
    if report_stats:
        result["task_queues"] = [_to_dict(tq) for tq in response.version_task_queues]
    return _ok(result)
