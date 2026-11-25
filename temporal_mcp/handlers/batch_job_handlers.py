"""Handlers for server-side batch operations (Temporal batch jobs).

Unlike the client-side loops in ``batch_handlers``, these tools ask the Temporal
server to run a batch job. The job runs in the server's batcher, returns
immediately with a ``job_id``, and is visible on the Temporal Web UI Batch page.
"""

import base64
import binascii
import json
import math
import uuid
from typing import Any, Callable

from google.protobuf.empty_pb2 import Empty
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
from mcp.types import CallToolResult, TextContent
from temporalio.api.batch.v1 import (
    BatchOperationCancelActivities,
    BatchOperationCancellation,
    BatchOperationDeletion,
    BatchOperationReset,
    BatchOperationSignal,
    BatchOperationTermination,
    BatchOperationTerminateActivities,
)
from temporalio.api.common.v1 import ResetOptions, WorkflowExecution
from temporalio.api.workflowservice.v1 import (
    DescribeBatchOperationRequest,
    ListBatchOperationsRequest,
    StartBatchOperationRequest,
    StopBatchOperationRequest,
)
from temporalio.client import Client

JOB_ID_PREFIX = "mcp-batch-"

WORKFLOW_OPERATIONS = ("terminate", "cancel", "signal", "delete", "reset")
ACTIVITY_OPERATIONS = ("cancel_activities", "terminate_activities")
OPERATIONS = WORKFLOW_OPERATIONS + ACTIVITY_OPERATIONS
RESET_TYPES = ("first_workflow_task", "last_workflow_task")

# Arguments that only make sense for one operation.
OPERATION_SPECIFIC_ARGS = {
    "signal_name": "signal",
    "signal_args": "signal",
    "reset_type": "reset",
}

# int64 counters that MessageToDict would render as strings (and omit when zero).
_COUNT_FIELDS = ("total_operation_count", "complete_operation_count", "failure_operation_count")


class _InvalidArgument(ValueError):
    """A caller-supplied argument failed validation."""


def _error(tool: str, message: str, error_type: str = "invalid_argument") -> CallToolResult:
    payload = {"error": message, "type": error_type, "tool": tool}
    return CallToolResult(content=[TextContent(type="text", text=json.dumps(payload, indent=2))], is_error=True)


def _ok(payload: dict[str, Any]) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(payload, indent=2))]


def _require_text(args: dict, name: str) -> str:
    value = args.get(name)
    if value is None:
        raise _InvalidArgument(f"Missing required parameter: '{name}'")
    if not isinstance(value, str) or not value.strip():
        raise _InvalidArgument(f"'{name}' must be a non-empty string")
    return value


def _positive_number(args: dict, name: str) -> float | None:
    if name not in args:
        return None
    value = args[name]
    # bool is an int subclass; reject it explicitly.
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise _InvalidArgument(f"'{name}' must be a number greater than 0")
    return float(value)


def _to_dict(message: Message) -> dict[str, Any]:
    """Render a protobuf as JSON-ready data: RFC 3339 times, enum names, int counters."""
    data = MessageToDict(message, preserving_proto_field_name=True)
    for field in _COUNT_FIELDS:
        if field in message.DESCRIPTOR.fields_by_name:
            data[field] = int(getattr(message, field))
    return data


# --- request builders, one per operation -------------------------------------


async def _build_signal(client: Client, args: dict, identity: str) -> dict[str, Message]:
    signal_name = _require_text(args, "signal_name")
    operation = BatchOperationSignal(signal=signal_name, identity=identity)
    if "signal_args" in args:
        # Encode with the client's data converter so any payload codec applies.
        payloads = await client.data_converter.encode([args["signal_args"]])
        operation.input.payloads.extend(payloads)
    return {"signal_operation": operation}


async def _build_reset(client: Client, args: dict, identity: str) -> dict[str, Message]:
    reset_type = args.get("reset_type")
    if reset_type is None:
        raise _InvalidArgument(f"Missing required parameter: 'reset_type' (one of {', '.join(RESET_TYPES)})")
    if reset_type not in RESET_TYPES:
        raise _InvalidArgument(f"Invalid reset_type {reset_type!r}; expected one of {', '.join(RESET_TYPES)}")
    options = ResetOptions()
    getattr(options, reset_type).CopyFrom(Empty())  # selects the reset point in the oneof
    return {"reset_operation": BatchOperationReset(identity=identity, options=options)}


async def _build_terminate(client: Client, args: dict, identity: str) -> dict[str, Message]:
    return {"termination_operation": BatchOperationTermination(identity=identity)}


async def _build_cancel(client: Client, args: dict, identity: str) -> dict[str, Message]:
    return {"cancellation_operation": BatchOperationCancellation(identity=identity)}


async def _build_delete(client: Client, args: dict, identity: str) -> dict[str, Message]:
    return {"deletion_operation": BatchOperationDeletion(identity=identity)}


async def _build_cancel_activities(client: Client, args: dict, identity: str) -> dict[str, Message]:
    return {"cancel_activities_operation": BatchOperationCancelActivities(identity=identity, reason=args["reason"])}


async def _build_terminate_activities(client: Client, args: dict, identity: str) -> dict[str, Message]:
    return {"terminate_activities_operation": BatchOperationTerminateActivities(identity=identity, reason=args["reason"])}


_BUILDERS: dict[str, Callable[[Client, dict, str], Any]] = {
    "terminate": _build_terminate,
    "cancel": _build_cancel,
    "signal": _build_signal,
    "delete": _build_delete,
    "reset": _build_reset,
    "cancel_activities": _build_cancel_activities,
    "terminate_activities": _build_terminate_activities,
}


def _validate_filter(args: dict, operation: str) -> tuple[str | None, list[str] | None]:
    """Return (query, workflow_ids), exactly one set. An empty filter would target the whole namespace."""
    has_query = "query" in args and args["query"] is not None
    has_ids = "workflow_ids" in args and args["workflow_ids"] is not None
    if has_query and has_ids:
        raise _InvalidArgument("Pass either 'query' or 'workflow_ids', not both")
    if not has_query and not has_ids:
        raise _InvalidArgument("Missing filter: pass 'query' or 'workflow_ids'. A batch job without a filter would target every execution in the namespace")

    if has_query:
        query = args["query"]
        if not isinstance(query, str) or not query.strip():
            raise _InvalidArgument("'query' must be a non-empty visibility query; an empty query would target every execution in the namespace")
        return query, None

    if operation in ACTIVITY_OPERATIONS:
        raise _InvalidArgument(f"'{operation}' selects standalone activities and accepts only 'query', not 'workflow_ids'")
    workflow_ids = args["workflow_ids"]
    if not isinstance(workflow_ids, list) or not workflow_ids:
        raise _InvalidArgument("'workflow_ids' must be a non-empty list of workflow IDs")
    if not all(isinstance(wid, str) and wid.strip() for wid in workflow_ids):
        raise _InvalidArgument("Every entry in 'workflow_ids' must be a non-empty string")
    return None, workflow_ids


async def start_batch_job(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """Start a server-side batch job and return its job ID without waiting for it."""
    tool = "start_batch_job"
    try:
        operation = args.get("operation")
        if operation is None:
            raise _InvalidArgument(f"Missing required parameter: 'operation' (one of {', '.join(OPERATIONS)})")
        if operation not in OPERATIONS:
            raise _InvalidArgument(f"Unknown operation {operation!r}; expected one of {', '.join(OPERATIONS)}")
        reason = _require_text(args, "reason")

        for arg_name, owner in OPERATION_SPECIFIC_ARGS.items():
            if arg_name in args and operation != owner:
                raise _InvalidArgument(f"'{arg_name}' applies only to the '{owner}' operation, not '{operation}'")

        query, workflow_ids = _validate_filter(args, operation)
        max_ops = _positive_number(args, "max_operations_per_second")
        job_id = _require_text(args, "job_id") if "job_id" in args else f"{JOB_ID_PREFIX}{uuid.uuid4()}"

        identity = client.identity
        operation_fields = await _BUILDERS[operation](client, args, identity)
    except _InvalidArgument as e:
        return _error(tool, str(e))

    request = StartBatchOperationRequest(namespace=client.namespace, job_id=job_id, reason=reason, **operation_fields)
    if query is not None:
        request.visibility_query = query
    if workflow_ids is not None:
        request.executions.extend(WorkflowExecution(workflow_id=wid) for wid in workflow_ids)
    if max_ops is not None:
        request.max_operations_per_second = max_ops

    await client.workflow_service.start_batch_operation(request)

    result: dict[str, Any] = {
        "job_id": job_id,
        "status": "started",
        "operation": operation,
        "namespace": client.namespace,
        "reason": reason,
        "message": "Batch job started on the server. Track it with describe_batch_job; it also appears on the Temporal UI Batch page.",
    }
    if query is not None:
        result["query"] = query
    if workflow_ids is not None:
        result["workflow_ids"] = workflow_ids
    return _ok(result)


async def describe_batch_job(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """Describe a server-side batch job: state, type, timing and progress counts."""
    try:
        job_id = _require_text(args, "job_id")
    except _InvalidArgument as e:
        return _error("describe_batch_job", str(e))

    response = await client.workflow_service.describe_batch_operation(DescribeBatchOperationRequest(namespace=client.namespace, job_id=job_id))
    return _ok(_to_dict(response))


async def list_batch_jobs(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """List server-side batch jobs in the namespace, one page at a time."""
    tool = "list_batch_jobs"
    request = ListBatchOperationsRequest(namespace=client.namespace)
    try:
        if "page_size" in args:
            page_size = args["page_size"]
            if isinstance(page_size, bool) or not isinstance(page_size, (int, float)) or page_size != int(page_size) or page_size <= 0:
                raise _InvalidArgument("'page_size' must be a positive integer")
            request.page_size = int(page_size)
        token = args.get("next_page_token")
        if token:
            if not isinstance(token, str):
                raise _InvalidArgument("'next_page_token' must be the string returned by a previous list_batch_jobs call")
            try:
                request.next_page_token = base64.b64decode(token, validate=True)
            except (binascii.Error, ValueError):
                raise _InvalidArgument("'next_page_token' is not a valid page token")
    except _InvalidArgument as e:
        return _error(tool, str(e))

    response = await client.workflow_service.list_batch_operations(request)
    jobs = [_to_dict(info) for info in response.operation_info]
    result = {
        "jobs": jobs,
        "count": len(jobs),
        "next_page_token": base64.b64encode(response.next_page_token).decode() if response.next_page_token else None,
    }
    return _ok(result)


async def stop_batch_job(client: Client, args: dict) -> list[TextContent] | CallToolResult:
    """Ask the server to stop a running batch job."""
    try:
        job_id = _require_text(args, "job_id")
        reason = _require_text(args, "reason")
    except _InvalidArgument as e:
        return _error("stop_batch_job", str(e))

    request = StopBatchOperationRequest(namespace=client.namespace, job_id=job_id, reason=reason, identity=client.identity)
    await client.workflow_service.stop_batch_operation(request)
    return _ok({"job_id": job_id, "status": "stop_requested", "reason": reason})
