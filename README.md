# Temporal Python SDK 32586 update bug repro

Standalone reproduction for a suspected Temporal Python SDK bug:

`client.start_workflow(..., id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING)`
can return a handle that fails `handle.start_update(...)` with
`workflow execution not found` when the reused execution is no longer the first
run in its workflow chain.

The same update succeeds if the caller discards the returned start handle and
reacquires an ID-only handle with `client.get_workflow_handle(workflow_id)`.

## What this repro does

1. Starts `StateWorkflow`.
2. The workflow immediately continues-as-new once, so the current run ID differs
   from the workflow chain's first run ID.
3. Calls `start_workflow` again with the same workflow ID and
   `WorkflowIDConflictPolicy.USE_EXISTING`.
4. Prints the returned handle's `response.started`, `response.run_id`,
   `handle.result_run_id`, and `handle.first_execution_run_id`.
5. Attempts `start_update` on that returned handle.
6. Reacquires `client.get_workflow_handle(workflow_id)` and shows the update
   succeeds through the ID-only handle.

On `temporalio==1.28.0`, the second start response has `started == False`, but
the returned handle still carries `first_execution_run_id=response.run_id`.
`start_update` forwards that chain guard and the server rejects the update.

## Run

Install prerequisites:

- [uv](https://docs.astral.sh/uv/)
- [Temporal CLI](https://docs.temporal.io/cli)

In one terminal:

```bash
temporal server start-dev
```

In another terminal:

```bash
uv sync
uv run repro-use-existing-update-bug
```

You can also point at a specific Temporal frontend:

```bash
uv run repro-use-existing-update-bug --target-host 127.0.0.1:7233
```

## Expected output on affected SDKs

```text
initial start: response.started=True, ...
current workflow generation after continue-as-new: 1
USE_EXISTING start: response.started=False, ...
USE_EXISTING returned handle: update failed with RPCError: workflow execution not found
ID-only handle workaround: update succeeded ...
BUG REPRODUCED: the USE_EXISTING returned start handle failed, but reacquiring an ID-only handle worked.
```

This was reproduced with:

- `temporalio==1.28.0`
- Temporal CLI `1.6.2`
- Temporal server `1.30.2`
