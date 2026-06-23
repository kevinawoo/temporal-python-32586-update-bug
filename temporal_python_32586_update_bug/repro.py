import argparse
import asyncio
import uuid
from typing import Any

from temporalio.client import (
    Client,
    WorkflowHandle,
    WorkflowUpdateHandle,
    WorkflowUpdateStage,
)
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.envconfig import ClientConfig
from temporalio.service import RPCError
from temporalio.worker import Worker

from temporal_python_32586_update_bug import TASK_QUEUE
from temporal_python_32586_update_bug.workflows import (
    StateWorkflow,
    StateWorkflowInput,
)


def workflow_start_response(handle: WorkflowHandle[Any, Any]) -> tuple[Any, Any]:
    response = getattr(handle, "_start_workflow_response", None)
    return getattr(response, "started", None), getattr(response, "run_id", None)


def print_handle(label: str, handle: WorkflowHandle[Any, Any]) -> None:
    response_started, response_run_id = workflow_start_response(handle)
    print(
        f"{label}: "
        f"response.started={response_started!r}, "
        f"response.run_id={response_run_id!r}, "
        f"handle.run_id={handle.run_id!r}, "
        f"handle.result_run_id={handle.result_run_id!r}, "
        f"handle.first_execution_run_id={handle.first_execution_run_id!r}"
    )


async def wait_for_continue_as_new(client: Client, workflow_id: str) -> int:
    handle = client.get_workflow_handle(workflow_id)
    for _ in range(100):
        try:
            generation = await handle.query(StateWorkflow.current_generation)
            if generation == 1:
                return generation
        except RPCError:
            pass
        await asyncio.sleep(0.1)
    raise RuntimeError("workflow did not continue-as-new to generation 1")


async def update_from_handle(
    label: str, handle: WorkflowHandle[Any, Any], alert: str
) -> list[str]:
    update_handle: WorkflowUpdateHandle[list[str]] = await handle.start_update(
        StateWorkflow.record_alert,
        alert,
        wait_for_stage=WorkflowUpdateStage.COMPLETED,
    )
    result = await update_handle.result()
    print(f"{label}: update succeeded with alerts={result!r}")
    return result


async def run_repro(target_host: str | None, workflow_id: str) -> None:
    config = ClientConfig.load_client_connect_config()
    if target_host:
        config["target_host"] = target_host
    else:
        config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    async with Worker(client, task_queue=TASK_QUEUE, workflows=[StateWorkflow]):
        first_handle = await client.start_workflow(
            StateWorkflow.run,
            StateWorkflowInput(),
            id=workflow_id,
            task_queue=TASK_QUEUE,
        )
        print_handle("initial start", first_handle)

        generation = await wait_for_continue_as_new(client, workflow_id)
        print(f"current workflow generation after continue-as-new: {generation}")

        reused_start_handle = await client.start_workflow(
            StateWorkflow.run,
            StateWorkflowInput(generation=999),
            id=workflow_id,
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        print_handle("USE_EXISTING start", reused_start_handle)

        # here's the reproduction inline to re-fetch the handle, which works
        # it's also done on line 114 id_only_handle = ...
        # reused_start_handle = client.get_workflow_handle(workflow_id)

        start_handle_error: Exception | None = None
        try:
            await update_from_handle(
                "USE_EXISTING returned handle",
                reused_start_handle,
                "alert sent through returned start handle",
            )
        except Exception as err:
            start_handle_error = err
            print(
                "USE_EXISTING returned handle: update failed with "
                f"{type(err).__name__}: {err}"
            )

        id_only_handle = client.get_workflow_handle(workflow_id)
        print_handle("ID-only handle", id_only_handle)
        await update_from_handle(
            "ID-only handle workaround",
            id_only_handle,
            "alert sent through ID-only handle",
        )

        await id_only_handle.signal(StateWorkflow.finish)
        final_alerts = await id_only_handle.result()
        print(f"workflow result after cleanup: {final_alerts!r}")

        if start_handle_error is None:
            print("BUG NOT REPRODUCED: the returned start handle accepted the update.")
        else:
            print(
                "BUG REPRODUCED: the USE_EXISTING returned start handle failed, "
                "but reacquiring an ID-only handle worked."
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reproduce Python SDK USE_EXISTING start handle update behavior after "
            "continue-as-new."
        )
    )
    parser.add_argument(
        "--target-host",
        help="Temporal frontend address. Defaults to env config or localhost:7233.",
    )
    parser.add_argument(
        "--workflow-id",
        default=f"use-existing-start-handle-update-bug-{uuid.uuid4()}",
        help="Workflow ID to use for the repro run.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    await run_repro(args.target_host, args.workflow_id)


def main_sync() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    main_sync()
