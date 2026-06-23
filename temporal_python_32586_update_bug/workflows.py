from dataclasses import dataclass, field

from temporalio import workflow


@dataclass
class StateWorkflowInput:
    generation: int = 0
    alerts: list[str] = field(default_factory=list)


@workflow.defn
class StateWorkflow:
    @workflow.init
    def __init__(self, input: StateWorkflowInput) -> None:
        self.generation = input.generation
        self.alerts = list(input.alerts)
        self.completed = False

    @workflow.run
    async def run(self, input: StateWorkflowInput) -> list[str]:
        if input.generation == 0:
            workflow.continue_as_new(
                StateWorkflowInput(generation=1, alerts=input.alerts)
            )

        await workflow.wait_condition(lambda: self.completed)
        return self.alerts

    @workflow.update
    async def record_alert(self, alert: str) -> list[str]:
        self.alerts.append(alert)
        return self.alerts

    @workflow.query
    def current_generation(self) -> int:
        return self.generation

    @workflow.signal
    def finish(self) -> None:
        self.completed = True
