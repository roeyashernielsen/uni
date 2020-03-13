import mlflow
from mlflow.tracking import MlflowClient


class UFlow(object):
    """
    Main class from which all pipelines should inherit.
    """

    # def __init__(self, name=None, experiment_name=None):
    #     self.name = name
    #     self.mlflow_client = MlflowClient()
    #     if experiment_name is not None:
    #         mlflow.set_experiment(experiment_name)
    #     else:
    #         mlflow.set_experiment(name)
    #
    # def __enter__(self) -> "UFlow":
    #     self.__previous_flow = prefect.context.get("flow")
    #     prefect.context.update(flow=self)
    #     return self
    #
    # def __exit__(self, _type, _value, _tb) -> None:  # type: ignore
    #     del prefect.context.flow
    #     if self.__previous_flow is not None:
    #         prefect.context.update(flow=self.__previous_flow)
    #
    #     del self.__previous_flow

    def run(self, start_step=None, end_step=None, backend=None):
        """
        Run the pipeline from start_step to end_step
        """
        pass

    def run_step(self, step_name=None, backend=None):
        """
        Run a single step
        """
        pass

    def resume(self, run_id=None, backend=None):
        """
        Resume execution of a previous run of this pipeline
        """
        pass
