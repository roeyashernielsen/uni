"""Package contains UNI UFlow class for creating, executing, and recording pipelines."""
import mlflow
import prefect


class UFlow(prefect.Flow):
    """
    UFlow object is a pipeline containing tasks and their order of execution.

    This object enables a user to define and execute a pipeline using standard python
    code (via the Prefect library). In addition, each execution of a pipeline is
    recorded using the MLFlow library and its tracking API. Repeated executions of the
    same pipeline are organized as nested runs within an MLFlow experiment. Artifacts
    of each executed task are stored as individual MLFlow runs within a nested run. This
    extended functionality is provided as a convenience to the user and also used by UNI
    for converting a UFlow object into other pipeline objects such as an Airflow DAG
    object.
    """

    def __init__(self, pipeline_name: str = None, experiment_name: str = None):
        """Instantiate UFlow object."""
        super().__init__(pipeline_name)

        # Initialize how many times the pipeline is executed
        self.run_count = 0

        # Append pipeline to an existing MLflow experiment, otherwise create new MLflow
        # experiment
        if experiment_name:
            mlflow.set_experiment(experiment_name)
        else:
            mlflow.set_experiment(pipeline_name)

    def run(self) -> "prefect.engine.state.State":
        """Execute pipeline while recording its execution and artifacts using MLFlow."""
        with mlflow.start_run(run_name=f"Run #{self.run_count}"):
            run_result = super().run()
        self.run_count += 1
        return run_result

    # def resume(self, run_id=None, backend=None):
    #    """Resume execution of a previous run of pipeline."""
