"""Package contains UNI UFlow class for creating, executing, and recording flows."""
import mlflow
import prefect

from uni.utils import SparkEnv


class UFlow(prefect.Flow):
    """
    UFlow object is a flow containing tasks and their order of execution.

    This object enables a user to define and execute a flow using standard python code
    (via the Prefect library). In addition, each execution of a flow is recorded using
    the MLFlow library and its tracking API. Repeated executions of the same flow are
    organized as nested runs within an MLFlow experiment. Artifacts of each executed
    task are stored as individual MLFlow runs within a nested run. This extended
    functionality is provided as a convenience to the user and also used by UNI for
    converting a UFlow object into other objects such as an Airflow DAG object.
    """

    def __init__(self, flow_name: str = None, experiment_name: str = None, spark_env=SparkEnv.Local):
        """Instantiate UFlow object."""
        super().__init__(name=flow_name)
        self.spark_env = spark_env

        # Initialize how many times the flow is executed
        self.run_count = 0

        # Append flow to an existing MLflow experiment, otherwise create new MLflow
        # experiment
        if experiment_name:
            mlflow.set_experiment(experiment_name)
        else:
            mlflow.set_experiment(flow_name)

    def run(self, **kwargs) -> "prefect.engine.state.State":
        """Execute flow while recording its execution and artifacts using MLFlow."""
        with mlflow.start_run(run_name=f"Run #{self.run_count}"):
            with prefect.context(spark_env=self.spark_env):
                run_result = super().run(**kwargs)
        self.run_count += 1
        return run_result

    # def resume(self, run_id=None, backend=None):
    #    """Resume execution of a previous run of flow."""
