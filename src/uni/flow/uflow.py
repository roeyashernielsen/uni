"""Package contain UNI Pipeline class."""
import mlflow
from prefect import Flow


class Pipeline(Flow):
    """
    The Pipeline object contains tasks that must be completed in a specified order.

    This class inherits from Prefect flow object to utilize as much functionality as
    possible from the parent class, while allowing the execution of a pipeline to be
    recorded using the mlflow tracking API. This enables a user to define a pipeline
    using native python (via prefect) while benefiting from tracking services provided
    by mlflow automatically.
    """

    def __init__(self, pipeline_name=None, experiment_name=None):
        """Instantiate new Pipeline object."""
        super().__init__(pipeline_name)

        # Initialize how many times the pipeline is executed
        self.run_count = 0

        # Append pipeline to an existing mlflow experiment, otherwise create new mlflow
        # experiment
        if experiment_name:
            mlflow.set_experiment(experiment_name)
        else:
            mlflow.set_experiment(pipeline_name)

    def run(self):
        """Execute entire pipeline while recording artifacts."""
        with mlflow.start_run(run_name=f"Run #{self.run_count}"):
            run_result = super().run()
        self.run_count += 1
        return run_result

    def resume(self, run_id=None, backend=None):
        """Resume execution of a previous run of pipeline."""
