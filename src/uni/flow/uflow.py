"""UNI Flow class."""
import mlflow
from prefect import Flow


class UFlow(Flow):
    """Main class from which all pipelines should inherit."""

    def __init__(self, name=None, experiment_name=None):
        """Init UNI flow."""
        self.run_cnt = 0
        super().__init__(name)
        if experiment_name is not None:
            mlflow.set_experiment(experiment_name)
        else:
            mlflow.set_experiment(self.name)

    def run(self):
        """Run the pipeline from start_step to end_step."""
        with mlflow.start_run(run_name=f"Run #{self.run_cnt}"):
            run_result = super().run()
        self.run_cnt += 1
        return run_result

    # TODO do we want to support state?
    def show(self, filename=None):
        """Print the flow."""
        return super().visualize(filename=filename)

    def resume(self, run_id=None, backend=None):
        """Resume execution of a previous run of this pipeline."""
