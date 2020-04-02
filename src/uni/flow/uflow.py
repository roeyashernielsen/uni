"""UNI Flow class."""
import mlflow
import prefect


class UFlow:
    """Main class from which all pipelines should inherit."""

    def __init__(self, name=None, experiment_name=None):
        """Init UNI flow."""
        self.run_cnt = 0
        self.name = name
        self.flow = prefect.Flow(self.name)
        self.tasks = self.flow.tasks
        self.edges = self.flow.edges
        self.constants = self.flow.constants

        if experiment_name is not None:
            mlflow.set_experiment(experiment_name)
        else:
            mlflow.set_experiment(self.name)

    def __enter__(self):
        """Trigger the enter function in Prefect flow."""
        self.flow.__enter__()
        return self

    # TODO pass to flow.__exit__ the right params
    def __exit__(self, _type, _value, _tb):  # type: ignore
        """Trigger the exit function in Prefect flow."""
        self.flow.__exit__(None, None, None)

    def run(self):
        """Run the pipeline from start_step to end_step."""
        with mlflow.start_run(run_name=f"Run #{self.run_cnt}"):
            run_result = self.flow.run()
        self.run_cnt += 1
        return run_result

    # TODO do we want to support state?
    def show(self, filename=None):
        """Print the flow."""
        return self.flow.visualize(filename=filename)

    def resume(self, run_id=None, backend=None):
        """Resume execution of a previous run of this pipeline."""
