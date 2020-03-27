"""UNI Flow class."""
import mlflow
import prefect


class UFlow:
    """Main class from which all pipelines should inherit."""

    def __init__(self, flow_name=None, experiment_name=None):
        """Init UNI flow."""
        self.run_cnt = 0
        self.flow_name = flow_name
        self._flow = prefect.Flow(self.flow_name)

        if experiment_name is not None:
            mlflow.set_experiment(experiment_name)
        else:
            mlflow.set_experiment(self.flow_name)

    def __enter__(self):
        """Trigger the enter function in Prefect flow."""
        self._flow.__enter__()
        return self

    # TODO pass to flow.__exit__ the right params
    def __exit__(self, _type, _value, _tb):  # type: ignore
        """Trigger the exit function in Prefect flow."""
        self._flow.__exit__(None, None, None)

    def run(self):
        """Run the pipeline from start_step to end_step."""
        with mlflow.start_run(run_name=f"Run #{self.run_cnt}"):
            run_result = self._flow.run()
        self.run_cnt += 1
        return run_result

    # TODO do we want to support state?
    def show(self, filename=None):
        """Print the flow."""
        self._flow.visualize(filename=filename)

    def resume(self, run_id=None, backend=None):
        """Resume execution of a previous run of this pipeline."""
