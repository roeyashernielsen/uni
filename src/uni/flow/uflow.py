"""UNI Flow class."""
import mlflow
import prefect
from ..flow import ustep


class UFlow:
    """Main class from which all pipelines should inherit."""

    def __init__(self, flow_name=None, experiment_name=None):
        """Init UNI flow."""
        self.flow_name = flow_name
        self.flow = prefect.Flow(self.flow_name)
        if experiment_name is not None:
            mlflow.set_experiment(experiment_name)
        else:
            mlflow.set_experiment(self.flow_name)
        self.mlflow_runs = {}
        self.cnt = 0

    def __enter__(self):
        """Trigger the enter function in Prefect flow."""
        self.flow.__enter__()
        return self

    # TODO pass flow.__exit__ the right params
    def __exit__(self, _type, _value, _tb):  # type: ignore
        """Trigger the exit function in Prefect flow."""
        self.flow.__exit__(None, None, None)

    # TODO: should we move the creation of the run object in the step? (not need to share the run_id anymore)
    def __init_mlflow_runs(self):
        for task in self.flow.tasks:
            if task.name.startswith(ustep.TASK_PREFIX):
                task_run = mlflow.start_run(run_name=task.name, nested=True)
                self.mlflow_runs.update({task.name: task_run.info.run_id})
                mlflow.end_run()

    def run(self, start_step=None, end_step=None, backend=None):
        """Run the pipeline from start_step to end_step."""
        with mlflow.start_run(run_name=f"Run #{self.cnt}") as run:
            self.mlflow_runs.update({self.flow_name: run.info.run_id})
            self.__init_mlflow_runs()
            with prefect.context(runs=self.mlflow_runs, uflow=self):
                run_result = self.flow.run()
        self.cnt += 1
        return run_result

    # TODO do we want to support state?
    def show(self, filename=None):
        self.flow.visualize(filename=filename)

    def run_step(self, step_name=None, backend=None):
        """Run a single step."""

    def resume(self, run_id=None, backend=None):
        """Resume execution of a previous run of this pipeline."""
