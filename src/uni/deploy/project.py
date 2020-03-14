"""Deploy utils."""


class Project:
    """Main class from which all pipelines should inherit."""

    def __init__(self, pipeline):
        """Project constructor."""

    def save(self, repo_uri=None):
        """Build a UNI project from a pipeline."""

    def load(self, repo_uri=None):
        """Load a UNI project form uri."""

    # # cli
    # def run(
    #     self,
    #     uri,
    #     entry_point="main",
    #     version=None,
    #     parameters=None,
    #     experiment_name=None,
    #     experiment_id=None,
    #     backend=None,
    #     backend_config=None,
    #     use_conda=True,
    #     storage_dir=None,
    #     synchronous=True,
    #     run_id=None,
    # ):
    #     """Run the project in the specified environment."""
    #
    # def deploy(
    #     self,
    #     uri,
    #     entry_point="main",
    #     version=None,
    #     parameters=None,
    #     experiment_name=None,
    #     experiment_id=None,
    #     backend=None,
    #     backend_config=None,
    #     use_conda=True,
    #     storage_dir=None,
    #     synchronous=True,
    #     run_id=None,
    #     cron_expression=None,
    # ):
    #     """Run the project in the specified environment."""
