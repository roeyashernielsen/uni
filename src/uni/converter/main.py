"""
Tool for converting flow definition file into an IS recipe.

The input flow should be defined using UNI UFlow and UStep. The resulting recipe is
ready to be executed immediately.
"""

import click
import subprocess
import shutil
import yaml
from re import search as re_search
from pathlib import Path
from runpy import run_path
from typing import Any, Dict, Set, DefaultDict
from textwrap import dedent, indent
from collections import Counter


def load_flow_object(flow_definition_path: Path, flow_object_name: str) -> Any:
    """Extract flow object from flow definition file."""
    try:
        global_vars = run_path(flow_definition_path)
    except Exception:
        click.echo("Flow definition file contains errors. Cannot convert")

    if flow_object_name in global_vars:
        return global_vars[flow_object_name]
    else:
        raise KeyError(
            "Provided name for flow object does not match flow definition file"
        )


def create_recipe(new_recipe_path: Path) -> None:
    """Create new recipe directory using template (default behavior is overwrite)."""
    recipe_template_path = Path("src/uni/converter/recipe_template")
    shutil.rmtree(new_recipe_path, ignore_errors=True)
    shutil.copytree(recipe_template_path, new_recipe_path)


def create_task_name_map(flow: Any) -> Dict[int, str]:
    """
    Generate hash map of task memory address to labeled task name.

    This function adds numeric labels to names of tasks that are used multiple times in
    a flow definition file. For example, if task 'clean_data' is used 3 times, new names
    of the tasks are 'clean_data', 'clean_data_2', 'clean_data_3'. Each of the new names
    are then mapped to the memory address (aka unique identifier) of the task. The
    numbering of the labels is arbitrary, but using the memory address respects the
    upstream and downstream dependencies of the usage of each task.
    """
    # Create set of unique tasks and how many times each task is used
    task_name_counts = Counter(task.name for task in flow.tasks)

    task_name_map = {}
    for task in flow.tasks:
        memory_address = id(task)
        task_name_count = task_name_counts[task.name]
        if task_name_count == 1:
            # No need to label unique tasks or first task of a set of reused tasks
            label_str = ""
        else:
            # Use task count to label reused tasks
            label_str = f"_{task_name_count}"
            task_name_counts[task.name] -= 1
        task_name_map[memory_address] = f"{task.name}{label_str}"
    return task_name_map


def write_dag_configuration(
    flow: Any, flow_definition_path: Path, dag_definition_path: Path
) -> None:
    """Dynamically write dag configuration statements of dag definition file."""
    with open(dag_definition_path, "w") as dag_definition_file:
        # Extract name of pipeline
        dag_id = flow.name

        # Extract filename sans extension from path of flow definition file
        flow_definition_name = flow_definition_path.stem

        # Assemble unique task names
        task_names = set(task.name for task in flow.tasks)

        # Assemble top-level import statements
        imports_str = """\
            from datetime import datetime, timedelta
            from airflow import DAG
            from dss_airflow_utils.operators.python_operator import PythonOperator
            from dss_airflow_utils.dag_factory import dag_factory
        """

        # Assemble default argument configuration statements
        default_args_str = (
            "default_args = {"
            "'owner': 'red',"
            "'start_date': datetime(2017, 3, 20),"
            "'retries': 0,"
            "'retry_delay': timedelta(seconds=10),"
            "'queue': {"
            "'request_memory': '16G',"
            "'request_cpu': '4',"
            "'worker_type': 'spark2.4.4-python3.7-worker',}"
            "}\n"
        )

        # Assemble dag definition statements
        create_dag_function_str = "@dag_factory\ndef create_dag():"
        with_statement_str = f"""
            with DAG(
                dag_id='{dag_id}', schedule_interval=None, default_args=default_args
            ) as dag:
        """

        # Assemble UNI-related import statements
        uni_imports_str = f"""
            from .lib.uni.flow import init_step
            from .lib.{flow_definition_name} import (
        """

        # Write dag configuration statements
        dag_definition_file.write(dedent(imports_str))
        dag_definition_file.write(dedent(default_args_str))
        dag_definition_file.write(dedent(create_dag_function_str))
        dag_definition_file.write(indent(dedent(with_statement_str), prefix=" " * 4))

        # Write UNI-related import statements
        dag_definition_file.write(indent(dedent(uni_imports_str), prefix=" " * 8))
        for task_name in sorted(task_names):
            task_name_str = f"{task_name},\n"
            dag_definition_file.write(indent(dedent(task_name_str), prefix=" " * 8))
        dag_definition_file.write(")\n")


def get_func_params(
    edges: Set, labeled_task_name: str, task_name_map: Dict[int, str]
) -> Dict[str, str]:
    """Record upstream tasks and passed parameters for each task."""
    result = {}
    for edge in edges:
        if task_name_map[id(edge.downstream_task)] == labeled_task_name:
            result.update({task_name_map[id(edge.upstream_task)]: edge.key})
    return result


def get_const_params(task: Any, constants: DefaultDict) -> Dict:
    """Record values of constant parameters, if any, for a task."""
    if task in constants:
        return constants[task]
    return {}


def write_operator_definitions(
    flow: Any, flow_definition_path: Path, dag_definition_path: Path
) -> None:
    """Dynamically write airflow operator statements of dag definition file."""
    # Retrieve hash map of task memory address to labeled task name
    task_name_map = create_task_name_map(flow)

    with open(dag_definition_path, "a") as dag_definition_file:
        # Write operator statement for init task that records each root task as an
        # MLflow run
        init_operator_str = (
            f"init = PythonOperator("
            f"task_id='init', "
            f"python_callable=init_step, "
            "provide_context=True"
            ")\n"
        )
        dag_definition_file.write(indent(dedent(init_operator_str), prefix=" " * 8))

        # Write remaining operator statements
        for task in flow.tasks:
            labeled_task_name = task_name_map[id(task)]
            func_params = get_func_params(flow.edges, labeled_task_name, task_name_map)
            const_params = get_const_params(task, flow.constants)
            operator_str = (
                f"{labeled_task_name} = PythonOperator("
                f"task_id='{labeled_task_name}', "
                f"python_callable={task.name}.airflow_step, "
                f"op_kwargs={{'name': '{labeled_task_name}', "
                f"'func_param': {func_params}, "
                f"'const_params': {const_params}}}, "
                "provide_context=True"
                ")\n"
            )
            dag_definition_file.write(indent(dedent(operator_str), prefix=" " * 8))
        dag_definition_file.write("\n")


def write_dependency_definitions(
    flow: Any, flow_definition_path: Path, dag_definition_path: Path
) -> None:
    """Dynamically write dependency definition statements of dag definition file."""
    # Retrieve hash map of task memory address to labeled task name
    task_name_map = create_task_name_map(flow)

    # Write dependency definition statements using bitshift operator API in airflow
    with open(dag_definition_path, "a") as dag_definition_file:
        # Add an init task dependency to each root task
        for task in flow.root_tasks():
            labeled_task_name = task_name_map[id(task)]
            edge_str = f"init >> {labeled_task_name}\n"
            dag_definition_file.write(indent(dedent(edge_str), prefix=" " * 8))

        # Add remaining dependencies
        for edge in flow.edges:
            labeled_task_name = task_name_map[id(edge.upstream_task)]
            labeled_downstream_task_name = task_name_map[id(edge.downstream_task)]
            edge_str = f"{labeled_task_name} >> {labeled_downstream_task_name}\n"
            dag_definition_file.write(indent(dedent(edge_str), prefix=" " * 8))

        # Add return statement for create_dag() function
        dag_definition_file.write(indent(dedent("\nreturn dag"), prefix=" " * 8))


def write_dag_file(
    flow: Any, dag_definition_path: Path, flow_definition_path: Path
) -> None:
    """Generate python file containing dag definition using flow object."""
    write_dag_configuration(flow, flow_definition_path, dag_definition_path)
    write_operator_definitions(flow, flow_definition_path, dag_definition_path)
    write_dependency_definitions(flow, flow_definition_path, dag_definition_path)


def update_fields_in_config_file(config: Dict, updated_config: Dict) -> Dict:
    """Update fields in recipe config file."""
    for field in updated_config:
        if field not in config:
            raise KeyError(f"Config file does not contain field '{field}'")
        config[field] = updated_config[field]
    return config


def blacken_file(python_file_path: Path) -> None:
    """Process python file through black autoformatter without confirmation messages."""
    subprocess.run(f"black -q {python_file_path}", shell=True)


def update_config_files(flow: Any, new_recipe_path: Path) -> None:
    """Update recipe config files with user-defined parameters from flow object."""
    job_request_config_path = new_recipe_path.joinpath("job_request.yaml")
    metadata_config_path = new_recipe_path.joinpath("metadata.yaml")

    with open(job_request_config_path, "r") as file1, open(
        metadata_config_path, "r"
    ) as file2:
        job_request_config = yaml.safe_load(file1)
        metadata_config = yaml.safe_load(file2)

        # Define fields to be updated. ADD MORE AS NEED IN THIS DICT.
        updated_config = {"recipe_id": flow.name}

        # Apply updates to recipe config files
        job_request_config = update_fields_in_config_file(
            job_request_config, updated_config
        )
        metadata_config = update_fields_in_config_file(metadata_config, updated_config)

    # Write out updated recipe config files
    with open(job_request_config_path, "w") as file1, open(
        metadata_config_path, "w"
    ) as file2:
        yaml.dump(job_request_config, file1, sort_keys=False)
        yaml.dump(metadata_config, file2, sort_keys=False)


def modify_flow_definition_file(flow_definition_path: Path) -> None:
    """Modify flow definition file to enable compatibility in IS recipe."""
    with open(flow_definition_path, "r") as file:
        file_contents = file.read()

    # Modify import statements. UFlow import statement is removed because this object
    # requires importing prefect package, which is not installed on the IS Airflow
    # instance (this statement is also not needed for executing a recipe). In addition,
    # import statements for UStep and get_spark_session are loaded via relative imports
    file_contents = (
        file_contents.replace("from uni.flow.uflow import UFlow", "")
        .replace(
            "from uni.flow.ustep import UStep", "from .uni.flow.ustep import UStep"
        )
        .replace(
            "from uni.utils.spark import get_spark_session",
            "from .uni.utils.spark import get_spark_session",
        )
    )

    # Write out modified flow definition file while deleting context manager used for
    # defining flow because it references UFlow object
    with open(flow_definition_path, "w") as file:
        context_manager_found = False
        line_count = len(file_contents.split("\n"))
        index = 0
        file_contents_split = file_contents.split("\n")

        while index < line_count and not context_manager_found:
            file.write(file_contents_split[index] + "\n")
            index += 1
            context_manager_found = "with UFlow" in file_contents_split[index]

    blacken_file(flow_definition_path)


def copy_flow_definition_file(
    flow_definition_path: Path, new_recipe_path: Path
) -> None:
    """Copy flow definition file into dag/lib directory of recipe."""
    flow_definition_filename = flow_definition_path.stem + flow_definition_path.suffix
    destination_path = new_recipe_path.joinpath("dag/lib/").joinpath(
        flow_definition_filename
    )
    shutil.copyfile(flow_definition_path, destination_path)

    # Modify copied flow definition file to enable compatibility in IS recipe
    modify_flow_definition_file(destination_path)


def copy_uni_source_code(new_recipe_path: Path) -> None:
    """Copy uni source code into dag/lib directory of recipe."""
    source_code_path = Path("src/uni/")
    destination_path = new_recipe_path.joinpath("dag/lib/uni")
    shutil.copytree(source_code_path, destination_path)


@click.command()
@click.argument("flow_definition_path", type=click.Path(exists=True))
@click.option(
    "--new-recipe-path",
    "-n",
    default="../my_recipe",
    show_default=True,
    help="location of directory containing newly created recipe",
    type=click.Path(resolve_path=True),
)
@click.option(
    "--flow-object-name",
    "-f",
    default="flow",
    show_default=True,
    help="name of flow object defined in flow definition file",
)
def cli(flow_definition_path: str, new_recipe_path: str, flow_object_name: str) -> None:
    """FLOW_DEFINITION_PATH: location of .py file containing flow definition."""
    # Convert string paths into OS-agnostic Path objects
    flow_definition_path = Path(flow_definition_path)
    new_recipe_path = Path(new_recipe_path)

    # Create new recipe directory with default config files
    create_recipe(new_recipe_path)

    # Convert flow definition file into dag definition file
    flow = load_flow_object(flow_definition_path, flow_object_name)
    dag_definition_path = new_recipe_path.joinpath("dag/dag.py")
    write_dag_file(flow, dag_definition_path, flow_definition_path)
    blacken_file(dag_definition_path)

    # Update recipe config files with user-defined parameters in flow definition
    update_config_files(flow, new_recipe_path)

    # Copy flow definition file into recipe because it is currently referenced by dag
    # definition file
    copy_flow_definition_file(flow_definition_path, new_recipe_path)

    # Copy UNI source coded directory into recipe because UNI is currently not available
    # as a package
    copy_uni_source_code(new_recipe_path)
    click.echo("Creating recipe...COMPLETE")


if __name__ == "__main__":
    cli()
