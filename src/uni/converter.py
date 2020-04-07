"""
Tool for converting flow definition file into Airflow dag definition file.

The input flow may be defined using either an UNI UFlow or Prefect Flow.
"""

import click
import subprocess
from re import search as re_search
from pathlib import Path
from runpy import run_path
from typing import Any, Dict, Set, DefaultDict
from textwrap import dedent, indent
from utils import logger
from collections import Counter


def load_flow_object(flow_definition_path: Path, flow_object_name: str) -> Any:
    """Extract flow object from flow definition file."""
    try:
        global_vars = run_path(flow_definition_path)
    except Exception:
        logger.error(
            "Flow definition file contains errors. Cannot convert", reraise=True
        )

    try:
        return global_vars[flow_object_name]
    except KeyError:
        logger.error(
            "Provided name for flow object does not match flow definition file",
            reraise=True,
        )


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


def write_imports(
    flow: Any, flow_definition_path: Path, dag_definition_path: Path
) -> None:
    """Dynamically write import statements of dag definition file."""
    with open(dag_definition_path, "w") as dag_definition_file:
        # Extract filename sans extension from path of flow definition file
        flow_definition_name = re_search(
            r"[\w-]+?(?=\.)", flow_definition_path.as_posix()
        ).group(0)

        # Assemble import statements
        imports_str = f"""\
            from datetime import datetime, timedelta
            from airflow import DAG
            from dss_airflow_utils.operators.python_operator import PythonOperator
            from dss_airflow_utils.dag_factory import dag_factory
            from dss_airflow_utils.workspace_utils import path_in_workspace
            from dss_airflow_utils.utils import get_config
            from dss_datacache_client import client
            from uni.flow import init_step
            from lib.{flow_definition_name} import (
        """

        # Assemble unique task names
        task_names = set(task.name for task in flow.tasks)

        # Write import statements
        dag_definition_file.write(dedent(imports_str))
        for task_name in sorted(task_names):
            task_name_str = f"{task_name},\n"
            dag_definition_file.write(indent(dedent(task_name_str), prefix=" " * 4))
        dag_definition_file.write(")\n\n")


def write_dag_configuration(
    flow: Any, flow_definition_path: Path, dag_definition_path: Path
) -> None:
    """Dynamically write dag configuration statements of dag definition file."""
    with open(dag_definition_path, "a") as dag_definition_file:
        # Extract name of pipeline
        dag_id = flow.name

        # Assemble dag configuration statements
        default_args_str = (
            "default_args = {"
            "'owner': 'red',"
            "'start_date': datetime(2017, 3, 20),"
            "'retries': 0,"
            "'retry_delay': timedelta(seconds=10),"
            "'queue': {"
            "'request_memory': '16G',"
            "'request_cpu': '4',"
            "'worker_type': 'python3.7-worker',}"
            "}\n\n"
        )

        create_dag_function_str = (
            "@dag_factory\n"
            "def create_dag():"
        )

        with_statement_str = f"""
            with DAG(
                dag_id='{dag_id}', schedule_interval=None, default_args=default_args
            ) as dag:
        """

        # Write dag configuration statements
        dag_definition_file.write(dedent(default_args_str))
        dag_definition_file.write(dedent(create_dag_function_str))
        dag_definition_file.write(indent(dedent(with_statement_str), prefix=" " * 4))


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
    write_imports(flow, flow_definition_path, dag_definition_path)
    write_dag_configuration(flow, flow_definition_path, dag_definition_path)
    write_operator_definitions(flow, flow_definition_path, dag_definition_path)
    write_dependency_definitions(flow, flow_definition_path, dag_definition_path)


@click.command()
@click.argument("flow_definition_path", type=click.Path(exists=True))
@click.option(
    "--dag-definition-path",
    "-d",
    default="dag.py",
    show_default=True,
    help="location of .py file containing airflow dag definition",
    type=click.Path(resolve_path=True),
)
@click.option(
    "--flow-object-name",
    "-f",
    default="flow",
    show_default=True,
    help="name of flow object defined in flow definition file",
)
def cli(
    flow_definition_path: str, dag_definition_path: str, flow_object_name: str
) -> None:
    """FLOW_DEFINITION_PATH: location of .py file containing flow definition."""
    # Convert string paths into OS-agnostic Path objects
    flow_definition_path = Path(flow_definition_path)
    dag_definition_path = Path(dag_definition_path)

    flow = load_flow_object(flow_definition_path, flow_object_name)
    write_dag_file(flow, dag_definition_path, flow_definition_path)

    # Process output file through black autoformatter
    subprocess.run(f"black -q {dag_definition_path}", shell=True)
    click.echo("Writing dag definition file...COMPLETE")


if __name__ == "__main__":
    cli()
