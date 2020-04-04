"""
Tool for converting pipeline definition file into airflow dag definition file.

Pipeline definition file can be written using the prefect (vanilla) package or using UNI
pipeline package.
"""

import click
import subprocess
from re import search as re_search
from pathlib import Path
from runpy import run_path
from typing import Any, Dict, Set
from textwrap import dedent, indent
from utils import logger
from collections import Counter


def load_pipeline_object(pipeline_definition_path: Path) -> Any:
    """Extract Pipeline object from pipeline definition file."""
    try:
        global_vars = run_path(pipeline_definition_path)
    except Exception:
        logger.error(
            "Flow definition file contains errors. Cannot convert", reraise=True
        )
    else:
        return global_vars["flow"]


def create_task_name_map(pipeline: Any) -> Dict[int, str]:
    """
    Generate hash map of task memory address to labeled task name.

    This function adds numeric labels to names of tasks that are used multiple times in
    a pipeline definition file. For example, if task 'clean_data' is used 3 times, new
    names of the tasks are 'clean_data', 'clean_data_2', 'clean_data_3'. Each of the new
    names are then mapped to the memory address (aka unique identifier) of the task. The
    numbering of the labels is arbitrary, but using the memory address respects the
    upstream and downstream dependencies of the usage of each task.
    """
    # Create set of unique tasks and how many times each task is used
    task_name_counts = Counter(task.name for task in pipeline.tasks)

    task_name_map = {}
    for task in pipeline.tasks:
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
        pipeline: Any, pipeline_definition_path: Path, dag_definition_path: Path
) -> None:
    """Dynamically write import statements of dag definition file."""
    with open(dag_definition_path, "w") as dag_definition_file:
        # Extract filename sans extension from path of pipeline definition file
        pipeline_definition_name = re_search(
            r"[\w-]+?(?=\.)", pipeline_definition_path.as_posix()
        ).group(0)

        # Assemble import statements
        imports_str = f"""\
            from datetime import datetime
            from airflow import DAG
            from airflow.operators.python_operator import PythonOperator
            from uni.flow import init_step
            from {pipeline_definition_name} import (
        """

        # Assemble unique task names
        task_names = set(task.name for task in pipeline.tasks)

        # Write import statements
        dag_definition_file.write(dedent(imports_str))
        for task_name in sorted(task_names):
            task_name_str = f"{task_name},\n"
            dag_definition_file.write(indent(dedent(task_name_str), prefix=" " * 4))
        dag_definition_file.write(")\n\n")


def write_dag_configuration(
        pipeline: Any, pipeline_definition_path: Path, dag_definition_path: Path
) -> None:
    """Dynamically write dag configuration statements of dag definition file."""
    with open(dag_definition_path, "a") as dag_definition_file:
        # Extract name of pipeline
        dag_id = pipeline.name

        # Assemble dag configuration statements
        default_args_str = (
            "default_args = {'owner': 'red', 'start_date': datetime(2017, 3, 20)}\n"
        )

        with_statement_str = f"""
            with DAG(
                dag_id='{dag_id}', schedule_interval=None, default_args=default_args
            ) as dag:
        """

        # Write dag configuration statements
        dag_definition_file.write(dedent(default_args_str))
        dag_definition_file.write(dedent(with_statement_str))


def get_func_params(
        edges: Set, labeled_task_name: str, task_name_map: Dict[int, str]
) -> Dict[str, str]:
    """Record upstream tasks and passed parameters for each task."""
    result = {}
    for edge in edges:
        if task_name_map[id(edge.downstream_task)] == labeled_task_name:
            result.update({task_name_map[id(edge.upstream_task)]: edge.key})
    return result


def get_const_params(task, constants):
    if task in constants:
        return constants[task]
    return {}


def write_operator_definitions(
        pipeline: Any, pipeline_definition_path: Path, dag_definition_path: Path
) -> None:
    """Dynamically write airflow operator statements of dag definition file."""
    # Retrieve hash map of task memory address to labeled task name
    task_name_map = create_task_name_map(pipeline)

    with open(dag_definition_path, "a") as dag_definition_file:
        # Write init operator
        operator_str = (
            f"init = PythonOperator("
            f"task_id='init', "
            f"python_callable=init_step, "
            "provide_context=True"
            ")\n"
        )
        dag_definition_file.write(indent(dedent(operator_str), prefix=" " * 4))
        # Write airflow operator statements
        for task in pipeline.tasks:
            labeled_task_name = task_name_map[id(task)]
            func_params = get_func_params(pipeline.edges, labeled_task_name, task_name_map)
            const_params = get_const_params(task, pipeline.constants)
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
            dag_definition_file.write(indent(dedent(operator_str), prefix=" " * 4))
        dag_definition_file.write("\n")


def write_dependency_definitions(
        pipeline: Any, pipeline_definition_path: Path, dag_definition_path: Path
) -> None:
    """Dynamically write dependency definition statements of dag definition file."""
    # Retrieve hash map of task memory address to labeled task name
    task_name_map = create_task_name_map(pipeline)

    with open(dag_definition_path, "a") as dag_definition_file:
        # Write dependency definition statements using bitshift operator API in airflow
        for task in pipeline.root_tasks():
            labeled_task_name = task_name_map[id(task)]
            edge_str = f"init >> {labeled_task_name}\n"
            dag_definition_file.write(indent(dedent(edge_str), prefix=" " * 4))
        for edge in pipeline.edges:
            labeled_task_name = task_name_map[id(edge.upstream_task)]
            labeled_downstream_task_name = task_name_map[id(edge.downstream_task)]
            edge_str = f"{labeled_task_name} >> {labeled_downstream_task_name}\n"
            dag_definition_file.write(indent(dedent(edge_str), prefix=" " * 4))


def write_dag_file(
        pipeline: Any, dag_definition_path: Path, pipeline_definition_path: Path
) -> None:
    """Generate python file containing dag definition using Pipeline object."""
    write_imports(pipeline, pipeline_definition_path, dag_definition_path)
    write_dag_configuration(pipeline, pipeline_definition_path, dag_definition_path)
    write_operator_definitions(pipeline, pipeline_definition_path, dag_definition_path)
    write_dependency_definitions(pipeline, pipeline_definition_path, dag_definition_path)


@click.command()
@click.argument("pipeline_definition_path", type=click.Path(exists=True))
@click.option(
    "--dag-definition-path",
    "-d",
    default="dag.py",
    show_default=True,
    help="location of .py file containing airflow dag definition",
    type=click.Path(resolve_path=True),
)
def cli(pipeline_definition_path: str, dag_definition_path: str) -> None:
    """FLOW_DEFINITION_PATH: location of .py file containing pipeline definition."""
    # Convert string paths into OS-agnostic Path objects
    pipeline_definition_path = Path(pipeline_definition_path)
    dag_definition_path = Path(dag_definition_path)

    pipeline = load_pipeline_object(pipeline_definition_path)
    write_dag_file(pipeline, dag_definition_path, pipeline_definition_path)

    # Process output file through black autoformatter
    subprocess.run(f"black -q {dag_definition_path}", shell=True)
    click.echo("Writing dag definition file...COMPLETE")


if __name__ == "__main__":
    cli()
