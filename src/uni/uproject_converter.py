"""
Tool for converting flow definition file into an UNI project

The input flow definition file must be defined using an UNI UFlow.
"""

import click
import inspect
from pathlib import Path
from runpy import run_path


from uni.flow.uflow import UFlow
from uni.flow.ustep import UStep
from uni.converter import load_flow_object

def write_step_files(step_code, imports, uproject_folder_path):
    for step in step_code.keys():
        click.echo(f'Writing the .py file for step {step}')
        with Path.open(uproject_folder_path.joinpath(step+'.py'), mode='w') as step_file:
            for line in imports:
                step_file.write(line)

            step_file.write('\n\n')

            for line in step_code[step]:
                step_file.write(line)
    return


@click.command()
@click.argument("uflow_definition_path", type=click.Path(exists=True))
@click.option(
    "--uproject-folder-path",
    "-p",
    default=Path.home() / 'UProject',
    show_default=True,
    help="location of the UProject",
    type=click.Path(resolve_path=True),
)
@click.option(
    "--uflow-object-name",
    "-f",
    default="flow",
    show_default=True,
    help="name of flow object defined in flow definition file",
)
def cli(
    uflow_definition_path: str, uproject_folder_path: str, uflow_object_name: str
) -> None:
    """FLOW_DEFINITION_PATH: location of .py file containing flow definition."""
    uflow_definition_path = Path(uflow_definition_path)
    uproject_folder_path = Path(uproject_folder_path)

    # Create the directory
    uproject_folder_path.mkdir()

    click.echo(f'Processing the UFlow in {uflow_definition_path}')
    click.echo(f'Saving the UProject in the {uproject_folder_path} directory')

    with open(uflow_definition_path, 'r') as uflow_file:
        uflow_file_lines = uflow_file.readlines()

    all_imports = [line for line in uflow_file_lines if 'import' in line]

    #with open(uproject_folder_path, "w") as dag_definition_file:

    print(all_imports)

    step_code = {}

    #
    global_vars = run_path(uflow_definition_path)

    try:
        flow = global_vars[uflow_object_name]
    except KeyError:
        logger.error(
            "Provided name for flow object does not match flow definition file",
            reraise=True,
        )

    # Create a list for the UStep
    usteps = []

    for obj in global_vars.keys():
        # Check if this obj is a UStep
        if isinstance(global_vars[obj], UStep):
            # Grab the source code for the step
            step_code[obj] = inspect.getsourcelines(global_vars[obj])[0]

        # Verify there is only one flow and it matches the name provided
        if isinstance(global_vars[obj], UFlow):
            if obj != uflow_object_name:
                click.echo("A UFlow object was found that does not match provide name")

    click.echo(f"Found the following USteps: {step_code.keys()}")

    click.echo(step_code)

    # Write the steps
    write_step_files(step_code, all_imports, uproject_folder_path)

    print(flow)


    # Identify all the step functions via global_vars


    # First load the flow
    #flow = load_flow_object(uflow_definition_path, uflow_object_name)

    # Identify all the step functions
    # task_set = set(task.name for task in flow.tasks)
    # click.echo(task_set)

    # for task in flow.tasks:
    #     click.echo(task.name)
    #
    # finder = ModuleFinder()
    # finder.run_script(uflow_definition_path)
    # print('\n')
    # print('\n', finder.modules.keys())

    return

if __name__ == "__main__":
    cli()
