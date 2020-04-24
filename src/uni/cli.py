"""UNI cli."""

import click
from .converter.convert import convert

@click.group()
def uni():
    """The uni cli tool that allows uni framework functionality to be used in
    the command line."""
    pass


@uni.command('convert')
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
def cli_convert(flow_definition_path: str, new_recipe_path: str, flow_object_name: str) -> None:
    return convert(flow_definition_path, new_recipe_path, flow_object_name)


@uni.command('other')
@click.argument('text')
def cli_other(text):
    click.echo('Running other command with argument' + text)


if __name__ == "__main__":
    uni()
