"""Tests for verifying functionality of UNI flow converter application."""

from pathlib import Path
from src.uni import converter
from prefect import Flow as flow_type
import filecmp
import pytest
from typing import Iterator


@pytest.fixture()
def dag_definition_path() -> Iterator[Path]:
    """Perform setup and tear down for temporary dag definition file."""
    # Define path of dag definition file to be generated
    path = Path("examples/generated_dag_definition.py")
    yield path

    # Delete dag definition file generated during tests
    path.unlink()


def test_load_flow_object() -> None:
    """Test functionality of function load_flow_object()."""
    flow_definition_path = Path("examples/flow_definition.py")
    flow = converter.load_flow_object(flow_definition_path)
    assert isinstance(flow, flow_type)


@pytest.mark.xfail(
    strict=True,
    reason=(
        """this test fails because operator definition and airflow dependency
        statements are written out but not ordered in any way. This means that the
        resulting dag definition file is not idempotent. In order for this test to pass,
        the order must be made static."""
    ),
)
def test_write_dag_file(dag_definition_path: Path) -> None:
    """
    Test functionality of function write_dag_file().

    This is an integration test that evaluates whether all components of the dag
    definition file is written as expected.
    """
    flow_definition_path = Path("examples/flow_definition.py")
    expected_dag_definition_path = Path("examples/dag_definition.py")

    # Generate flow object using flow definition file
    flow = converter.load_flow_object(flow_definition_path)

    # Perform conversion of flow definition file into dag definition file
    converter.write_dag_file(flow, dag_definition_path, flow_definition_path)

    # Check whether dag definition file was generated as expected
    assert filecmp.cmp(expected_dag_definition_path, dag_definition_path)
