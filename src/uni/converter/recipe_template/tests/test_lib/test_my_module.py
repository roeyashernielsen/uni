import pytest
import os

# Import of the custom modules should start with the '...dag'.
from ...dag.lib.my_module import my_business_logic

# Test functions must start with "test_" prefix

def test_my_business_logic_file_exists(tmpdir):
    temp_file = tmpdir.join("test_file")
    temp_file.write("Hello world")
    my_business_logic(temp_file.strpath, '', '')


def test_my_business_logic_file_does_not_exists(tmpdir):
    with pytest.raises(IOError):
        my_business_logic(os.path.join(tmpdir.strpath, "not_exists"), '', '')
