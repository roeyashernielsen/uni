# UNI

## Contents

- [Flow converter](#converter)

<a name="converter"></a>
## Flow converter

Flow converter (`src/uni/converter.py`) is a tool for converting a Python file containing a flow definition into a recipe ready to be executed in Intelligence Studio. _No knowledge of Airflow, DAGs, or recipe structure is required._

### Introduction

A flow definition file contains a set of tasks&mdash;each a standard Python function prefixed with the `@UStep` decorator. Function signatures must contain the argument `**kwargs`. Tasks support working with Spark DataFrames via pySpark. 

Flow dependency definitions should be placed below the task definitions, constructed using `UFlow`, and written in standard Python. Function calls must use keyword arguments. 

A simple flow definition file:

```python
from uni.flow.uflow import UFlow
from uni.flow.ustep import UStep

# Define tasks
@UStep
def task1(**kwargs):
    return "hello"

@UStep
def task2(arg1, **kwargs):
    print(arg1 + " world")

# Define task dependencies
with UFlow("my_flow") as flow:
    result = task1()
    task2(arg1=result)
```

### Installation and usage

1. Clone this repository
2. Open a terminal window and navigate to the repository directory
3. Install dependencies via conda (can be installed via [Miniconda](https://docs.conda.io/en/latest/miniconda.html)):
```
conda env create -f conda.yml
```
4. Activate virtual environment
```
source activate uni
```
5. Install UNI
```
pip install -e . -U
```
5. Perform conversion by executing command from top-level directory of repository
```
python src/uni/converter.py <input-flow-path> -f <flow-object-name> -n <new-recipe-path>
```

- `<input-flow-path>` refers to path of Python file containing the flow definition

- `<flow-object-name`> refers to variable name of the flow object defined in flow definition file (typically in the `with` statement). The default value is `flow`.

- `<new-recipe-path>` refers to path of newly created recipe. The default path is `../my_recipe`.

Execute the recipe by copying the entire directory into Intelligence Studio, navigating to the directory in the terminal, and enter `recipe taste job_request.yaml`.

<a name="examples"></a>
### Examples

Example flow definition files can be found in the directory `examples`.