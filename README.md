# UNI

## Contents

- [Flow converter](#converter)

<a name="converter"></a>
## Flow converter

Flow converter (`src/uni/converter.py`) is a tool to convert a Python file containing a flow definition into a new Python file containing an Airflow DAG definition. The input flow may be defined using either an UNI UFlow or Prefect Flow.

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
python src/uni/converter.py <input-flow-path> -f <flow-object-name> -d <output-dag-path>
```

- `<input-flow-path>` refers to path of Python file containing the flow definition

- `<flow-object-name`> refers to variable name of the flow object defined in flow definition file (typically in the `with` statement). The default value is `flow`.

- `<output-file-path>` refers to path of resulting Python file containing the converted Airflow DAG definition. The default path is `dag.py`.

### Examples

Example Python files containing flow definitions can be found in the directory `examples`. Flow definitions written using Prefect Flow are `flow_definition.py` and `flow_definition_2.py`. Flow definition written using UNI UFlow is `uflow_definition.py`.