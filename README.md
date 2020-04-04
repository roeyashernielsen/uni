# UNI

## Contents

- [Pipeline converter](#converter)

<a name="converter"></a>
## Pipeline converter

Pipeline converter (`src/uni/converter.py`) is a tool to convert a python file containing a pipeline definition into a new python file containing an Airflow (vanilla) DAG definition. The input pipeline may be defined using either the UNI Pipeline API or the Prefect API.

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
5. Execute following command from top-level directory of repository to perform conversion
```
python src/uni/converter.py <input-file-path> --pipeline-object-name <pipeline-obj-name> --dag-definition-path <output-file-path>
```

- `<input-file-path>` refers to path of python file containing a pipeline definition

- `<pipeline-obj-name`> refers to python variable name of the pipeline object defined in pipeline definition file. The default name is `pipeline`.

- `<output-file-path>` refers to path of python file where the resulting converted python file will be stored. The default path is `dag.py`.

### Examples

Example python files containing pipeline definitions can be found in the directory `examples`. Pipeline definitions written using the Prefect API are `flow_definition.py` and `flow_definition_2.py`. Pipeline definition written using the UNI Pipeline API is `uni_pipeline_definition.py`.