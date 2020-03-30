# UNI

## Contents

- [UNI flow converter](#converter)

<a name="converter"></a>
## UNI flow converter

UNI flow converter is a tool to convert a python file containing a Prefect flow definition into a new python file containing an Airflow DAG definition. The converter dynamically extracts the necessary information from a Prefect `flow` object&mdash;it does not parse the input python file.

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
5. Execute following command from top-level directory of repository to perform conversion
```
python src/uni/converter.py <input-file-path> --dag-definition-path <output-file-path>
```

- `<input-file-path>` refers to path of python file containing a Prefect flow definition

- `<output-file-path>` refers to path of python file where the resulting converted python file will be stored. The default path is `dag.py`.

### Examples

Example python files containing Prefect flow definitions (and their resulting converted dag definition files) can be found in the directory `examples`.