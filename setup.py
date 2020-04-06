"""Setup functionality for the Uni package."""

import importlib.util
import os

from setuptools import find_packages, setup

spec = importlib.util.spec_from_file_location(
    "uni.version", os.path.join("src", "uni", "version.py")
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

setup(
    name="uni",
    version=module.VERSION,
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=["uni"],
    install_requires=[
        "click>=7.0",
        "mlflow",
        "prefect[dev]",
        "apache-airflow",
        "pendulum==1.4.4",
        "pyarrow",
        "pyspark", 'cloudpickle', 'pandas'
    ],
    entry_points="""
            [console_scripts]
            uni=uni.cli:cli
    """,
    include_package_data=True,
    python_requires=">=3.7",
)
