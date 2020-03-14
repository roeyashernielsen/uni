"""Setup functionality for the Uni package."""
import os
from importlib.machinery import SourceFileLoader

from setuptools import find_packages, setup

version = (
    SourceFileLoader("uni.version", os.path.join("src", "uni", "version.py"))
    .load_module()
    .VERSION
)

setup(
    name="uni",
    version=version,
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=["uni"],
    install_requires=["click>=7.0", "mlflow", "prefect", "prefect[viz]"],
    entry_points="""
            [console_scripts]
            uni=uni.cli:cli
    """,
    include_package_data=True,
    python_requires=">=3.7",
)
