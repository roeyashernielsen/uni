"""Setup functionality for the Uni package."""
import os
from setuptools import setup, find_packages
from importlib.machinery import SourceFileLoader

# version = (
#     SourceFileLoader("uni.version", os.path.join("src", "uni", "version.py"))
#     .load_module()
#     .VERSION
# )

setup(
    name="uni",
    version="0.0.1",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=["click>=7.0", "mlflow", "prefect", "prefect[viz]"],
    entry_points="""
            [console_scripts]
            uni=uni.cli:cli
    """,
    zip_safe=False,
    include_package_data=True,
    python_requires=">=3.7",
)
