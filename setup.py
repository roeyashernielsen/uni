"""Setup functionality for the Uni package."""
import os
import sys
from setuptools import setup, find_packages
from importlib.machinery import SourceFileLoader

if sys.version_info < (3, 7, 0):
    sys.exit("Python 3.7.0 is the minimum required version")

PROJECT_ROOT = os.path.dirname(__file__)

about = {}
with open(os.path.join(PROJECT_ROOT, "src", "uni", "__about__.py")) as file_:
    exec(file_.read(), about)

setup(
    name="uni",
    version=about["__version__"],
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=["click>=7.0", "mlflow", "prefect", "prefect[viz]"],
    entry_points="""
            [console_scripts]
            uni=uni.cli:cli
    """,
    zip_safe=False,
    python_requires=">=3.6",
)
