"""Setup functionality for the Uni package."""

from setuptools import find_packages, setup

setup(
    name="uni",
    version="0.0.0",
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
