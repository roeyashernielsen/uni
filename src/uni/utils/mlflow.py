"""MLflow utils."""
import os
from dataclasses import dataclass

import mlflow

from ..io import PathType
from ..io.reader import load


@dataclass
class ArtifactMD:
    name: str
    path: str
    obj_type: str = None
    file_format: str = None
    path_type: PathType = None


def log_artifact(artifact_md: ArtifactMD):
    "Log log_artifact to MLflow."
    if artifact_md.path_type == PathType.Directory:
        mlflow.log_artifacts(artifact_md.path, artifact_md.name)
        mlflow.log_param(
            f"{artifact_md.name}_path", mlflow.get_artifact_uri(artifact_md.name),
        )
    elif artifact_md.path_type == PathType.File:
        mlflow.log_artifact(artifact_md.path, artifact_md.name)
        mlflow.log_param(
            f"{artifact_md.name}_path",
            os.path.join(
                mlflow.get_artifact_uri(artifact_md.name),
                artifact_md.name + artifact_md.file_format,
            ),
        )
    else:
        raise ValueError(
            f"path_type must be a PathType but got {type(artifact_md.path_type)}"
        )

    mlflow.log_param(f"{artifact_md.name}_type", artifact_md.obj_type)


def get_artifact_md(run_id, name) -> ArtifactMD:
    """Get artifact metadata."""
    run_md = mlflow.get_run(run_id)
    return ArtifactMD(
        name=name,
        path=run_md.data.params.get(f"{name}_path", None),
        obj_type=run_md.data.params.get(f"{name}_type", None),
    )


def load_artifact(run_id, name, **kwargs):
    """Load MLflow artifact"""
    artifact_md = get_artifact_md(run_id, name)
    if artifact_md.path is None:
        raise FileNotFoundError(f"Artifact={name} did not found in run_id={run_id}")
    return load(path=artifact_md.path, obj_type=artifact_md.obj_type, **kwargs)
