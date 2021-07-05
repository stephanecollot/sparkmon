"""mlflow missing utilities."""
import tempfile
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def log_file(artifact_full_path: str):
    """Yield a file-object that is going to be savec as an artifact in mlflow.

    mlflow API is really missing this functionality, so let's implement it via a temporary directory.
    """
    import mlflow

    artifact_full_path = Path(artifact_full_path)

    # The artifact_path argument of log_artifact() is actually the directory path
    artifact_path = artifact_full_path.parent
    if len(artifact_path.parts) == 0:
        artifact_path = None
    else:
        artifact_path = str(artifact_path)

    # Let's create a temporary directory and save the file with the same file name
    tmpdir = tempfile.TemporaryDirectory()
    local_path_tmp = Path(tmpdir.name) / artifact_full_path
    local_path_tmp.parent.mkdir(parents=True, exist_ok=True)
    fp = open(local_path_tmp, "w")

    try:
        yield fp
    finally:
        fp.close()
        mlflow.log_artifact(local_path=local_path_tmp, artifact_path=artifact_path)

    tmpdir.cleanup()


def active_run():
    """Get the active run with all logs updated."""
    import mlflow

    active_run = mlflow.active_run()
    # active_run.data.params # This is not updated
    return mlflow.get_run(active_run.info.run_id)
