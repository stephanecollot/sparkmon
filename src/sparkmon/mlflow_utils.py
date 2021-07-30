# Copyright (c) 2021 ING Wholesale Banking Advanced Analytics
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""mlflow missing utilities."""
import tempfile
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def log_file(artifact_full_path: str):
    """Yield a file-object that is going to be saved as an artifact in mlflow.

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
