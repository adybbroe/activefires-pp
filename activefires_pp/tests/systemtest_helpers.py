# Copyright (c) 2026 Adam.Dybbroe
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

"""Helpers for regression testing the standalone system-test scripts.

This module is intentionally temporary scaffolding.  The standalone system-test
script is executed as-is so that the regression test locks down its current
behaviour before the processing API is refactored.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
SYSTEM_TESTING_DIR = REPO_ROOT / "system_testing"


def run_read_and_filter_systemtest(tmp_path: Path) -> dict:
    """Run the documented standalone filtering example and return its GeoJSON.

    The complete ``system_testing`` directory is copied to a temporary working
    directory first. This keeps the repository clean because the script writes
    its result to the fixed filename ``test_output.geojson``.
    """
    workdir = tmp_path / "system_testing"
    shutil.copytree(SYSTEM_TESTING_DIR, workdir)

    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        f"{REPO_ROOT}{os.pathsep}{pythonpath}" if pythonpath else str(REPO_ROOT)
    )

    command = [
        sys.executable,
        "systemtest_read_and_filter.py",
        "-f",
        "activefires_pp_test_mask.shp",
        "-b",
        "Sverige.shp",
        "-vv",
    ]
    completed = subprocess.run(
        command,
        cwd=workdir,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    output_file = workdir / "test_output.geojson"
    if not output_file.exists():
        raise AssertionError(
            "The standalone system test did not create test_output.geojson.\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    with output_file.open(encoding="utf-8") as stream:
        return json.load(stream)
