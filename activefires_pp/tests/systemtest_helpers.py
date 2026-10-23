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


def _geojson_files(directory: Path) -> dict[str, bytes]:
    """Return GeoJSON files and their contents, excluding the expected result."""
    return {
        path.name: path.read_bytes()
        for path in directory.glob("*.geojson")
        if path.name != "expected_result.geojson"
    }


def run_read_and_filter_systemtest(tmp_path: Path) -> dict:
    """Run the documented standalone filtering example and return its GeoJSON.

    The complete ``system_testing`` directory is copied to a temporary working
    directory first.  This keeps the repository clean even if the script writes
    its output to a fixed filename.
    """
    workdir = tmp_path / "system_testing"
    shutil.copytree(SYSTEM_TESTING_DIR, workdir)

    before = _geojson_files(workdir)

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

    after = _geojson_files(workdir)
    changed = [
        workdir / name
        for name, contents in after.items()
        if name not in before or before[name] != contents
    ]

    if len(changed) != 1:
        changed_names = [path.name for path in changed]
        raise AssertionError(
            "Expected the standalone system test to create or update exactly one "
            f"GeoJSON file, but found {changed_names}.\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    with changed[0].open(encoding="utf-8") as stream:
        return json.load(stream)
