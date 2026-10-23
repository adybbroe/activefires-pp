# Copyright (c) 2026 Adam.Dybbroe
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

"""Regression tests for the standalone processing examples."""

import json

from activefires_pp.tests.systemtest_helpers import (
    SYSTEM_TESTING_DIR,
    run_read_and_filter_systemtest,
)


def test_systemtest_read_and_filter(tmp_path):
    """The standalone filtering example should reproduce the accepted output."""
    actual = run_read_and_filter_systemtest(tmp_path)

    expected_file = SYSTEM_TESTING_DIR / "expected_result.geojson"
    with expected_file.open(encoding="utf-8") as stream:
        expected = json.load(stream)

    assert actual == expected
