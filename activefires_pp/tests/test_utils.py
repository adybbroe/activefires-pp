#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit testing the utility functions.
"""

import pytest
import unittest
from datetime import datetime

from activefires_pp.utils import get_geometry_from_shapefile
from activefires_pp.utils import datetime_from_utc_to_local
from activefires_pp.utils import json_serial


def test_json_serial():
    """Test the json_serial function."""

    dtime_obj = datetime(2021, 4, 7, 11, 58, 53, 200000)
    res = json_serial(dtime_obj)

    assert res == "2021-04-07T11:58:53.200000"

    with pytest.raises(TypeError) as exec_info:
        other_obj = 'not okay'
        res = json_serial(other_obj)

    exception_raised = exec_info.value

    assert str(exception_raised) == "Type <class 'str'> not serializable"
