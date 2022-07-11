#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021, 2022 Adam.Dybbroe

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
from datetime import datetime, timedelta
from activefires_pp.utils import datetime_utc2local
from activefires_pp.utils import json_serial
from freezegun import freeze_time


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


@freeze_time('2022-03-26 18:12:05')
def test_utc2localtime_conversion():
    """Test converting utc time to local time."""

    atime1 = datetime.utcnow()
    dtobj = datetime_utc2local(atime1, 'Europe/Stockholm')
    assert dtobj.strftime('%Y%m%d-%H%M') == '20220326-1912'

    atime2 = atime1 + timedelta(days=1)
    dtobj = datetime_utc2local(atime2, 'Europe/Stockholm')
    assert dtobj.strftime('%Y%m%d-%H%M') == '20220327-2012'

    dtobj = datetime_utc2local(atime1, 'Australia/Sydney')
    assert dtobj.strftime('%Y%m%d-%H%M') == '20220327-0512'

    dtobj2 = datetime_utc2local(atime1, 'Etc/GMT-11')
    assert dtobj2.strftime('%Y%m%d-%H%M') == '20220327-0512'

    dtobj = datetime_utc2local(atime1 + timedelta(days=30), 'Australia/Sydney')
    assert dtobj.strftime('%Y%m%d-%H%M') == '20220426-0412'
