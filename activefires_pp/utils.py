#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021-2022 Adam Dybbroe

# Author(s):

#   Adam Dybbroe <Firstname.Lastname at smhi.se>

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

"""
"""

import pytz
import cartopy.io.shapereader as shpreader
from datetime import date, datetime, timezone, timedelta
from urllib.parse import urlparse
import pathlib
import logging

LOG = logging.getLogger(__name__)


def datetime_from_utc_to_local(utc_dt, tzone):
    """Convert datetime from UTC to local time."""

    cest = pytz.timezone(tzone)
    loc_dt = utc_dt.astimezone(cest)

    return loc_dt


def get_local_timezone():
    """Get the local timezone of this computation environment.

    https://stackoverflow.com/questions/2720319/python-figure-out-local-timezone
    """
    return datetime.now(timezone(timedelta(0))).astimezone().tzinfo


def get_geometry_from_shapefile(shapefile):
    """Read shapefile and return geometry as a multipolygon"""
    records = shpreader.Reader(shapefile).records()
    geometries = [c.geometry for c in records]

    return geometries


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def get_filename_from_posttroll_message(pytroll_message):
    """Get the filename from the Posttroll message."""
    url = urlparse(pytroll_message.data.get('uri'))
    filepath = pathlib.Path(url.path)
    LOG.info('File path: %s', str(filepath))
    return filepath
