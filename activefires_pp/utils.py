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

"""
"""

import cartopy.io.shapereader as shpreader
from datetime import date, datetime, timezone, timedelta
import zoneinfo
import yaml
from yaml import UnsafeLoader


def read_config(config_filepath):
    """Read and extract config information."""
    with open(config_filepath, 'r') as fp_:
        config = yaml.load(fp_, Loader=UnsafeLoader)

    return config


def datetime_utc2local(utc_dtime, tzone_str, is_dst=True):
    """Convert a UTC datetime to local time, using DST on default."""
    tzone = zoneinfo.ZoneInfo(tzone_str)
    utc_dtime = utc_dtime.replace(tzinfo=timezone.utc)
    return utc_dtime.astimezone(tzone)


def get_local_timezone_offset(timezone_str):
    """Get the local time zone offset as a timedelta object."""
    utcnow = datetime.utcnow()
    utcnow = utcnow.replace(tzinfo=timezone.utc)
    tzone = zoneinfo.ZoneInfo(timezone_str)
    return utcnow.astimezone(tzone).replace(tzinfo=timezone.utc) - utcnow


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
