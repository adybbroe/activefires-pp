#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Adam Dybbroe

# Author(s):

#   Adam Dybbroe <Firstname.Lastname@smhi.se>

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

"""Geojson utilities."""

import geojson
import json
import logging
from trollsift import Parser, globify
import pytz
from datetime import datetime
import numpy as np

LOG = logging.getLogger(__name__)


def read_geojson_data(filename):
    """Read Geo json data from file."""
    if str(filename).endswith('.geojson') and filename.exists():
        # Read the file:
        try:
            with open(filename, "r") as fpt:
                return geojson.load(fpt)
        except json.decoder.JSONDecodeError:
            LOG.exception("Geojson file invalid and cannot be read: %s", str(filename))
    else:
        LOG.error("No valid filename to read: %s", str(filename))


def get_geojson_files_in_observation_time_order(path, pattern, time_interval):
    """Get all geojson files with filtered active fire detections (=triggered alarms) since *dtime*."""
    dtime_start = time_interval[0]
    dtime_end = time_interval[1]
    if not dtime_end:
        dtime_end = datetime.utcnow()

    p__ = Parser(pattern)
    files = path.glob(globify(pattern))
    dtimes = []
    fnames = []
    for gjson_file in files:
        fname = gjson_file.name
        res = p__.parse(fname)
        if res['start_time'] > dtime_start and res['start_time'] < dtime_end:
            dtimes.append(res['start_time'])
            fnames.append(fname)

    dtimes = np.array(dtimes)
    fnames = np.array(fnames)

    idx = dtimes.argsort()
    files = np.take(fnames, idx)
    return files.tolist()


def store_geojson_alarm(fires_alarms_dir, file_parser, idx, alarm):
    """Store the fire alarm to a geojson file."""
    utc = pytz.timezone('utc')
    start_time = datetime.fromisoformat(alarm["features"]["properties"]["observation_time"])
    platform_name = alarm["features"]["properties"]["platform_name"]
    start_time = start_time.astimezone(utc).replace(tzinfo=None)
    fname = file_parser.compose({'start_time': start_time, 'id': idx,
                                 'platform_name': platform_name})
    output_filename = fires_alarms_dir / fname
    with open(output_filename, 'w') as fpt:
        geojson.dump(alarm, fpt)

    return output_filename
