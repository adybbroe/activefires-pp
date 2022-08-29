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

"""Post geojson formatted Alarms to a ReST-API."""

import logging
import requests

# Data payload to be posted - Example:
# {"type": "Feature", "geometry": {"type": "Point", "coordinates": [15.860621, 61.403141]},
# "properties": {"power": 3.09576535, "tb": 328.81933594, "confidence": 8,
#                "observation_time": "2022-08-02T03:27:43.850000",
#                "platform_name": "NOAA-20",  "related_detection": false}}

LOG = logging.getLogger(__name__)


def post_alarm(geojson_data, api_url, xauth=None):
    """Post an Alarm to a rest-api stored as a geojson file."""
    if xauth is None:
        headers = {"Content-Type": "application/json; charset=utf-8"}
    else:
        headers = {"Content-Type": "application/json; charset=utf-8",
                   "x-auth-satellite-alarm": xauth}

    response = requests.post(api_url,
                             headers=headers,
                             json=geojson_data)

    LOG.info("Alarm posted: Response = %s", str(response))
    LOG.debug("Status code = %d", response.status_code)
    response.raise_for_status()
