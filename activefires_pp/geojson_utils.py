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

"""Geojson utilities.
"""

import os
import geojson
import logging

LOG = logging.getLogger(__name__)


def read_geojson_data(filename):
    """Read Geo json data from file."""
    if str(filename).endswith('.geojson') and filename.exists:
        # Read the file:
        with open(filename, "r") as fpt:
            return geojson.load(fpt)
    else:
        LOG.warning("No filename to read: %s", str(filename))
