#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2024 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c22526.ad.smhi.se>

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

"""Sanity checking the active fire detections - removing spurious data points.

So far the only known spurious detections are those caused by onboaud issues in
I-band 4, due to high energy ionisizing particles. These events are frequent
over the South atlantic magnetic anomaly, but occassionally also occur
elsewhere on the globe, probably more frequent when the solar activity is high.

"""

import logging

logger = logging.getLogger(__name__)


def remove_spurious_detections(af_dataframe):
    """Check active fires data and return those that are not classified as spurious."""
    spurious = (af_dataframe['tb']/af_dataframe['power'] > 1000) & (af_dataframe['tb'] > 310)
    non_spurious = ~spurious
    n_spurious = len(af_dataframe[spurious])
    if n_spurious > 0:
        logger.info(f"Number of spurious detections filtered out = {n_spurious}")
        for idx in range(len(af_dataframe[spurious])):
            logger.info("({lon},{lat}): Tb4 = {tb4} FRP = {frp}".format(
                lon=af_dataframe[spurious]['longitude'].values[idx],
                lat=af_dataframe[spurious]['latitude'].values[idx],
                tb4=af_dataframe[spurious]['tb'].values[idx],
                frp=af_dataframe[spurious]['power'].values[idx]))

    return af_dataframe[non_spurious]
