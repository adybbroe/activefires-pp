#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2026 Adam.Dybbroe

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

"""Unit testing the core post processing part of the project."""


import logging
import datetime as dt

from posttroll.message import Message
from posttroll.testing import patched_publisher, patched_subscriber_recv

from activefires_pp.post_processing import ActiveFiresPostprocessing
from activefires_pp.post_processing import get_metadata_from_filename

EDRFILE_PATTERN = 'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}_b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt'  # noqa


def test_run_postprocessing(caplog,
                            fake_yamlconfig_file_post_processing,
                            fake_active_fires_ascii_file_system_test_case,
                            fake_detection_id_cache_file,
                            sample_filtermask,
                            sample_sweden):
    """Test run the Active Fires post processing and publishing."""
    res = get_metadata_from_filename(EDRFILE_PATTERN, str(fake_active_fires_ascii_file_system_test_case))

    start_time = res['start_time']
    end_time = res['end_time']

    data = {"uri": str(fake_active_fires_ascii_file_system_test_case),
            "uid": fake_active_fires_ascii_file_system_test_case.name,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "orbit_number": int(res['orbit']),
            "platform_name": "NOAA-20",
            "sensor": "viirs",
            "format": "edr",
            "type": "txt",
            "data_processing_level": "2",
            "variant": "DR", "orig_orbit_number": (res['orbit']),
            "origin": "xxx.xx.x.xxx:xxxx"
            }

    messages = [Message("VIIRS/L2/AFI", "file", data=data)]

    with caplog.at_level(logging.DEBUG):
        with patched_subscriber_recv(messages):
            with patched_publisher() as published_messages:
                fire_pp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                                    sample_sweden, sample_filtermask)
                fire_pp.filepath_detection_id_cache = fake_detection_id_cache_file
                fire_pp.run_and_publish()

                assert len(published_messages) == 2
                message = Message(rawstr=published_messages[0])
                assert message.data['start_time'] == dt.datetime(2021, 4, 14, 11, 26, 43, 900000)
