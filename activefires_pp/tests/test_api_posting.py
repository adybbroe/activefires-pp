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

"""Test the api-posting part."""


import pytest
import logging
import requests
import responses
import json
from activefires_pp.api_posting import post_alarm

PAST_ALARMS_MONSTERAS3 = """{"features": {"geometry": {"coordinates": [16.252192, 57.15242], "type": "Point"},
"properties": {"confidence": 8, "observation_time": "2021-06-18T14:49:01.750000+02:00",
"platform_name": "NOAA-20", "related_detection": false, "power": 2.87395763, "tb": 330.10293579},
"type": "Feature"}, "type": "FeatureCollection"}"""


@responses.activate
def test_send_alarm_post_ok():
    """Test send alarm."""
    features = json.loads(PAST_ALARMS_MONSTERAS3)
    alarm = features['features']
    restapi_url = "https://my-fake-example.org"

    rsp1 = responses.Response(
        method="POST",
        url=restapi_url,
    )
    responses.add(rsp1)

    post_alarm(alarm, restapi_url)


@responses.activate
def test_send_alarm_post_raise_exception():
    """Test send alarm."""
    features = json.loads(PAST_ALARMS_MONSTERAS3)
    alarm = features['features']
    restapi_url = "https://my-fake-example.org"

    rsp1 = responses.Response(
        method="POST",
        url=restapi_url,
    )
    responses.add(rsp1)
    responses.add(
        responses.POST,
        "http://twitter.com/api/1/foobar",
        status=500,
    )
    with pytest.raises(Exception) as exec_info:
        post_alarm(alarm, "http://twitter.com/api/1/foobar")

    assert exec_info.type == requests.exceptions.HTTPError


@responses.activate
def test_send_alarm_post_log_messages(caplog):
    """Test send alarm."""
    features = json.loads(PAST_ALARMS_MONSTERAS3)
    alarm = features['features']
    restapi_url = "https://my-fake-example.org"

    rsp1 = responses.Response(
        method="POST",
        url=restapi_url,
    )
    responses.add(rsp1)

    with caplog.at_level(logging.INFO):
        _ = post_alarm(alarm, restapi_url)

    log_output = "Alarm posted: Response = <Response [200]>"
    assert log_output in caplog.text
