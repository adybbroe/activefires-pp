#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Adam.Dybbroe

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

"""Test getting the yaml configurations from file."""

from activefires_pp.config import read_config
from activefires_pp.config import get_xauthentication_token


def test_get_yaml_configuration(fake_yamlconfig_file):
    """Test read and get the yaml configuration from file."""
    config = read_config(fake_yamlconfig_file)
    assert config['subscribe_topics'] == '/VIIRS/L2/Fires/PP/National'
    assert config['publish_topic'] == '/VIIRS/L2/Fires/PP/SOSAlarm'
    assert config['geojson_file_pattern_alarms'] == 'sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson'
    assert config['fire_alarms_dir'] == '/path/where/the/filtered/alarms/will/be/stored'
    assert config['restapi_url'] == 'https://xxx.smhi.se:xxxx'


def test_get_xauthentication_token(fake_token_file):
    """Test getting the xauthentication token from a file."""
    fake_token = get_xauthentication_token(fake_token_file)
    assert fake_token == 'my-token'
