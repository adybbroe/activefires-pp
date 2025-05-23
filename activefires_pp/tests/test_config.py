#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022, 2023, 2024 Adam.Dybbroe

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


def test_get_yaml_configuration_for_alarm_filtering(fake_yamlconfig_file):
    """Test read and get the yaml configuration from file for alarm filtering."""
    config = read_config(fake_yamlconfig_file)
    assert config['products'] == ['afimg']
    assert config['subscribe_topics'] == '/VIIRS/L2/Fires/PP/National'
    assert config['publish_topic'] == '/VIIRS/L2/Fires/PP/SOSAlarm'
    assert config['geojson_file_pattern_alarms'] == 'sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson'
    assert config['fire_alarms_dir'] == '/path/where/the/filtered/alarms/will/be/stored'
    assert config['restapi_url'] == 'https://xxx.smhi.se:xxxx'


def test_get_xauthentication_token(fake_token_file):
    """Test getting the xauthentication token from a file."""
    fake_token = get_xauthentication_token(fake_token_file)
    assert fake_token == 'my-token'


def test_read_yaml_configuration_for_postprocessing(fake_yamlconfig_file_post_processing):
    """Test read in the yaml configuration for fires post processing."""
    config = read_config(fake_yamlconfig_file_post_processing)

    assert config['subscribe_topics'] == 'VIIRS/L2/AFI'
    assert config['publish_topic'] == '/VIIRS/L2/Fires/PP'
    assert config['timezone'] == 'Europe/Stockholm'
    assert config['af_pattern_ibands'] == 'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}_b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt'  # noqa
    assert config['regional_shapefiles_format'] == 'omr_{region_code:s}_Buffer.{ext:s}'
    assert config['output_dir'] == '/path/where/the/filtered/results/will/be/stored'

    assert len(config['output']['national']['default']) == 1
    assert config['output']['national']['default'] == {'geojson_file_pattern':
                                              'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}.geojson'}  # noqa
    assert config['output']['national']['sweref99'] == {'geojson_file_pattern':
                                                        'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}_sweref99.geojson',  # noqa
                                                        'projection': "EPSG:3006"}

    assert config['output']['regional']['default'] == {'geojson_file_pattern':
                                              'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}_{region_name:s}.geojson'}  # noqa
