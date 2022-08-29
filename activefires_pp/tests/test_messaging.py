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

"""Unit testing the message handling part of the post-processing
"""

from unittest.mock import patch
from datetime import datetime

from posttroll.message import Message
from activefires_pp.post_processing import ActiveFiresPostprocessing
from activefires_pp.spatiotemporal_alarm_filtering import _create_output_message


TEST_MSG = """pytroll://VIIRS/L2/AFI/edr/2/nrk/test/polar/direct_readout file safusr.t@lxserv2313.smhi.se 2021-04-07T00:41:41.568370 v1.01 application/json {"start_time": "2021-04-07T00:28:17", "end_time": "2021-04-07T00:29:40", "orbit_number": 1, "platform_name": "NOAA-20", "sensor": "viirs", "format": "edr", "type": "netcdf", "data_processing_level": "2", "variant": "DR", "orig_orbit_number": 17530, "origin": "172.29.4.164:9099", "uri": "ssh://lxserv2313.smhi.se/san1/polar_out/direct_readout/viirs_active_fires/unfiltered/AFIMG_j01_d20210407_t0028179_e0029407_b17531_c20210407004133375592_cspp_dev.nc", "uid": "AFIMG_j01_d20210407_t0028179_e0029407_b17531_c20210407004133375592_cspp_dev.nc"}"""

CONFIG_EXAMPLE = {'publish_topic': '/VIIRS/L2/Fires/PP',
                  'subscribe_topics': 'VIIRS/L2/AFI',
                  'af_pattern_ibands':
                  'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}_b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt',
                  'geojson_file_pattern_national': 'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}.geojson',
                  'geojson_file_pattern_regional': 'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}_{region_name:s}.geojson',
                  'regional_shapefiles_format': 'omr_{region_code:s}_Buffer.{ext:s}',
                  'output_dir': '/path/where/the/filtered/results/will/be/stored'}


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_prepare_posttroll_message(setup_comm, get_config, gethostname):
    """Test setup the posttroll message."""

    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myboarders_file = "/my/shape/file/with/country/boarders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myboarders_file, mymask_file)

    test_filepath = "/my/geojson/file/path"

    input_msg = Message.decode(rawstr=TEST_MSG)
    res_msg = afpp._generate_output_message(test_filepath, input_msg)

    assert res_msg.data['platform_name'] == 'NOAA-20'
    assert res_msg.data['type'] == 'GEOJSON-filtered'
    assert res_msg.data['format'] == 'geojson'
    assert res_msg.data['product'] == 'afimg'
    assert res_msg.subject == '/VIIRS/L2/Fires/PP/National'
    assert res_msg.data['uri'] == 'ssh://my.host.name//my/geojson/file/path'

    input_msg = Message.decode(rawstr=TEST_MSG)

    fake_region_mask = {'attributes': {'Kod_omr': '9999',
                                       'Testomr': 'Some area description'}}
    res_msg = afpp._generate_output_message(test_filepath, input_msg, region=fake_region_mask)

    assert res_msg.subject == '/VIIRS/L2/Fires/PP/Regional/9999'

    msg_str = 'No fire detections for this granule'

    result_messages = afpp._generate_no_fires_messages(input_msg, msg_str)

    for (nat_or_reg, res_msg) in zip(['National', 'Regional'], result_messages):
        assert res_msg.data['info'] == msg_str
        assert res_msg.subject == '/VIIRS/L2/Fires/PP/' + nat_or_reg
        assert 'type' not in res_msg.data
        assert 'format' not in res_msg.data
        assert 'product' not in res_msg.data
        assert 'uri' not in res_msg.data


def test_create_output_message(tmp_path):
    """Test create output message from geojson payload."""

    input_msg = Message.decode(rawstr=TEST_MSG)
    filename = tmp_path / 'test_geojson_alarm_file.geojson'
    output_topic = '/VIIRS/L2/Fires/PP/SOSAlarm'
    geojson_alarm = {"features": {"geometry": {"coordinates": [16.249069, 57.156235], "type": "Point"},
                                  "properties": {"confidence": 8, "observation_time": "2021-06-19T02:58:45.700000+02:00",
                                                 "platform_name": "NOAA-20",
                                                 "power": 2.23312426,
                                                 "related_detection": False,
                                                 "tb": 310.37322998}, "type": "Feature"},
                     "type": "FeatureCollection"}

    output_msg = _create_output_message(input_msg, output_topic, geojson_alarm, filename)

    assert output_msg.data == {'start_time': datetime(2021, 4, 7, 0, 28, 17),
                               'end_time': datetime(2021, 4, 7, 0, 29, 40),
                               'orbit_number': 1,
                               'platform_name': 'NOAA-20',
                               'sensor': 'viirs',
                               'data_processing_level': '2',
                               'format': 'geojson',
                               'variant': 'DR',
                               'orig_orbit_number': 17530,
                               'origin': '172.29.4.164:9099',
                               'related_detection': False,
                               'power': 2.23312426,
                               'tb': 310.37322998,
                               'coordinates': [16.249069, 57.156235],
                               'file': 'test_geojson_alarm_file.geojson',
                               'uri': str(filename)}
