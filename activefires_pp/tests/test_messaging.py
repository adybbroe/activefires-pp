#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021-2023 Adam.Dybbroe

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

"""Unit testing the message handling part of the post-processing."""

import pytest
from unittest.mock import patch
from datetime import datetime
import logging

from posttroll.message import Message
from posttroll.testing import patched_publisher
from posttroll.publisher import create_publisher_from_dict_config

from activefires_pp.post_processing import ActiveFiresPostprocessing
from activefires_pp.spatiotemporal_alarm_filtering import _create_output_message


TEST_MSG = """pytroll://VIIRS/L2/AFI/edr/2/nrk/test/polar/direct_readout file safusr.t@lxserv2313.smhi.se 2021-04-07T00:41:41.568370 v1.01 application/json {"start_time": "2021-04-07T00:28:17", "end_time": "2021-04-07T00:29:40", "orbit_number": 1, "platform_name": "NOAA-20", "sensor": "viirs", "format": "edr", "type": "netcdf", "data_processing_level": "2", "variant": "DR", "orig_orbit_number": 17530, "origin": "172.29.4.164:9099", "uri": "ssh://lxserv2313.smhi.se/san1/polar_out/direct_readout/viirs_active_fires/unfiltered/AFIMG_j01_d20210407_t0028179_e0029407_b17531_c20210407004133375592_cspp_dev.nc", "uid": "AFIMG_j01_d20210407_t0028179_e0029407_b17531_c20210407004133375592_cspp_dev.nc"}"""  # noqa

TEST_MSG_TXT = """pytroll://VIIRS/L2/AFI/edr/2/nrk/test/polar/direct_readout file safusr.t@lxserv2313.smhi.se 2023-07-05T10:27:28.821803 v1.01 application/json {"start_time": "2023-07-05T10:07:50", "end_time": "2023-07-05T10:09:15", "orbit_number": 1, "platform_name": "Suomi-NPP", "sensor": "viirs", "format": "edr", "type": "txt", "data_processing_level": "2", "variant": "DR", "orig_orbit_number": 60553, "origin": "172.29.4.164:9099", "uri": "/san1/polar_out/direct_readout/viirs_active_fires/unfiltered/AFIMG_npp_d20230705_t1007509_e1009151_b60553_c20230705102721942345_cspp_dev.txt", "uid": "AFIMG_npp_d20230705_t1007509_e1009151_b60553_c20230705102721942345_cspp_dev.txt"}"""  # noqa


def get_fake_publiser(portnumber=1979):
    """Return a fake publisher."""
    return create_publisher_from_dict_config(dict(port=portnumber, nameservers=False))


# @pytest.fixture(scope='session')
# @patch('os.path.exists')
# @patch('socket.gethostname')
# @patch('activefires_pp.post_processing.read_config')
# @patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
# def fake_af_instance(setup_comm, get_config, gethostname, path_exists):

#     get_config.return_value = CONFIG_EXAMPLE
#     gethostname.return_value = "my.host.name"
#     path_exists.return_value = True

#     myconfigfile = "/my/config/file/path"
#     myborders_file = "/my/shape/file/with/country/borders"
#     mymask_file = "/my/shape/file/with/polygons/to/filter/out"

#     afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)
#     afpp.publisher = get_fake_publiser()
#     afpp.publisher.start()

#     return afpp


# class TestCheckMessaging:

#     @pytest.fixture(autouse=True)
#     def setup_method(self, fake_af_instance):
#         self.afpp = fake_af_instance

#     def test_check_incoming_message_nc_file_exists(self):
#         input_msg = Message.decode(rawstr=TEST_MSG)

#         with patched_publisher() as published_messages:
#             result = self.afpp.check_incoming_message_and_get_filename(input_msg)

#         assert result is None
#         assert len(published_messages) == 2
#         assert 'No fire detections for this granule' in published_messages[0]
#         assert 'No fire detections for this granule' in published_messages[1]
#         assert 'VIIRS/L2/Fires/PP/National' in published_messages[0]
#         assert 'VIIRS/L2/Fires/PP/Regional' in published_messages[1]
#         self.afpp.publisher.stop()


@patch('os.path.exists')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing.get_id_from_file')
def test_check_incoming_message_nc_file_exists(setup_comm, gethostname, path_exists,
                                               get_id_from_file,
                                               fake_yamlconfig_file_post_processing):
    """Test the check of incoming message content and getting the file path from the message.

    Here we test the case when a netCDF file is provided in the message and we
    test the behaviour when the file also actually exist on the file system.
    """
    gethostname.return_value = "my.host.name"
    path_exists.return_value = True
    get_id_from_file.return_value = {'date': datetime.utcnow(), 'counter': 0}

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp.filepath_detection_id_cache = False

    afpp.publisher = get_fake_publiser(1979)
    afpp.publisher.start()

    input_msg = Message.decode(rawstr=TEST_MSG)
    with patched_publisher() as published_messages:
        result = afpp.check_incoming_message_and_get_filename(input_msg)

    afpp.publisher.stop()
    assert result is None
    assert len(published_messages) == 2
    assert 'No fire detections for this granule' in published_messages[0]
    assert 'No fire detections for this granule' in published_messages[1]
    assert 'VIIRS/L2/Fires/PP/National' in published_messages[0]
    assert 'VIIRS/L2/Fires/PP/Regional' in published_messages[1]


@patch('os.path.exists')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing.get_id_from_file')
def test_check_incoming_message_txt_file_exists(setup_comm, gethostname, path_exists,
                                                get_id_from_file,
                                                fake_yamlconfig_file_post_processing):
    """Test the check of incoming message content and getting the file path from the message.

    Here we test the case when a txt file is provided in the message and we
    test the behaviour when the file also actually exist on the file system.
    """
    gethostname.return_value = "my.host.name"
    path_exists.return_value = True
    get_id_from_file.return_value = {'date': datetime.utcnow(), 'counter': 0}

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp.publisher = get_fake_publiser(1980)
    afpp.publisher.start()

    input_msg = Message.decode(rawstr=TEST_MSG_TXT)
    with patched_publisher() as published_messages:
        result = afpp.check_incoming_message_and_get_filename(input_msg)

    afpp.publisher.stop()
    assert len(published_messages) == 0
    assert result == '/san1/polar_out/direct_readout/viirs_active_fires/unfiltered/AFIMG_npp_d20230705_t1007509_e1009151_b60553_c20230705102721942345_cspp_dev.txt'  # noqa


@patch('os.path.exists')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_check_incoming_message_txt_file_does_not_exist(setup_comm, gethostname, path_exists,
                                                        fake_yamlconfig_file_post_processing):
    """Test the check of incoming message content and getting the file path from the message.

    Here we test the case when a txt file is provided in the message and we
    test the behaviour when the file does not exist on the file system.
    """
    gethostname.return_value = "my.host.name"
    path_exists.return_value = False

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp.publisher = get_fake_publiser(1981)
    afpp.publisher.start()

    input_msg = Message.decode(rawstr=TEST_MSG_TXT)
    with patched_publisher() as published_messages:
        result = afpp.check_incoming_message_and_get_filename(input_msg)

    afpp.publisher.stop()
    assert len(published_messages) == 0
    assert result is None


@pytest.mark.parametrize("projname, expected",
                         [("sweref99", 'afimg_sweref99'),
                          ("default", 'afimg')]
                         )
def test_prepare_posttroll_message_national(caplog, projname, expected,
                                            fake_yamlconfig_file_post_processing):
    """Test prepare the posttroll message for detections on a National level."""
    myboarders_file = "/my/shape/file/with/country/boarders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    with patch('socket.gethostname') as gethostname:
        with patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication'):
            gethostname.return_value = "my.host.name"

            afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                             myboarders_file, mymask_file)

    test_filepath = "/my/geojson/file/path"

    input_msg = Message.decode(rawstr=TEST_MSG)
    with caplog.at_level(logging.INFO):
        result_messages = afpp.get_output_messages(test_filepath, input_msg, 1, proj_name=projname)

    log_expected = "Geojson file created! Number of fires = 1"
    assert log_expected in caplog.text

    res_msg = result_messages[0]

    assert res_msg.data['platform_name'] == 'NOAA-20'
    assert res_msg.data['type'] == 'GEOJSON-filtered'
    assert res_msg.data['format'] == 'geojson'
    assert res_msg.data['product'] == expected
    assert res_msg.subject == '/VIIRS/L2/Fires/PP/National'
    assert res_msg.data['uri'] == '/my/geojson/file/path'


def test_prepare_posttroll_message_regional(caplog, fake_yamlconfig_file_post_processing):
    """Test setup the posttroll message."""
    myboarders_file = "/my/shape/file/with/country/boarders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    with patch('socket.gethostname') as gethostname:
        with patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication'):
            gethostname.return_value = "my.host.name"
            afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                             myboarders_file, mymask_file)

    test_filepath = "/my/geojson/file/path"
    input_msg = Message.decode(rawstr=TEST_MSG)

    fake_region_mask = {'attributes': {'Kod_omr': '9999',
                                       'Testomr': 'Some area description'}}
    with caplog.at_level(logging.INFO):
        res_msg = afpp._generate_output_message(test_filepath, input_msg, 'default',
                                                region=fake_region_mask)

    assert res_msg.data['uri'] == test_filepath
    assert caplog.text == ''
    assert res_msg.subject == '/VIIRS/L2/Fires/PP/Regional/9999'


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_prepare_posttroll_message_no_fires(setup_comm, gethostname,
                                            fake_yamlconfig_file_post_processing):
    """Test setup the posttroll message."""
    gethostname.return_value = "my.host.name"

    myboarders_file = "/my/shape/file/with/country/boarders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myboarders_file, mymask_file)

    input_msg = Message.decode(rawstr=TEST_MSG)
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
                                  "properties": {"confidence": 8,
                                                 "observation_time": "2021-06-19T02:58:45.700000+02:00",
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
