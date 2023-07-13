#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Adam.Dybbroe

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

"""Test operations on the fire detection id."""

from unittest.mock import patch
from unittest import TestCase
import pandas as pd

import io
from datetime import datetime
from freezegun import freeze_time

from activefires_pp.post_processing import ActiveFiresShapefileFiltering
from activefires_pp.post_processing import ActiveFiresPostprocessing
from activefires_pp.post_processing import COL_NAMES


TEST_ACTIVE_FIRES_FILEPATH2 = "./AFIMG_npp_d20230616_t1110054_e1111296_b60284_c20230616112418557033_cspp_dev.txt"
TEST_ACTIVE_FIRES_FILEPATH3 = "./AFIMG_j01_d20230617_t1140564_e1142209_b28903_c20230617115513873196_cspp_dev.txt"
TEST_ACTIVE_FIRES_FILEPATH4 = "./AFIMG_j01_d20230618_t0942269_e0943514_b28916_c20230618095604331171_cspp_dev.txt"


# Here we have sorted out all detections not passing the filter mask!
# So, 4 fire detections are left corresponding to what would end up in the geojson files:
TEST_ACTIVE_FIRES_FILE_DATA2 = """
# Active Fires I-band EDR
#
# source: AFIMG_npp_d20230616_t1110054_e1111296_b60284_c20230616112418557033_cspp_dev.nc
# version: CSPP Active Fires version: cspp-active-fire-noaa_1.1.0
#
# column 1: latitude of fire pixel (degrees)
# column 2: longitude of fire pixel (degrees)
# column 3: I04 brightness temperature of fire pixel (K)
# column 4: Along-scan fire pixel resolution (km)
# column 5: Along-track fire pixel resolution (km)
# column 6: detection confidence ([7,8,9]->[lo,med,hi])
# column 7: fire radiative power (MW)
#
# number of fire pixels: 14
#
  62.65801239,   17.25905228,  339.66326904,  0.375,  0.375,    8,    2.51202917
  64.21694183,   17.42074966,  329.65161133,  0.375,  0.375,    8,    3.39806151
  64.56904602,   16.60095215,  346.52050781,  0.375,  0.375,    8,   20.59289360
  64.57222748,   16.59840012,  348.72860718,  0.375,  0.375,    8,   20.59289360
"""

# Here we have sorted out all detections not passing the filter mask!
# So, 1 fire detection is left corresponding to what would end up in the geojson files:
TEST_ACTIVE_FIRES_FILE_DATA3 = """
# Active Fires I-band EDR
#
# source: AFIMG_j01_d20230617_t1140564_e1142209_b28903_c20230617115513873196_cspp_dev.nc
# version: CSPP Active Fires version: cspp-active-fire-noaa_1.1.0
#
# column 1: latitude of fire pixel (degrees)
# column 2: longitude of fire pixel (degrees)
# column 3: I04 brightness temperature of fire pixel (K)
# column 4: Along-scan fire pixel resolution (km)
# column 5: Along-track fire pixel resolution (km)
# column 6: detection confidence ([7,8,9]->[lo,med,hi])
# column 7: fire radiative power (MW)
#
# number of fire pixels: 9
#
  64.46707153,   17.65028381,  330.15390015,  0.375,  0.375,    8,    3.75669074
"""


# Here we have sorted out all detections not passing the filter mask!
# So, 2 fire detections are left corresponding to what would end up in the geojson files:
TEST_ACTIVE_FIRES_FILE_DATA4 = """
# Active Fires I-band EDR
#
# source: AFIMG_j01_d20230618_t0942269_e0943514_b28916_c20230618095604331171_cspp_dev.nc
# version: CSPP Active Fires version: cspp-active-fire-noaa_1.1.0
#
# column 1: latitude of fire pixel (degrees)
# column 2: longitude of fire pixel (degrees)
# column 3: I04 brightness temperature of fire pixel (K)
# column 4: Along-scan fire pixel resolution (km)
# column 5: Along-track fire pixel resolution (km)
# column 6: detection confidence ([7,8,9]->[lo,med,hi])
# column 7: fire radiative power (MW)
#
# number of fire pixels: 10
#
  65.55922699,   17.62709618,  335.81488037,  0.375,  0.375,    8,    4.66374302
  67.27209473,   20.14731216,  348.89843750,  0.375,  0.375,    8,   11.79477501
"""

CONFIG_EXAMPLE = {'publish_topic': '/VIIRS/L2/Fires/PP',
                  'subscribe_topics': 'VIIRS/L2/AFI',
                  'af_pattern_ibands':
                  'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}' +
                  '_b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt',
                  'geojson_file_pattern_national': 'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}.geojson',
                  'geojson_file_pattern_regional': 'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}_' +
                  '{region_name:s}.geojson',
                  'regional_shapefiles_format': 'omr_{region_code:s}_Buffer.{ext:s}',
                  'output_dir': '/path/where/the/filtered/results/will/be/stored',
                  'filepath_detection_id_cache': '/path/to/the/detection_id/cache',
                  'timezone': 'Europe/Stockholm'}

MY_FILE_PATTERN = ("AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}_" +
                   "b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt")


@freeze_time('2023-06-16 11:24:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing._read_data')
def test_add_unique_day_id_to_detections_sameday(readdata, setup_comm, get_config, gethostname):
    """Test adding unique id's to the fire detection data."""
    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)

    myfilepath = TEST_ACTIVE_FIRES_FILEPATH2

    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA2)
    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='GMT')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        afdata = this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=False)

    TestCase().assertDictEqual(afpp._fire_detection_id, {'date': datetime.utcnow(),
                                                         'counter': 0})

    # 4 fire detections, so (current) ID should be raised by 4
    afdata = afpp.add_unique_day_id(afdata)
    assert 'detection_id' in afdata
    assert afdata['detection_id'].values.tolist() == ['20230616-1', '20230616-2',
                                                      '20230616-3', '20230616-4']
    TestCase().assertDictEqual(afpp._fire_detection_id, {'date': datetime.utcnow(),
                                                         'counter': 4})


@freeze_time('2023-06-17 11:55:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing._read_data')
def test_add_unique_day_id_to_detections_24hours_plus(readdata, setup_comm,
                                                      get_config, gethostname):
    """Test adding unique id's to the fire detection data."""
    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)
    afpp._fire_detection_id = {'date': datetime(2023, 6, 16, 11, 24, 0), 'counter': 4}

    myfilepath = TEST_ACTIVE_FIRES_FILEPATH3

    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA3)
    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='GMT')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        afdata = this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=False)

    TestCase().assertDictEqual(afpp._fire_detection_id, {'date': datetime(2023, 6, 16, 11, 24, 0),
                                                         'counter': 4})
    # 1 new fire detection, so (current) ID should be raised - a new day, so id
    # starting over from 0, and a new date!
    afdata = afpp.add_unique_day_id(afdata)
    assert 'detection_id' in afdata
    assert afdata['detection_id'].values.tolist() == ['20230617-1']
    TestCase().assertDictEqual(afpp._fire_detection_id, {'date': datetime(2023, 6, 17, 11, 55, 0),
                                                         'counter': 1})


@freeze_time('2023-06-18 09:56:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing._read_data')
def test_add_unique_day_id_to_detections_newday(readdata, setup_comm, get_config, gethostname):
    """Test adding unique id's to the fire detection data."""
    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)
    afpp._fire_detection_id = {'date': datetime(2023, 6, 17, 11, 55, 0), 'counter': 1}

    myfilepath = TEST_ACTIVE_FIRES_FILEPATH4

    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA4)
    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='GMT')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        afdata = this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=False)

    TestCase().assertDictEqual(afpp._fire_detection_id, {'date': datetime(2023, 6, 17, 11, 55, 0),
                                                         'counter': 1})
    # 2 new fire detections, so (current) ID should be raised - a new day, so id
    # starting over from 0, and a new date!
    afdata = afpp.add_unique_day_id(afdata)
    assert 'detection_id' in afdata
    assert afdata['detection_id'].values.tolist() == ['20230618-1', '20230618-2']
    TestCase().assertDictEqual(afpp._fire_detection_id, {'date': datetime(2023, 6, 18, 9, 56, 0),
                                                         'counter': 2})


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing._read_data')
def test_store_fire_detection_id_on_disk(readdata, setup_comm,
                                         get_config, gethostname, tmp_path):
    """Test store the latest/current detection id to a file."""
    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)
    afpp._fire_detection_id = {'date': datetime(2023, 6, 17, 11, 55, 0), 'counter': 1}

    detection_id_cache = tmp_path / 'detection_id_cache.txt'
    afpp.filepath_detection_id_cache = str(detection_id_cache)
    afpp.save_id_to_file()

    with open(afpp.filepath_detection_id_cache) as fpt:
        result = fpt.read()

    assert result == '20230617-1'


@freeze_time('2023-06-18 12:00:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing._read_data')
def test_initialize_fire_detection_id_nofile(readdata, setup_comm,
                                             get_config, gethostname, tmp_path):
    """Test initialize the fire detection id with no cache on disk."""
    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)

    assert afpp.filepath_detection_id_cache == '/path/to/the/detection_id/cache'

    expected = {'date': datetime(2023, 6, 18, 12, 0, 0), 'counter': 0}

    afpp._initialize_fire_detection_id()
    TestCase().assertDictEqual(afpp._fire_detection_id, expected)


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing._read_data')
def test_get_fire_detection_id_from_file(readdata, setup_comm,
                                         get_config, gethostname, tmp_path):
    """Test rtrieve the detection id from file."""
    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)
    afpp._fire_detection_id = {'date': datetime(2023, 6, 17, 11, 55, 0), 'counter': 1}

    detection_id_cache = tmp_path / 'detection_id_cache.txt'
    afpp.filepath_detection_id_cache = str(detection_id_cache)
    afpp.save_id_to_file()
    result = afpp.get_id_from_file()
    expected = {'date': datetime(2023, 6, 17), 'counter': 1}
    TestCase().assertDictEqual(result, expected)

    afpp._initialize_fire_detection_id()
    TestCase().assertDictEqual(afpp._fire_detection_id, expected)
