#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021, 2022 Adam Dybbroe

# Author(s):

#   Adam Dybbroe <Firstname.Lastname at smhi.se>

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

"""Test the Fires filtering functionality."""

import pytest
from unittest.mock import patch
import pandas as pd
import numpy as np
import io
from datetime import datetime

from activefires_pp.post_processing import ActiveFiresShapefileFiltering
from activefires_pp.post_processing import ActiveFiresPostprocessing
from activefires_pp.post_processing import COL_NAMES


TEST_ACTIVE_FIRES_FILEPATH = "./AFIMG_j01_d20210414_t1126439_e1128084_b17637_c20210414114130392094_cspp_dev.txt"

TEST_ACTIVE_FIRES_FILE_DATA = """
# Active Fires I-band EDR
#
# source: AFIMG_j01_d20210414_t1126439_e1128084_b17637_c20210414114130392094_cspp_dev.nc
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
# number of fire pixels: 18
#
  59.14783859,   37.85886765,  331.58309937,  0.375,  0.375,    8,    4.67825794
  59.05127335,   28.15227890,  349.83993530,  0.375,  0.375,    8,    7.10289335
  59.05587006,   28.15146446,  326.76165771,  0.375,  0.375,    8,    7.10289335
  59.46587372,   29.04332352,  327.60366821,  0.375,  0.375,    8,    5.01662874
  59.59255981,   28.77226448,  345.88961792,  0.375,  0.375,    8,   13.13724804
  59.58853149,   28.77531433,  339.56134033,  0.375,  0.375,    8,    8.76600266
  59.59326553,   28.77456856,  352.21545410,  0.375,  0.375,    8,    8.76600266
  59.59757233,   28.76391029,  328.43835449,  0.375,  0.375,    8,    5.08633661
  58.35777283,   12.37761784,  327.17175293,  0.375,  0.375,    8,   17.58141518
  60.30867004,   25.53105164,  349.98794556,  0.375,  0.375,    8,    6.93412018
  55.01095581,   -2.28794742,  335.89736938,  0.375,  0.375,    8,    4.39908028
  59.52483368,   17.16816330,  336.57437134,  0.375,  0.375,    8,   14.13167953
  55.00822449,   -2.28098702,  344.50894165,  0.375,  0.375,    8,    4.16644764
  60.13325882,   16.18420029,  329.47689819,  0.375,  0.375,    8,    5.32859230
  61.30901337,   21.98561668,  341.69180298,  0.375,  0.375,    8,    8.87900448
  58.29126740,    0.20132475,  331.47875977,  0.375,  0.375,    8,    3.64687872
  57.42922211,   -3.47403550,  336.02111816,  0.375,  0.375,    8,    8.39092922
  57.42747116,   -3.47912717,  353.80722046,  0.375,  0.375,    8,   12.13035393
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
                  'timezone': 'Europe/Stockholm'}

OPEN_FSTREAM = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA)


MY_FILE_PATTERN = ("AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}_" +
                   "b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt")

TEST_REGIONAL_MASK = {}
TEST_REGIONAL_MASK['Bergslagen (RRB)'] = {'mask': np.array([False, False, False, False, False,
                                                            False, False, False, False,
                                                            False, False, False, False,  True,
                                                            False, False, False, False]),
                                          'attributes': {'Join_Count': 2, 'TARGET_FID': 142,
                                                         'Kod_omr': '1438', 'KNNAMN': 'Dals-Ed',
                                                         'LANDAREAKM': 728.0, 'KNBEF96': 5287.0,
                                                         'OBJECTID': 1080804, 'Datum_Tid': '2016-06-13',
                                                         'Testomr': 'Bergslagen (RRB)',
                                                         'Shape_Leng': 2131994.36042, 'Shape_Area': 53512139344.2},
                                          'all_inside_test_area': False, 'some_inside_test_area': True}
TEST_REGIONAL_MASK['Västerviks kommun'] = {'mask': np.array([False, False, False, False, False,
                                                             False, False, False, False,
                                                             False, False, False, False, False,
                                                             False, False, False, False]),
                                           'attributes': {'Join_Count': 30, 'TARGET_FID': 85,
                                                          'Kod_omr': '0883', 'KNNAMN': 'Västervik',
                                                          'LANDAREAKM': 1870.5, 'KNBEF96': 39579.0,
                                                          'OBJECTID': 1079223, 'Datum_Tid': '2016-06-13',
                                                          'Testomr': 'Västerviks kommun',
                                                          'Shape_Leng': 251653.298274,
                                                          'Shape_Area': 2040770168.02},
                                           'all_inside_test_area': False, 'some_inside_test_area': False}

FAKE_MASK1 = np.array([False, False, False, False, False, False, False, False,  True,
                       False, False,  True, False,  True, False, False, False, False])
FAKE_MASK2 = np.array([True, False,  True])


@patch('activefires_pp.post_processing._read_data')
def test_add_start_and_end_time_to_active_fires_data_utc(readdata):
    """Test adding start and end times to the active fires data."""
    myfilepath = TEST_ACTIVE_FIRES_FILEPATH

    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA)
    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='GMT')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=False)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata
    assert this._afdata['starttime'].shape == (18,)

    assert str(this._afdata['starttime'][0]) == '2021-04-14 11:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 11:28:08'


@patch('activefires_pp.post_processing._read_data')
def test_add_start_and_end_time_to_active_fires_data_localtime(readdata):
    """Test adding start and end times to the active fires data."""
    myfilepath = TEST_ACTIVE_FIRES_FILEPATH

    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA)
    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Europe/Stockholm')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=True)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata

    assert str(this._afdata['starttime'][0]) == '2021-04-14 13:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 13:28:08'

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Iceland')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=True)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata

    assert str(this._afdata['starttime'][0]) == '2021-04-14 11:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 11:28:08'

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Europe/Helsinki')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=True)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata

    assert str(this._afdata['starttime'][0]) == '2021-04-14 14:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 14:28:08'

    this = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Europe/Lisbon')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        this.get_af_data(filepattern=MY_FILE_PATTERN, localtime=True)

    assert 'starttime' in this._afdata
    assert 'endtime' in this._afdata

    assert str(this._afdata['starttime'][0]) == '2021-04-14 12:26:43.900000'
    assert str(this._afdata['endtime'][0]) == '2021-04-14 12:28:08'


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_regional_fires_filtering(setup_comm, get_config, gethostname):
    """Test the regional fires filtering."""
    # FIXME! This test is to big/broad. Need for refactoring!

    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)

    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA)
    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)

    # Add metadata to the pandas dataframe:
    fake_metadata = {'platform': 'j01',
                     'start_time': datetime(2021, 4, 14, 11, 26, 43, 900000),
                     'end_hour': datetime(1900, 1, 1, 11, 28, 8, 400000),
                     'orbit': '17637',
                     'processing_time': datetime(2021, 4, 14, 11, 41, 30, 392094),
                     'end_time': datetime(2021, 4, 14, 11, 28, 8)}
    afdata.attrs = fake_metadata

    af_shapeff = ActiveFiresShapefileFiltering(afdata=afdata, platform_name='NOAA-20')
    regional_fmask = TEST_REGIONAL_MASK

    mymsg = "Fake message"
    with patch('activefires_pp.post_processing.store_geojson') as store_geojson:
        with patch('activefires_pp.post_processing.ActiveFiresPostprocessing._generate_output_message') as generate_msg:
            store_geojson.return_value = "/some/output/path"
            generate_msg.return_value = "my fake output message"
            result = afpp.regional_fires_filtering_and_publishing(mymsg, regional_fmask, af_shapeff)

    store_geojson.assert_called_once()
    generate_msg.assert_called_once()

    assert len(result) == 1
    assert result[0] == "my fake output message"


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.get_global_mask_from_shapefile', side_effect=[FAKE_MASK1, FAKE_MASK2])
def test_general_national_fires_filtering(get_global_mask, setup_comm, get_config, gethostname):
    """Test the general/basic national fires filtering."""
    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)

    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA)
    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)

    # Add metadata to the pandas dataframe:
    fake_metadata = {'platform': 'j01',
                     'start_time': datetime(2021, 4, 14, 11, 26, 43, 900000),
                     'end_hour': datetime(1900, 1, 1, 11, 28, 8, 400000),
                     'orbit': '17637',
                     'processing_time': datetime(2021, 4, 14, 11, 41, 30, 392094),
                     'end_time': datetime(2021, 4, 14, 11, 28, 8)}
    afdata.attrs = fake_metadata

    af_shapeff = ActiveFiresShapefileFiltering(afdata=afdata, platform_name='NOAA-20')
    afdata = af_shapeff.get_af_data(MY_FILE_PATTERN)

    mymsg = "Fake message"

    with patch('activefires_pp.post_processing.store_geojson') as store_geojson:
        with patch('activefires_pp.post_processing.ActiveFiresPostprocessing.get_output_messages') as get_output_msg:
            store_geojson.return_value = "/some/output/path"
            get_output_msg.return_value = ["my fake output message"]
            outmsg, result = afpp.fires_filtering(mymsg, af_shapeff)

    store_geojson.assert_called_once()
    get_output_msg.assert_called_once()
    assert get_global_mask.call_count == 2

    assert isinstance(result, pd.core.frame.DataFrame)
    assert len(result) == 1
    np.testing.assert_almost_equal(result.iloc[0]['latitude'], 59.52483368)
    np.testing.assert_almost_equal(result.iloc[0]['longitude'], 17.1681633)
    assert outmsg == ["my fake output message"]


@pytest.mark.usefixtures("fake_national_borders_shapefile")
@pytest.mark.usefixtures("fake_yamlconfig_file_post_processing")
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_checking_national_borders_shapefile_file_exists(setup_comm, gethostname,
                                                         fake_yamlconfig_file_post_processing,
                                                         fake_national_borders_shapefile):
    """Test the checking of the national borders shapefile - borders shapefile exists."""
    gethostname.return_value = "my.host.name"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(str(fake_yamlconfig_file_post_processing),
                                     fake_national_borders_shapefile, mymask_file)
    afpp._check_borders_shapes_exists()

    assert afpp.shp_borders.name == 'some_national_borders_shape.yaml'
    assert afpp.shp_borders.is_file()


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.read_config')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_checking_national_borders_shapefile_file_nonexisting(setup_comm, get_config, gethostname):
    """Test the checking of the national borders shapefile - borders shapefile does not exist."""
    get_config.return_value = CONFIG_EXAMPLE
    gethostname.return_value = "my.host.name"

    myconfigfile = "/my/config/file/path"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(myconfigfile, myborders_file, mymask_file)
    with pytest.raises(OSError) as exec_info:
        afpp._check_borders_shapes_exists()

    expected = "Shape file does not exist! Filename = /my/shape/file/with/country/borders"
    assert str(exec_info.value) == expected
