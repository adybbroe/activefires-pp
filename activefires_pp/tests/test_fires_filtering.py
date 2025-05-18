#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 - 2024 Adam Dybbroe

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
# along with this 2program.  If not, see <http://www.gnu.org/licenses/>.

"""Test the Fires filtering functionality."""

import pytest
from unittest.mock import patch
from unittest import TestCase

import pandas as pd
from geojson import FeatureCollection
import numpy as np
import io
import logging
from datetime import datetime
from freezegun import freeze_time

from activefires_pp.post_processing import ActiveFiresShapefileFiltering
from activefires_pp.post_processing import ActiveFiresPostprocessing
from activefires_pp.post_processing import COL_NAMES
from activefires_pp.tests.test_utils import AF_FILE_PATTERN
from activefires_pp.utils import UnitConverter
from activefires_pp.post_processing import geojson_feature_collection_from_detections
from activefires_pp.post_processing import read_cspp_output_data
from activefires_pp.post_processing import CSPP_ASCII_FILE_FORMAT_ERROR


TEST_ACTIVE_FIRES_FILEPATH = "./AFIMG_j01_d20210414_t1126439_e1128084_b17637_c20210414114130392094_cspp_dev.txt"
TEST_ACTIVE_FIRES_FILEPATH2 = "./AFIMG_npp_d20230616_t1110054_e1111296_b60284_c20230616112418557033_cspp_dev.txt"


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
  58.74638367,    8.54766846,  340.68481445,  0.375,  0.375,    8,   10.83046722
  55.34669113,   -4.51371527,  325.72799683,  0.375,  0.375,    8,    6.21815872
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

FAKE_MASK11 = np.array([True, True])
FAKE_MASK12 = np.array([False])


@patch('activefires_pp.post_processing.read_cspp_output_data')
def test_add_start_and_end_time_to_active_fires_data_utc(readdata, fake_active_fires_file_data):
    """Test adding start and end times to the active fires data."""
    open_fstream, myfilepath = fake_active_fires_file_data
    afdata = pd.read_csv(open_fstream, index_col=None, header=None, comment='#', names=COL_NAMES)

    readdata.return_value = afdata

    af_shpfile_filter = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='GMT')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        af_shpfile_filter.get_af_data(filepattern=AF_FILE_PATTERN, localtime=False)

    assert 'starttime' in af_shpfile_filter.afdata
    assert 'endtime' in af_shpfile_filter.afdata
    assert af_shpfile_filter.afdata['starttime'].shape == (18,)

    assert str(af_shpfile_filter.afdata['starttime'][0]) == '2021-04-14 11:26:43.900000'
    assert str(af_shpfile_filter.afdata['endtime'][0]) == '2021-04-14 11:28:08'


@patch('activefires_pp.post_processing.read_cspp_output_data')
def test_add_start_and_end_time_to_active_fires_data_localtime(readdata, fake_active_fires_file_data):
    """Test adding start and end times to the active fires data."""
    open_fstream, myfilepath = fake_active_fires_file_data

    afdata = pd.read_csv(open_fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    af_shpfile_filter = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Europe/Stockholm')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        af_shpfile_filter.get_af_data(filepattern=AF_FILE_PATTERN, localtime=True)

    assert 'starttime' in af_shpfile_filter.afdata
    assert 'endtime' in af_shpfile_filter.afdata

    assert str(af_shpfile_filter.afdata['starttime'][0]) == '2021-04-14 13:26:43.900000'
    assert str(af_shpfile_filter.afdata['endtime'][0]) == '2021-04-14 13:28:08'

    af_shpfile_filter = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Iceland')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        af_shpfile_filter.get_af_data(filepattern=AF_FILE_PATTERN, localtime=True)

    assert 'starttime' in af_shpfile_filter.afdata
    assert 'endtime' in af_shpfile_filter.afdata

    assert str(af_shpfile_filter.afdata['starttime'][0]) == '2021-04-14 11:26:43.900000'
    assert str(af_shpfile_filter.afdata['endtime'][0]) == '2021-04-14 11:28:08'

    af_shpfile_filter = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Europe/Helsinki')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        af_shpfile_filter.get_af_data(filepattern=AF_FILE_PATTERN, localtime=True)

    assert 'starttime' in af_shpfile_filter.afdata
    assert 'endtime' in af_shpfile_filter.afdata

    assert str(af_shpfile_filter.afdata['starttime'][0]) == '2021-04-14 14:26:43.900000'
    assert str(af_shpfile_filter.afdata['endtime'][0]) == '2021-04-14 14:28:08'

    af_shpfile_filter = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='Europe/Lisbon')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        af_shpfile_filter.get_af_data(filepattern=AF_FILE_PATTERN, localtime=True)

    assert 'starttime' in af_shpfile_filter.afdata
    assert 'endtime' in af_shpfile_filter.afdata

    assert str(af_shpfile_filter.afdata['starttime'][0]) == '2021-04-14 12:26:43.900000'
    assert str(af_shpfile_filter.afdata['endtime'][0]) == '2021-04-14 12:28:08'


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_get_output_filepath_from_projname(setup_comm, gethostname,
                                           fake_yamlconfig_file_post_processing):
    """Test getting the correct output file path from the projection name."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)

    fake_metadata = {'platform': 'j01',
                     'start_time': datetime(2021, 4, 14, 11, 26, 43, 900000),
                     'end_hour': datetime(1900, 1, 1, 11, 28, 8, 400000),
                     'orbit': '17637',
                     'processing_time': datetime(2021, 4, 14, 11, 41, 30, 392094),
                     'end_time': datetime(2021, 4, 14, 11, 28, 8)}

    outpath = afpp.get_output_filepath_from_projname('default', fake_metadata)
    assert outpath == '/path/where/the/filtered/results/will/be/stored/AFIMG_j01_d20210414_t112643.geojson'
    outpath = afpp.get_output_filepath_from_projname('sweref99', fake_metadata)
    assert outpath == '/path/where/the/filtered/results/will/be/stored/AFIMG_j01_d20210414_t112643_sweref99.geojson'

    with pytest.raises(KeyError) as exec_info:
        outpath = afpp.get_output_filepath_from_projname('some_other_projection_name', fake_metadata)

    expected = "'Projection name some_other_projection_name not supported in configuration!'"
    assert str(exec_info.value) == expected


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_regional_fires_filtering(setup_comm, gethostname,
                                  fake_active_fires_file_data,
                                  fake_yamlconfig_file_post_processing):
    """Test the regional fires filtering."""
    # FIXME! This test is too big/broad. Need for refactoring!
    open_fstream, _ = fake_active_fires_file_data

    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp._initialize_fire_detection_id()

    afdata = pd.read_csv(open_fstream, index_col=None, header=None, comment='#', names=COL_NAMES)

    starttime = datetime.fromisoformat('2021-04-14 11:26:43.900')
    endtime = datetime.fromisoformat('2021-04-14 11:28:08')

    afdata['starttime'] = np.repeat(starttime, len(afdata)).astype(np.datetime64)
    afdata['endtime'] = np.repeat(endtime, len(afdata)).astype(np.datetime64)

    afdata = afpp.add_unique_day_id(afdata)

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
            generate_msg.return_value = "my fake output message"
            result = afpp.regional_fires_filtering_and_publishing(mymsg, regional_fmask, af_shapeff)

    store_geojson.assert_called_once()
    generate_msg.assert_called_once()
    assert len(result) == 1
    assert result[0] == "my fake output message"


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.get_global_mask_from_shapefile', side_effect=[FAKE_MASK1, FAKE_MASK2])
def test_general_national_fires_filtering(get_global_mask, setup_comm,
                                          gethostname, fake_active_fires_file_data,
                                          fake_yamlconfig_file_post_processing):
    """Test the general/basic national fires filtering."""
    open_fstream, _ = fake_active_fires_file_data
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afdata = pd.read_csv(open_fstream, index_col=None, header=None, comment='#', names=COL_NAMES)

    # Add metadata to the pandas dataframe:
    fake_metadata = {'platform': 'j01',
                     'start_time': datetime(2021, 4, 14, 11, 26, 43, 900000),
                     'end_hour': datetime(1900, 1, 1, 11, 28, 8, 400000),
                     'orbit': '17637',
                     'processing_time': datetime(2021, 4, 14, 11, 41, 30, 392094),
                     'end_time': datetime(2021, 4, 14, 11, 28, 8)}
    afdata.attrs = fake_metadata

    af_shapeff = ActiveFiresShapefileFiltering(afdata=afdata, platform_name='NOAA-20')
    afdata = af_shapeff.get_af_data(AF_FILE_PATTERN)

    mymsg = "Fake message"
    result = afpp.fires_filtering(mymsg, af_shapeff)

    assert get_global_mask.call_count == 2

    assert isinstance(result, pd.core.frame.DataFrame)
    assert len(result) == 1
    np.testing.assert_almost_equal(result.iloc[0]['latitude'], 59.52483368)
    np.testing.assert_almost_equal(result.iloc[0]['longitude'], 17.1681633)


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.get_global_mask_from_shapefile', side_effect=[FAKE_MASK11, FAKE_MASK12])
def test_general_national_fires_filtering_spurious_detections(get_global_mask, setup_comm,
                                                              gethostname, fake_active_fires_ascii_file5,
                                                              fake_yamlconfig_file_post_processing,
                                                              caplog):
    """Test the general/basic national fires filtering - here with one spurious detection (caused by SEU onboard)."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    af_shpfile_filter = ActiveFiresShapefileFiltering(filepath=fake_active_fires_ascii_file5, timezone='GMT')
    afdata = af_shpfile_filter.get_af_data(filepattern=AF_FILE_PATTERN, localtime=False)
    # Add metadata to the pandas dataframe:
    fake_metadata = {'platform': 'j02',
                     'start_time': datetime(2023, 12, 11, 1, 52, 44, 500000),
                     'end_hour': datetime(1900, 1, 1, 1, 54, 7, 400000),
                     'orbit': '5616',
                     'processing_time': datetime(2023, 12, 11, 2, 7, 10, 860273),
                     'end_time': datetime(2023, 12, 11, 1, 54, 7)}
    afdata.attrs = fake_metadata

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)

    af_shapeff = ActiveFiresShapefileFiltering(afdata=afdata, platform_name='NOAA-21')
    afdata = af_shapeff.get_af_data(AF_FILE_PATTERN)

    mymsg = "Fake message"
    with caplog.at_level(logging.INFO):
        result = afpp.fires_filtering(mymsg, af_shapeff)

    log_output1 = "Number of spurious detections filtered out = 1"
    assert log_output1 in caplog.text

    log_output2 = '(13.09044647,57.90747833): Tb4 = 324.07070923 FRP = 0.1102294'
    assert log_output2 in caplog.text

    assert len(result) == 1
    np.testing.assert_almost_equal(result.iloc[0]['latitude'], 60.17847443)
    np.testing.assert_almost_equal(result.iloc[0]['longitude'], -3.87098718)


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
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
def test_checking_national_borders_shapefile_file_nonexisting(setup_comm, gethostname,
                                                              fake_yamlconfig_file_post_processing):
    """Test the checking of the national borders shapefile - borders shapefile does not exist."""
    gethostname.return_value = "my.host.name"

    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    with pytest.raises(OSError) as exec_info:
        afpp._check_borders_shapes_exists()

    expected = "Shape file does not exist! Filename = /my/shape/file/with/country/borders"
    assert str(exec_info.value) == expected


@freeze_time('2023-06-16 11:24:00')
@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.read_cspp_output_data')
def test_get_feature_collection_from_firedata_with_detection_id(readdata, setup_comm, gethostname,
                                                                fake_yamlconfig_file_post_processing):
    """Test get the Geojson Feature Collection from fire detection."""
    gethostname.return_value = "my.host.name"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp._initialize_fire_detection_id()

    myfilepath = TEST_ACTIVE_FIRES_FILEPATH2

    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA2)
    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    af_shpfile_filter = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='GMT')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        afdata = af_shpfile_filter.get_af_data(filepattern=AF_FILE_PATTERN, localtime=False)

    afdata = afdata[2::]  # Reduce to only contain the last detections!
    afdata = afpp.add_unique_day_id(afdata)
    result = geojson_feature_collection_from_detections(afdata, platform_name='Suomi-NPP')

    # NB! The time of the afdata is here still in UTC!
    expected = FeatureCollection([{"geometry": {"coordinates": [17.259052, 62.658012],
                                                "type": "Point"},
                                   "properties": {"confidence": 8,
                                                  "observation_time": "2023-06-16T11:10:47.200000",
                                                  "platform_name": "Suomi-NPP",
                                                  "id": '20230616-1',
                                                  "power": 2.51202917, "tb": 339.66326904},
                                   "type": "Feature"},
                                  {"geometry": {"coordinates": [17.42075, 64.216942],
                                                "type": "Point"},
                                   "properties": {"confidence": 8,
                                                  "observation_time": "2023-06-16T11:10:47.200000",
                                                  "platform_name": "Suomi-NPP",
                                                  "id": '20230616-2',
                                                  "power": 3.39806151,
                                                  "tb": 329.65161133},
                                   "type": "Feature"},
                                  {"geometry": {"coordinates": [16.600952, 64.569046],
                                                "type": "Point"},
                                   "properties": {"confidence": 8,
                                                  "observation_time": "2023-06-16T11:10:47.200000",
                                                  "platform_name": "Suomi-NPP",
                                                  "id": '20230616-3',
                                                  "power": 20.5928936,
                                                  "tb": 346.52050781},
                                   "type": "Feature"},
                                  {"geometry": {"coordinates": [16.5984, 64.572227],
                                                "type": "Point"},
                                   "properties": {"confidence": 8,
                                                  "observation_time": "2023-06-16T11:10:47.200000",
                                                  "platform_name": "Suomi-NPP",
                                                  "id": '20230616-4',
                                                  "power": 20.5928936,
                                                  "tb": 348.72860718},
                                   "type": "Feature"}])

    TestCase().assertDictEqual(result, expected)


@patch('socket.gethostname')
@patch('activefires_pp.post_processing.ActiveFiresPostprocessing._setup_and_start_communication')
@patch('activefires_pp.post_processing.read_cspp_output_data')
def test_get_feature_collection_from_firedata_tb_celcius(readdata, setup_comm, gethostname,
                                                         fake_yamlconfig_file_post_processing):
    """Test get the Geojson Feature Collection from fire detection."""
    gethostname.return_value = "my.host.name"
    myborders_file = "/my/shape/file/with/country/borders"
    mymask_file = "/my/shape/file/with/polygons/to/filter/out"

    afpp = ActiveFiresPostprocessing(fake_yamlconfig_file_post_processing,
                                     myborders_file, mymask_file)
    afpp._initialize_fire_detection_id()

    units = {'temperature': 'degC'}
    afpp.unit_converter = UnitConverter(units)

    myfilepath = TEST_ACTIVE_FIRES_FILEPATH2
    fstream = io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA2)

    afdata = pd.read_csv(fstream, index_col=None, header=None, comment='#', names=COL_NAMES)
    readdata.return_value = afdata

    af_shpfile_filter = ActiveFiresShapefileFiltering(filepath=myfilepath, timezone='GMT')
    with patch('os.path.exists') as mypatch:
        mypatch.return_value = True
        afdata = af_shpfile_filter.get_af_data(filepattern=AF_FILE_PATTERN, localtime=False)

    afdata = afdata[2::]  # Reduce to only contain the last detections!

    afdata = afpp.add_tb_celcius(afdata)
    result = geojson_feature_collection_from_detections(afdata, platform_name='Suomi-NPP')

    # NB! The time of the afdata is here still in UTC!
    expected = FeatureCollection([{"geometry": {"coordinates": [17.259052, 62.658012],
                                                "type": "Point"},
                                   "properties": {
                                       "confidence": 8,
                                       "observation_time": "2023-06-16T11:10:47.200000",
                                       "platform_name": "Suomi-NPP",
                                       "power": 2.51202917,
                                       "tb": 339.66326904,
                                       "tb_celcius": 66.51326904000001},
                                   "type": "Feature"},
                                  {"geometry": {"coordinates": [17.42075, 64.216942],
                                                "type": "Point"},
                                   "properties": {
                                       "confidence": 8,
                                       "observation_time": "2023-06-16T11:10:47.200000",
                                       "platform_name": "Suomi-NPP",
                                       "power": 3.39806151,
                                       "tb": 329.65161133,
                                       "tb_celcius": 56.50161133},
                                   "type": "Feature"},
                                  {"geometry": {"coordinates": [16.600952, 64.569046],
                                                "type": "Point"},
                                   "properties": {
                                       "confidence": 8,
                                       "observation_time": "2023-06-16T11:10:47.200000",
                                       "platform_name": "Suomi-NPP",
                                       "power": 20.5928936,
                                       "tb": 346.52050781,
                                       "tb_celcius": 73.37050781000005},
                                   "type": "Feature"},
                                  {"geometry": {"coordinates": [16.5984, 64.572227],
                                                "type": "Point"},
                                   "properties": {"confidence": 8,
                                                  "observation_time": "2023-06-16T11:10:47.200000",
                                                  "platform_name": "Suomi-NPP",
                                                  "power": 20.5928936,
                                                  "tb": 348.72860718,
                                                  "tb_celcius": 75.57860718},
                                   "type": "Feature"}])

    TestCase().assertDictEqual(result, expected)


def test_read_cspp_ascii_output_old(fake_active_fires_ascii_file2):
    """Test read the CSPP AF ascii output from file - CSPP 1 format."""
    afdata = read_cspp_output_data(fake_active_fires_ascii_file2)

    expected = np.array([2.51202917, 3.39806151, 20.5928936, 20.5928936])
    result = afdata['power'].values
    np.testing.assert_allclose(result, expected)


def test_read_cspp_ascii_output_cspp21(fake_active_fires_ascii_file_cspp21_1):
    """Test read the CSPP AF ascii output from file - CSPP 2.1 format."""
    afdata = read_cspp_output_data(fake_active_fires_ascii_file_cspp21_1)

    expected = np.array([2.35572743,  2.70757842,  7.53941059,  7.53941059,  7.53941059,
                         4.89373064,  5.49030447,  4.26173353,  6.94581461,  7.20385265,
                         14.37703133,  2.31650853,  5.67659807, 15.50455284, 11.85416698,
                         6.91587162])
    result = afdata['power'].values
    np.testing.assert_allclose(result, expected)


def test_read_cspp_ascii_output_wrong_format(fake_active_fires_ascii_file_cspp21_2, caplog):
    """Test read the CSPP AF ascii output from file - wrong format."""
    with pytest.raises(CSPP_ASCII_FILE_FORMAT_ERROR) as exec_info:
        _ = read_cspp_output_data(fake_active_fires_ascii_file_cspp21_2)

    expected = "Unexpected number of data columns in file! 9 (should be either 7 or 8)"
    assert str(exec_info.value) == expected
