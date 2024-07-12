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

"""Fixtures for unittests."""

import pytest
import io

TEST_YAML_CONFIG_CONTENT = """# Publish/subscribe
subscribe_topics: /VIIRS/L2/Fires/PP/National
publish_topic: /VIIRS/L2/Fires/PP/SOSAlarm

products:
  - afimg

geojson_file_pattern_alarms: sos_{start_time:%Y%m%d_%H%M%S}_{id:d}.geojson

fire_alarms_dir: /path/where/the/filtered/alarms/will/be/stored

restapi_url: "https://xxx.smhi.se:xxxx"

time_and_space_thresholds:
  hour_threshold: 6
  long_fires_threshold_km: 1.2
  spatial_threshold_km: 0.8
"""

TEST_POST_PROCESSING_YAML_CONFIG_CONTENT = """# Publish/subscribe
publish_topic: /VIIRS/L2/Fires/PP
subscribe_topics: VIIRS/L2/AFI

af_pattern_ibands: AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}_b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt

output:
  national:
    default:
      geojson_file_pattern: AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}.geojson
    sweref99:
      geojson_file_pattern: AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}_sweref99.geojson
      projection: "EPSG:3006"
  regional:
    default:
      geojson_file_pattern: AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S}_{region_name:s}.geojson


regional_shapefiles_format: omr_{region_code:s}_Buffer.{ext:s}

output_dir: /path/where/the/filtered/results/will/be/stored

timezone: Europe/Stockholm


"""  # noqa

TEST_YAML_TOKENS = """xauth_tokens:
  x-auth-satellite-alarm : 'my-token'
"""

# AFIMG_NOAA-20_20210619_005803_sweden.geojson
# Added anomaly indicator - 2024-07-12
TEST_GEOJSON_FILE_CONTENT_MONSTERAS = """{"type": "FeatureCollection", "features":
[{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.240452, 57.17329]},
"properties": {"power": 4.19946575, "tb": 336.38024902, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.247334, 57.172443]},
"properties": {"power": 5.85325146, "tb": 339.84768677, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.242519, 57.17498]},
"properties": {"power": 3.34151864, "tb": 316.57772827, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.249384, 57.174122]},
"properties": {"power": 3.34151864, "tb": 310.37808228, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.241102, 57.171574]},
"properties": {"power": 3.34151864, "tb": 339.86465454, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.247967, 57.170712]},
"properties": {"power": 3.34151864, "tb": 335.95074463, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.246538, 57.167309]},
"properties": {"power": 3.10640526, "tb": 337.62503052, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.239674, 57.168167]},
"properties": {"power": 3.10640526, "tb": 305.36495972, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245104, 57.163902]},
"properties": {"power": 3.10640526, "tb": 336.21279907, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.251965, 57.16304]},
"properties": {"power": 2.40693879, "tb": 306.66555786, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.250517, 57.159637]},
"properties": {"power": 2.23312426, "tb": 325.92211914, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.24366, 57.160496]},
"properties": {"power": 1.51176202, "tb": 317.16009521, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.242212, 57.157097]},
"properties": {"power": 1.51176202, "tb": 303.77804565, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.249069, 57.156235]},
"properties": {"power": 2.23312426, "tb": 310.37322998, "confidence": 8, "anomaly": 0,
"observation_time": "2021-06-19T02:58:45.700000+02:00", "platform_name": "NOAA-20"}}]}"""

TEST_GEOJSON_FILE_CONTENT = """{"type": "FeatureCollection", "features":
[{"type": "Feature", "geometry": {"type": "Point", "coordinates": [23.562864, 67.341919]},
"properties": {"power": 1.62920368, "tb": 325.2354126, "confidence": 8,
"observation_time": "2022-06-29T14:01:08.850000", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [23.56245, 67.347328]},
"properties": {"power": 3.40044808, "tb": 329.46963501, "confidence": 8,
"observation_time": "2022-06-29T14:01:08.850000", "platform_name": "NOAA-20"}},
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [23.555086, 67.343231]},
"properties": {"power": 6.81757641, "tb": 334.62347412, "confidence": 8,
"observation_time": "2022-06-29T14:01:08.850000", "platform_name": "NOAA-20"}}]}"""

# Past alarms:
PAST_ALARMS_MONSTERAS1 = """{"type": "FeatureCollection", "features":
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.246222, 57.175987]},
"properties": {"power": 1.83814871, "tb": 302.3949585, "confidence": 8,
"observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP", "related_detection": true}}}"""

PAST_ALARMS_MONSTERAS2 = """{"type": "FeatureCollection", "features":
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16.245516, 57.1651]},
"properties": {"power": 2.94999027, "tb": 324.5098877, "confidence": 8,
"observation_time": "2021-06-19T02:07:33.050000+02:00", "platform_name": "Suomi-NPP", "related_detection": true}}}"""

PAST_ALARMS_MONSTERAS3 = """{"features": {"geometry": {"coordinates": [16.252192, 57.15242], "type": "Point"},
"properties": {"confidence": 8, "observation_time": "2021-06-18T14:49:01.750000+02:00",
"platform_name": "NOAA-20", "related_detection": false, "power": 2.87395763, "tb": 330.10293579},
"type": "Feature"}, "type": "FeatureCollection"}"""


TEST_ACTIVE_FIRES_FILEPATH = "./AFIMG_j01_d20210414_t1126439_e1128084_b17637_c20210414114130392094_cspp_dev.txt"
TEST_ACTIVE_FIRES_FILEPATH2 = "./AFIMG_npp_d20230616_t1110054_e1111296_b60284_c20230616112418557033_cspp_dev.txt"
TEST_ACTIVE_FIRES_FILEPATH3 = "./AFIMG_j01_d20230617_t1140564_e1142209_b28903_c20230617115513873196_cspp_dev.txt"
TEST_ACTIVE_FIRES_FILEPATH4 = "./AFIMG_j01_d20230618_t0942269_e0943514_b28916_c20230618095604331171_cspp_dev.txt"
TEST_ACTIVE_FIRES_FILEPATH5 = "./AFIMG_j02_d20231211_t0152445_e0154074_b05616_c20231211020710860273_cspp_dev.txt"
TEST_ACTIVE_FIRES_FILEPATH_CSPP21_1 = "./AFIMG_j01_d20240712_t0720206_e0721452_b34447_c20240712073948953196_cspp_dev.txt"  # noqa
TEST_ACTIVE_FIRES_FILEPATH_CSPP21_2 = "./AFIMG_j01_d20240712_t0720206_e0721452_b34447_c20240712073948953196_cspp_dev_wrongformat.txt"  # noqa


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

# Here an example with one spurious detection, with high TB in I-band 4 and very low FRP:
TEST_ACTIVE_FIRES_FILE_DATA5 = """
# Active Fires I-band EDR
#
# source: AFIMG_j02_d20231211_t0152445_e0154074_b05616_c20231211020710860273_cspp_dev.nc
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
# number of fire pixels: 2
#
  60.17847443,   -3.87098718,  295.43579102,  0.375,  0.375,    8,    0.82296646
  57.90747833,   13.09044647,  324.07070923,  0.375,  0.375,    8,    0.11022940
"""

# Here an example with the new file format as of CSPP-2.1.0:
TEST_ACTIVE_FIRES_FILE_DATA_CSPP21_1 = """
# Active Fires I-band EDR
#
# source: AFIMG_j01_d20240712_t0720206_e0721452_b34447_c20240712073948953196_cspp_dev.nc
# version: CSPP Active Fires version: 2.1
#
# column 1: latitude of fire pixel (degrees)
# column 2: longitude of fire pixel (degrees)
# column 3: I04 brightness temperature of fire pixel (K)
# column 4: Along-scan fire pixel resolution (km)
# column 5: Along-track fire pixel resolution (km)
# column 6: detection confidence ([7,8,9]->[lo,med,hi])
# column 7: fire radiative power (MW)
# column 8: Persistent Anomaly
#           0 - none
#           1 - oil/gas
#           2 - volcano
#           3 - solar panel
#           4 - urban (not in use)
#           5 - unclassified
#
# number of fire pixels: 16
#
  64.26649475,   69.67490387,  330.21212769,  0.375,  0.375,    8,    2.35572743,    0
  64.27099609,   69.67951202,  325.39312744,  0.375,  0.375,    8,    2.70757842,    0
  64.26960754,   69.67189026,  343.07489014,  0.375,  0.375,    8,    7.53941059,    0
  64.26821899,   69.66426849,  355.50091553,  0.375,  0.375,    8,    7.53941059,    0
  64.27273560,   69.66888428,  337.91378784,  0.375,  0.375,    8,    7.53941059,    0
  64.55986023,   69.31879425,  333.13845825,  0.375,  0.375,    8,    4.89373064,    0
  64.55844879,   69.31111145,  326.01187134,  0.375,  0.375,    8,    5.49030447,    0
  66.93852234,   80.74044037,  332.61676025,  0.375,  0.375,    8,    4.26173353,    1
  66.67069244,   77.28248596,  329.25610352,  0.375,  0.375,    8,    6.94581461,    1
  67.27344513,   83.20786285,  341.00994873,  0.375,  0.375,    8,    7.20385265,    0
  67.28264618,   83.04086304,  339.51040649,  0.375,  0.375,    8,   14.37703133,    0
  67.33503723,   83.13152313,  334.20611572,  0.375,  0.375,    8,    2.31650853,    0
  66.19812775,   70.99553680,  331.75051880,  0.375,  0.375,    8,    5.67659807,    1
  67.59332275,   83.24843597,  352.14266968,  0.375,  0.375,    8,   15.50455284,    0
  67.64979553,   83.21275330,  341.54864502,  0.375,  0.375,    8,   11.85416698,    0
  67.68718719,   81.37524414,  330.09078979,  0.375,  0.375,    8,    6.91587162,    0
"""


# Here an example with the new file format as of CSPP-2.1.0, but with an erroneous extra column:
TEST_ACTIVE_FIRES_FILE_DATA_CSPP21_2 = """
# Active Fires I-band EDR
#
# source: AFIMG_j01_d20240712_t0720206_e0721452_b34447_c20240712073948953196_cspp_dev.nc
# version: CSPP Active Fires version: 2.1
#
# column 1: latitude of fire pixel (degrees)
# column 2: longitude of fire pixel (degrees)
# column 3: I04 brightness temperature of fire pixel (K)
# column 4: Along-scan fire pixel resolution (km)
# column 5: Along-track fire pixel resolution (km)
# column 6: detection confidence ([7,8,9]->[lo,med,hi])
# column 7: fire radiative power (MW)
# column 8: Persistent Anomaly
#           0 - none
#           1 - oil/gas
#           2 - volcano
#           3 - solar panel
#           4 - urban (not in use)
#           5 - unclassified
#
# number of fire pixels: 2
#
  64.26649475,   69.67490387,  330.21212769,  0.375,  0.375,    8,    2.35572743,    0, 99
  64.27099609,   69.67951202,  325.39312744,  0.375,  0.375,    8,    2.70757842,    0, 99
"""


@pytest.fixture
def fake_active_fires_file_data():
    """Fake active fires output in a file - return an open stream with data and the filepath."""
    return io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA), TEST_ACTIVE_FIRES_FILEPATH


@pytest.fixture
def fake_active_fires_file_data2():
    """Fake active fires output in a file - return an open stream with data and the filepath."""
    return io.StringIO(TEST_ACTIVE_FIRES_FILE_DATA2), TEST_ACTIVE_FIRES_FILEPATH2


@pytest.fixture
def fake_active_fires_ascii_file2(tmp_path):
    """Create a fake active fires ascii file."""
    file_path = tmp_path / TEST_ACTIVE_FIRES_FILEPATH2
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_ACTIVE_FIRES_FILE_DATA2)

    yield file_path


@pytest.fixture
def fake_active_fires_ascii_file3(tmp_path):
    """Create a fake active fires ascii file."""
    file_path = tmp_path / TEST_ACTIVE_FIRES_FILEPATH3
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_ACTIVE_FIRES_FILE_DATA3)

    yield file_path


@pytest.fixture
def fake_active_fires_ascii_file4(tmp_path):
    """Create a fake active fires ascii file."""
    file_path = tmp_path / TEST_ACTIVE_FIRES_FILEPATH4
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_ACTIVE_FIRES_FILE_DATA4)

    yield file_path


@pytest.fixture
def fake_active_fires_ascii_file5(tmp_path):
    """Create a fake active fires ascii file."""
    file_path = tmp_path / TEST_ACTIVE_FIRES_FILEPATH5
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_ACTIVE_FIRES_FILE_DATA5)

    yield file_path


@pytest.fixture
def fake_active_fires_ascii_file_cspp21_1(tmp_path):
    """Create a fake active fires ascii file."""
    file_path = tmp_path / TEST_ACTIVE_FIRES_FILEPATH_CSPP21_1
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_ACTIVE_FIRES_FILE_DATA_CSPP21_1)

    yield file_path


@pytest.fixture
def fake_active_fires_ascii_file_cspp21_2(tmp_path):
    """Create a fake active fires ascii file."""
    file_path = tmp_path / TEST_ACTIVE_FIRES_FILEPATH_CSPP21_2
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_ACTIVE_FIRES_FILE_DATA_CSPP21_2)

    yield file_path


@pytest.fixture
def fake_token_file(tmp_path):
    """Write fake token file."""
    file_path = tmp_path / '.sometokenfile.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_TOKENS)

    yield file_path


@pytest.fixture
def fake_detection_id_cache_file(tmp_path):
    """Write fake detection-id cache file."""
    file_path = tmp_path / 'fire_detection_id_cache.txt'
    with open(file_path, 'w') as fpt:
        fpt.write('20230501-1')

    yield file_path


@pytest.fixture
def fake_yamlconfig_file(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / 'test_alarm_filtering_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT)

    yield file_path


@pytest.fixture
def fake_yamlconfig_file_post_processing(tmp_path):
    """Write fake yaml config file - with no id cache file."""
    file_path = tmp_path / 'test_af_post_processing_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_POST_PROCESSING_YAML_CONFIG_CONTENT)

    yield file_path


@pytest.fixture
def fake_yamlconfig_file_post_processing_with_id_cache(tmp_path, fake_detection_id_cache_file):
    """Write fake yaml config file - with a realistic id-cache file."""
    file_path = tmp_path / 'test_af_post_processing_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_POST_PROCESSING_YAML_CONFIG_CONTENT)
        fpt.write('filepath_detection_id_cache: ' + str(fake_detection_id_cache_file))

    yield file_path


@pytest.fixture
def fake_geojson_file_many_detections(tmp_path):
    """Write fake geojson file with many close detections."""
    file_path = tmp_path / 'test_afimg_NOAA-20_20210619_005803_sweden.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_GEOJSON_FILE_CONTENT_MONSTERAS)

    yield file_path


@pytest.fixture
def fake_geojson_file(tmp_path):
    """Write fake geojson file."""
    file_path = tmp_path / 'test_afimg_20220629_120026.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_GEOJSON_FILE_CONTENT)

    yield file_path


@pytest.fixture
def fake_past_detections_dir(tmp_path):
    """Create fake directory with past detections."""
    past_detections_dir = tmp_path / 'past_detections'
    past_detections_dir.mkdir()
    file_path = past_detections_dir / 'sos_20210619_000651_0.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(PAST_ALARMS_MONSTERAS1)

    file_path = past_detections_dir / 'sos_20210619_000651_1.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(PAST_ALARMS_MONSTERAS2)

    file_path = past_detections_dir / 'sos_20210618_124819_0.geojson'
    with open(file_path, 'w') as fpt:
        fpt.write(PAST_ALARMS_MONSTERAS2)

    yield file_path.parent


@pytest.fixture
def fake_national_borders_shapefile(tmp_path):
    """Write fake national borders shape file."""
    file_path = tmp_path / 'some_national_borders_shape.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write('')

    yield file_path
