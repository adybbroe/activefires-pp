#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 - 2026 Adam.Dybbroe

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

"""Test read an AF EDR (txt file) and perform national and urban mask filtering."""


import argparse

from activefires_pp.post_processing import store_geojson
from activefires_pp.post_processing import ActiveFiresShapefileFiltering
from activefires_pp.geojson_utils import geojson_feature_collection_from_detections


TESTFILE = "./AFIMG_j01_d20210414_t1126439_e1128084_b17637_c20210414114130392094_cspp_dev.txt"

INFILE_PATTERN = 'AFIMG_{platform:s}_d{start_time:%Y%m%d_t%H%M%S%f}_e{end_hour:%H%M%S%f}_b{orbit:s}_c{processing_time:%Y%m%d%H%M%S%f}_cspp_dev.txt'  # noqa


def get_arguments():
    """Get the command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--shp_boarders",
                        help="Path to shapefile with national boarders",
                        required=True)
    parser.add_argument("-f", "--shp_filtermask",
                        help="Path to shapefile with mask to filter false alarms",
                        required=True)

    return parser.parse_args()


if __name__ == "__main__":

    cmd_args = get_arguments()

    af_shapeff = ActiveFiresShapefileFiltering(TESTFILE)
    afdata = af_shapeff.get_af_data(INFILE_PATTERN)

    af_shapeff.fires_shapefile_filtering(cmd_args.shp_boarders)
    afdata_sweden = af_shapeff.get_af_data()

    af_shapeff.fires_shapefile_filtering(cmd_args.shp_filtermask, start_geometries_index=0, inside=False)
    afdata_ff = af_shapeff.get_af_data()

    feature_collection = geojson_feature_collection_from_detections(afdata_ff,
                                                                    platform_name=af_shapeff.platform_name)

    out_filepath = './test_output.geojson'
    store_geojson(out_filepath, feature_collection)
