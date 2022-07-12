#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021-2022 Adam Dybbroe

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

"""
Main module to run the Active Fires Postprocessing in real time.


"""

import logging
import argparse
import sys
from activefires_pp.logger import setup_logging
from activefires_pp.post_processing import ActiveFiresPostprocessing


logger = logging.getLogger('active_fires_postprocessing')


def main():
    """Start and run the active-fires post-processing."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument("-c", "--config",
                        help="YAML config file to use.")
    parser.add_argument("-b", "--shp_boarders",
                        help="Path to shapefile with national boarders",
                        required=True)
    parser.add_argument("-f", "--shp_filtermask",
                        help="Path to shapefile with mask to filter false alarms",
                        required=True)
    parser.add_argument("-r", "--regional_shapefile",
                        help="Path to shapefile with mask to filter detection on regional areas",
                        default=None)
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    cmd_args = parser.parse_args()
    setup_logging(cmd_args)

    configfile = cmd_args.config
    national_map = cmd_args.shp_boarders
    filtermask = cmd_args.shp_filtermask
    regional_filtermask = cmd_args.regional_shapefile

    logger.info("Starting up.")
    try:
        if regional_filtermask:
            fire_pp = ActiveFiresPostprocessing(configfile, national_map, filtermask,
                                                regional_filtermask=regional_filtermask)
        else:
            fire_pp = ActiveFiresPostprocessing(configfile, national_map, filtermask)

    except Exception as err:
        logger.error('Active Fires postprocessing crashed: %s', str(err))
        sys.exit(1)
    try:
        fire_pp.start()
        fire_pp.join()
    except KeyboardInterrupt:
        logger.debug("Interrupting")
    finally:
        fire_pp.close()


if __name__ == "__main__":
    main()
