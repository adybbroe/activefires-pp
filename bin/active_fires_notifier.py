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

"""Send mail/SMS notifications with Fire data to listed subscribers.

"""

import logging
import argparse
import sys
from activefires_pp.logger import setup_logging
from activefires_pp.fire_notifications import EndUserNotifier
from activefires_pp.fire_notifications import EndUserNotifierRegional

LOG = logging.getLogger('end_user_notifier_process')

#TEST_MESSAGE = """pytroll://VIIRS/L2/MSB/nrk/utv/polar/direct_readout dataset a000680@c21856.ad.smhi.se 2020-04-23T11:46:04.299244 v1.01 application/json {"start_time": "2020-04-23T11:02:01", "end_time": "2020-04-29T11:03:25", "orbit_number": 1, "platform_name": "NOAA-20", "sensor": "viirs", "format": "EDR", "type": "NETCDF", "data_processing_level": "2", "variant": "DR", "orig_orbit_number": 12586, "dataset": [{"uri": "file:///home/a000680/Satsa/Skogsbrander/AFIMG_j01_d20200423_t1102001_e1103246_b12586_c20200423112151310071_cspp_dev.geojson", "uid": "AFIMG_j01_d20200423_t1102001_e1103246_b12586_c20200423112151310071_cspp_dev.geojson"}]}"""
TEST_MESSAGE = """pytroll://VIIRS/L2/MSB/nrk/utv/polar/direct_readout dataset a000680@c21856.ad.smhi.se 2020-04-23T11:46:04.299244 v1.01 application/json {"start_time": "2020-04-23T11:02:01", "end_time": "2020-04-29T11:03:25", "orbit_number": 1, "platform_name": "NOAA-20", "sensor": "viirs", "format": "EDR", "type": "NETCDF", "data_processing_level": "2", "variant": "DR", "orig_orbit_number": 12586, "dataset": [{"uri": "file:///home/a000680/Satsa/Skogsbrander/AFIMG_j01_d20200423_t1102001_e1103246_b12586_c20200423112151310071_cspp_dev.geojson", "uid": "AFIMG_j01_d20200423_t1102001_e1103246_b12586_c20200423112151310071_cspp_dev.geojson"}]}"""


def main():
    """Start and run the dispatcher."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument("-c", "--config",
                        help="YAML config file to use.")
    parser.add_argument("-n", "--netrc",
                        help="Path to .netrc file to use.")
    parser.add_argument('-r', "--regional", action='store_true',
                        help="Regional notifier - default=False")
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    cmd_args = parser.parse_args()
    setup_logging(cmd_args)

    configfile = cmd_args.config
    netrcfile = cmd_args.netrc
    LOG.info("Starting up.")
    try:
        if cmd_args.regional:
            ffnotify = EndUserNotifierRegional(configfile, netrcfile=netrcfile)
        else:
            ffnotify = EndUserNotifier(configfile, netrcfile=netrcfile)

    except Exception as err:
        LOG.error('End User Notifier crashed: %s', str(err))
        sys.exit(1)
    try:
        ffnotify.start()
        ffnotify.join()
    except KeyboardInterrupt:
        LOG.debug("Interrupting")
    finally:
        ffnotify.close()


if __name__ == '__main__':
    main()
