#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Adam.Dybbroe

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

"""
"""

import logging
import argparse
import sys
from activefires_pp.logger import setup_logging
from activefires_pp.spatiotemporal_alarm_filtering import AlarmFilterRunner

LOG = logging.getLogger('spatiotemporal_alarm_filtering')


def main():
    """Start and run the Spatio-temporal fire detection alarm filtering."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument("-c", "--config",
                        help="YAML config file to use.")
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    cmd_args = parser.parse_args()
    setup_logging(cmd_args)

    configfile = cmd_args.config
    LOG.info("Starting up.")
    try:
        ffnotify = AlarmFilterRunner(configfile)
    except Exception as err:
        LOG.error('Alarm Filter Runner crashed: %s', str(err))
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
