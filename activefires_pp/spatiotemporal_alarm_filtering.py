#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Adam Dybbroe

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

"""Spatial and temporal filtering of fire alarms.

The aim of this filtering is to avoid multiple alarms/alerts being forwarded to
the end user(s) of the 'same' fire. A vegetation fire may give rise to more
than one VIIRS pixel detection in space, and if it does not move new detections
at the same location may be triggered at the following overpasses. This
filtering will also avoid repetative alerts of a sustainable fire. We will use
thresholds in space and time to define when a fire is a new fire and when a
pixel-detection is not associated with the same fire. Currently the thresholds
are:

 * Time threshold: 6 hours
 * Spacial threshold: 800 meters

"""

import socket
from datetime import datetime
from urllib.parse import urlparse

import logging
import signal
from queue import Empty
from threading import Thread
from posttroll.listener import ListenerContainer
from posttroll.message import Message
from posttroll.publisher import NoisyPublisher
from pathlib import Path

from activefires_pp.utils import read_config
from activefires_pp.geojson_utils import read_geojson_data
from activefires_pp.api_posting import post_alarm


LOG = logging.getLogger(__name__)


class AlarmFilterRunner(Thread):
    """The Alarm-Filter runner class."""

    def __init__(self, configfile):
        """Initialize the AlarmFilterRunner class."""
        super().__init__()
        self.configfile = configfile
        self.options = {}

        config = read_config(self.configfile)
        self._set_options_from_config(config)

        self.input_topic = self.options['subscribe_topics'][0]
        LOG.debug("Input topic: %s", self.input_topic)

        self.output_topic = self.options['publish_topic']

        self.listener = None
        self.publisher = None
        self.loop = False
        self._setup_and_start_communication()

    def _setup_and_start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        LOG.debug("Input topic: %s", self.input_topic)
        self.listener = ListenerContainer(topics=[self.input_topic])
        self.publisher = NoisyPublisher("spatiotemporal_alarm_filtering")
        self.publisher.start()
        self.loop = True
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def _set_options_from_config(self, config):
        """From the configuration on disk set the option dictionary with all metadata for real-time processing."""

        for item in config:
            self.options[item] = config[item]

        if isinstance(self.options.get('subscribe_topics'), str):
            subscribe_topics = self.options.get('subscribe_topics').split(',')
            for item in subscribe_topics:
                if len(item) == 0:
                    subscribe_topics.remove(item)
            self.options['subscribe_topics'] = subscribe_topics

        if isinstance(self.options.get('publish_topics'), str):
            publish_topics = self.options.get('publish_topics').split(',')
            for item in publish_topics:
                if len(item) == 0:
                    publish_topics.remove(item)
            self.options['publish_topics'] = publish_topics

    def signal_shutdown(self, *args, **kwargs):
        """Shutdown the Notifier process."""
        self.close()

    def run(self):
        """Run the spatiotemporal alarm filtering."""
        while self.loop:
            try:
                msg = self.listener.output_queue.get(timeout=1)
                LOG.debug("Message: %s", str(msg.data))
            except Empty:
                continue
            else:
                if msg.type in ['info', ]:
                    # No fires detected - no notification to send:
                    LOG.info("Message type info: No fires detected - no notification to send.")
                    continue
                elif msg.type not in ['file', 'collection', 'dataset']:
                    LOG.debug("Message type not supported: %s", str(msg.type))
                    continue

                output_msg = self.spatio_temporal_alarm_filtering(msg)
                if output_msg:
                    LOG.debug("Sending message: %s", str(output_msg))
                    self.publisher.send(str(output_msg))
                else:
                    LOG.debug("No message to send")

    def spatio_temporal_alarm_filtering(self, msg):
        """Spatial and temporal filtering of the fire detections and create and post the alarm.

        Perform the spatial and temporal filtering of the current detections
        against a catalogue of past detections and generate an alarm point by
        creating a geoJSON file and posting to a ReST-API.

        """
        LOG.debug("Start the spatio-temporal alarm filtering.")

        url = urlparse(msg.data.get('uri'))
        LOG.info('File path: %s', str(url.path))
        filename = url.path

        ffdata = read_geojson_data(filename)
        if not ffdata:
            return None

        #platform_name = msg.data.get("platform_name")

        # Here we should implement the actual filtering!
        # FIXME!
        geojson_alarm = create_alarm_from_fire_detections(ffdata, past_detections_path)
        post_alarm(geojson_alarm, api_url)

    def close(self):
        """Shutdown the AlarmFilterRunner process."""
        LOG.info('Terminating the Alarm Filter Runner process.')
        self.loop = False
        try:
            self.listener.stop()
        except Exception:
            LOG.exception("Couldn't stop listener.")
        if self.publisher:
            try:
                self.publisher.stop()
            except Exception:
                LOG.exception("Couldn't stop publisher.")


def create_alarm_from_fire_detections(fire_data, past_detections_dir):
    """Create alarm(s) from a set of detections."""
    pass
