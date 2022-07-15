#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021, 2022 Adam Dybbroe

# Author(s):

#   Adam Dybbroe <Adam Dybbroe at smhi.se>

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

"""Unit testing the fire notifications.
"""

import unittest
from unittest.mock import patch
import yaml
import io
from posttroll.message import Message

from activefires_pp.fire_notifications import EndUserNotifier
from activefires_pp.fire_notifications import EndUserNotifierRegional
from activefires_pp.fire_notifications import get_recipients_for_region

TEST_CONFIG_FILE = "/home/a000680/usr/src/forks/activefires-pp/examples/fire_notifier.yaml"
TEST_CONFIG_FILE_REGIONAL = "/home/a000680/usr/src/forks/activefires-pp/examples/fire_notifier_regional.yaml"

NAT_CONFIG = """
# Publish/subscribe
publish_topic: VIIRS/L2/MSB/National
subscribe_topics: VIIRS/L2/Fires/PP/National

smtp_server: smtp.mydomain.se

domain: mydomain.se

sender: active-fires@mydomain.se

recipients: ["recipient1@recipients.se", "recipient2@recipients.se", "recipient3@recipients.se"]
recipients_attachment: ["recipient1@recipients.se", "recipient2@recipients.se"]
subject: "My subject"

max_number_of_fires_in_sms: 3

fire_data:
  - power
  - observation_time

unsubscribe:
  address: unsubscribe@mydomain.xx
  text: 'Stop being bothered: Send a note to unsubscribe@mydomain.xx'
"""

REG_CONFIG = """# Publish/subscribe
publish_topic: VIIRS/L2/MSB/Regional
subscribe_topics: VIIRS/L2/Fires/PP/Regional

smtp_server: smtp.mydomain.xx

domain: mydomain.xx

sender: active-fires@mydomain.xx

recipients:
  Area1-name:
    subject: "My subject"
    Kod_omr: '0999'
    name: 'Name of my area 1'
    recipients:
      - active-fires-sms-0999@mydomain.xx
    recipients_attachment: [active-fires-0999@mydomain.xx, ]

  Area2-name:
    subject: "My subject"
    Kod_omr: '0114'
    name: 'Name of my area 2'
    recipients: [active-fires-sms-0998@mydomain.xx, ]
    recipients_attachment: [active-fires-0998@mydomain.xx, ]

max_number_of_fires_in_sms: 3

fire_data:
  - power
  - observation_time

unsubscribe:
  address: unsubscribe@mydomain.xx
  text: 'Stop being bothered: Send a note to unsubscribe@mydomain.xx'
"""

REGIONAL_TEST_MESSAGE = """pytroll://VIIRS/L2/Fires/PP/Regional/0114 file safusr.u@lxserv1043.smhi.se 2021-04-19T11:16:49.542021 v1.01 application/json {"start_time": "2021-04-16T12:29:53", "end_time": "2021-04-16T12:31:18", "orbit_number": 1, "platform_name": "NOAA-20", "sensor": "viirs", "data_processing_level": "2", "variant": "DR", "orig_orbit_number": 17666, "region_name": "Storstockholms brandf\u00f6rsvar", "region_code": "0114", "uri": "ssh://lxserv1043.smhi.se//san1/polar_out/direct_readout/viirs_active_fires/filtered/AFIMG_NOAA-20_d20210416_t122953_0114.geojson", "uid": "AFIMG_NOAA-20_d20210416_t122953_0114.geojson", "type": "GEOJSON-filtered", "format": "geojson", "product": "afimg"}"""

NATIONAL_TEST_MESSAGE = """pytroll://VIIRS/L2/Fires/PP/National file safusr.u@lxserv1043.smhi.se 2021-04-19T11:16:49.519087 v1.01 application/json {"start_time": "2021-04-16T12:29:53", "end_time": "2021-04-16T12:31:18", "orbit_number": 1, "platform_name": "NOAA-20", "sensor": "viirs", "data_processing_level": "2", "variant": "DR", "orig_orbit_number": 17666, "uri": "ssh://lxserv1043.smhi.se//san1/polar_out/direct_readout/viirs_active_fires/filtered/AFIMG_j01_d20210416_t122953.geojson", "uid": "AFIMG_j01_d20210416_t122953.geojson", "type": "GEOJSON-filtered", "format": "geojson", "product": "afimg"}"""


class MyNetrcMock(object):

    def __init__(self):

        self.hosts = {'default': ('my_user', None, 'my_passwd')}

    def authenticators(self, host):
        return self.hosts.get(host)


class TestNotifyEndUsers(unittest.TestCase):

    @patch('activefires_pp.fire_notifications.netrc')
    @patch('activefires_pp.fire_notifications.socket.gethostname')
    @patch('activefires_pp.fire_notifications.read_config')
    @patch('activefires_pp.fire_notifications.EndUserNotifier._setup_and_start_communication')
    def test_get_options_national_filtering(self, setup_comm, read_config, gethostname, netrc):

        secrets = MyNetrcMock()
        netrc.return_value = secrets
        gethostname.return_value = 'default'

        myconfigfile = "/my/config/file/path"
        natstream = io.StringIO(NAT_CONFIG)

        read_config.return_value = yaml.load(natstream, Loader=yaml.UnsafeLoader)

        this = EndUserNotifier(myconfigfile)

        expected = {'publish_topic': 'VIIRS/L2/MSB/National',
                    'subscribe_topics': ['VIIRS/L2/Fires/PP/National'],
                    'smtp_server': 'smtp.mydomain.se', 'domain': 'mydomain.se', 'sender': 'active-fires@mydomain.se',
                    'recipients': ['recipient1@recipients.se', 'recipient2@recipients.se', 'recipient3@recipients.se'],
                    'recipients_attachment': ['recipient1@recipients.se', 'recipient2@recipients.se'],
                    'subject': 'My subject', 'max_number_of_fires_in_sms': 3,
                    'fire_data': ['power', 'observation_time'],
                    'unsubscribe': {'address': 'unsubscribe@mydomain.xx',
                                    'text': 'Stop being bothered: Send a note to unsubscribe@mydomain.xx'},
                    'unsubscribe_address': 'unsubscribe@mydomain.xx',
                    'unsubscribe_text': 'Stop being bothered: Send a note to unsubscribe@mydomain.xx'}

        self.assertDictEqual(expected, this.options)

        assert this.smtp_server == 'smtp.mydomain.se'
        assert this.domain == 'mydomain.se'
        assert this.sender == 'active-fires@mydomain.se'

        self.assertListEqual(this.recipients.recipients_all,
                             ['recipient1@recipients.se', 'recipient2@recipients.se', 'recipient3@recipients.se'])
        self.assertListEqual(this.recipients.recipients_with_attachment,
                             ['recipient1@recipients.se', 'recipient2@recipients.se'])
        self.assertListEqual(this.recipients.recipients_without_attachment, ['recipient3@recipients.se'])

        assert this.subject == 'My subject'
        assert this.max_number_of_fires_in_sms == 3
        self.assertListEqual(this.fire_data, ['power', 'observation_time'])
        assert this.unsubscribe_address == 'unsubscribe@mydomain.xx'
        assert this.input_topic == 'VIIRS/L2/Fires/PP/National'
        assert this.output_topic == 'VIIRS/L2/MSB/National'


class TestNotifyEndUsersRegional(unittest.TestCase):

    @patch('activefires_pp.fire_notifications.netrc')
    @patch('activefires_pp.fire_notifications.socket.gethostname')
    @patch('activefires_pp.fire_notifications.read_config')
    @patch('activefires_pp.fire_notifications.EndUserNotifierRegional._setup_and_start_communication')
    def test_get_options_regional_filtering(self, setup_comm, read_config, gethostname, netrc):

        secrets = MyNetrcMock()
        netrc.return_value = secrets
        gethostname.return_value = 'default'

        myconfigfile = "/my/config/file/path"
        regstream = io.StringIO(REG_CONFIG)

        read_config.return_value = yaml.load(regstream, Loader=yaml.UnsafeLoader)

        this = EndUserNotifierRegional(myconfigfile)

        expected = {'publish_topic': 'VIIRS/L2/MSB/Regional',
                    'subscribe_topics': ['VIIRS/L2/Fires/PP/Regional'],
                    'smtp_server': 'smtp.mydomain.xx', 'domain': 'mydomain.xx',
                    'sender': 'active-fires@mydomain.xx',
                    'recipients': {
                        'Area1-name': {'subject': 'My subject', 'Kod_omr': '0999', 'name': 'Name of my area 1',
                                       'recipients': ['active-fires-sms-0999@mydomain.xx'],
                                       'recipients_attachment': ['active-fires-0999@mydomain.xx']},
                        'Area2-name': {'subject': 'My subject', 'Kod_omr': '0114', 'name':
                                       'Name of my area 2', 'recipients': ['active-fires-sms-0998@mydomain.xx'],
                                       'recipients_attachment': ['active-fires-0998@mydomain.xx']}},
                    'max_number_of_fires_in_sms': 3, 'fire_data': ['power', 'observation_time'],
                    'unsubscribe': {'address': 'unsubscribe@mydomain.xx',
                                    'text': 'Stop being bothered: Send a note to unsubscribe@mydomain.xx'},
                    'unsubscribe_address': 'unsubscribe@mydomain.xx',
                    'unsubscribe_text': 'Stop being bothered: Send a note to unsubscribe@mydomain.xx'}

        self.assertDictEqual(expected, this.options)

        assert this.smtp_server == 'smtp.mydomain.xx'
        assert this.domain == 'mydomain.xx'
        assert this.sender == 'active-fires@mydomain.xx'

        # assert len(this.recipients) == 2
        # recipients = this.recipients['Area1-name']

        # expected = {'subject': 'My subject', 'knkod': '0999', 'name': 'Name of my area 1',
        #             'recipients': ['active-fires-sms-0999@mydomain.xx'],
        #             'recipients_attachment': ['active-fires-0999@mydomain.xx']}
        # self.assertDictEqual(expected, recipients)

        # recipients = this.recipients['Area2-name']
        # expected = {'subject': 'My subject', 'knkod': '0114', 'name': 'Name of my area 2',
        #             'recipients': ['active-fires-sms-0998@mydomain.xx'],
        #             'recipients_attachment': ['active-fires-0998@mydomain.xx']}
        # self.assertDictEqual(expected, recipients)

        # assert this.subject is None
        # assert this.max_number_of_fires_in_sms == 3
        # self.assertListEqual(this.fire_data, ['power', 'observation_time'])
        # assert this.unsubscribe_address == 'unsubscribe@mydomain.xx'
        # assert this.input_topic == 'VIIRS/L2/Fires/PP/Regional'
        # assert this.output_topic == 'VIIRS/L2/MSB/Regional'

        input_msg = Message.decode(rawstr=REGIONAL_TEST_MESSAGE)
        # this.notify_end_users(input_msg)

        # ffdata = {"features": [{"geometry": {"coordinates": [17.198933, 59.577972], "type": "Point"},
        #                        "properties": {"observation_time": "2021-04-16T14:30:35.900000",
        #                                       "platform_name": "NOAA-20", "power": 5.53501701, "tb": 367.0},
        #                        "type": "Feature"}], "type": "FeatureCollection"}

    @patch('activefires_pp.fire_notifications.netrc')
    @patch('activefires_pp.fire_notifications.socket.gethostname')
    @patch('activefires_pp.fire_notifications.read_config')
    @patch('activefires_pp.fire_notifications.EndUserNotifierRegional._setup_and_start_communication')
    def test_get_recipients_for_region(self, setup_comm, read_config, gethostname, netrc):
        """Test getting the recipients for a given region."""

        secrets = MyNetrcMock()
        netrc.return_value = secrets
        gethostname.return_value = 'default'

        myconfigfile = "/my/config/file/path"
        regstream = io.StringIO(REG_CONFIG)

        read_config.return_value = yaml.load(regstream, Loader=yaml.UnsafeLoader)

        this = EndUserNotifierRegional(myconfigfile)

        recipients = this.options.get('recipients')
        region_code = '0114'
        result = get_recipients_for_region(recipients, region_code)

        assert result.region_name == 'Name of my area 2'
        assert result.region_code == '0114'
