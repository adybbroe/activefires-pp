#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam Dybbroe

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
from unittest.mock import mock_open
from unittest.mock import Mock

import yaml
import numpy as np
import io
from datetime import datetime

from activefires_pp.fire_notifications import EndUserNotifier

TEST_CONFIG_FILE = "/home/a000680/usr/src/forks/activefires-pp/examples/fire_notifier.yaml"
TEST_CONFIG_FILE_REGIONAL = "/home/a000680/usr/src/forks/activefires-pp/examples/fire_notifier_regional.yaml"

NAT_CONFIG = """
# Publish/subscribe
publish_topic: VIIRS/L2/MSB/National
subscribe_topics: VIIRS/L2/Fires/PP/National

smtp_server: smtp.mydomain.se

domain: mydomain.se

sender: active-fires@mydomain.se

recipients: ["list-of-mail-adresses@recipients.se"]
recipients_attachment: ["list-of-mail-adresses@recipients-who-wants-jsonfiles-in-attachments.se"]
subject: "My subject"

max_number_of_fires_in_sms: 3

fire_data:
  - power
  - observation_time

unsubscribe: adress-to-send-unsubscribe-message@mydomain.se
"""

REG_CONFIG = """# Publish/subscribe
publish_topic: VIIRS/L2/MSB/Regional
subscribe_topics: VIIRS/L2/Fires/PP/Regional

smtp_server: smtp.mydomain.xx

domain: mydomain.xx

sender: active-fires@mydomain.xx

recipients:
  - Area1-name:
      subject: "My subject"
      knkod: '0999'
      name: 'Name of my area 1'
      recipient: active-fires-sms-0999@mydomain.xx
      recipients_attachment: active-fires-0999@mydomain.xx

  - Area2-name:
      subject: "My subject"
      knkod: '0998'
      name: 'Name of my area 2'
      recipient: active-fires-sms-0998@mydomain.xx
      recipients_attachment: active-fires-0998@mydomain.xx

max_number_of_fires_in_sms: 3

fire_data:
  - power
  - observation_time

unsubscribe: adress-to-send-unsubscribe-message@mydomain.xx
"""


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

        expected = {'publish_topic': 'VIIRS/L2/MSB/National', 'subscribe_topics': ['VIIRS/L2/Fires/PP/National'],
                    'smtp_server': 'smtp.mydomain.se', 'domain': 'mydomain.se',
                    'sender': 'active-fires@mydomain.se', 'recipients': ['list-of-mail-adresses@recipients.se'],
                    'recipients_attachment': ['list-of-mail-adresses@recipients-who-wants-jsonfiles-in-attachments.se'],
                    'subject': 'My subject', 'max_number_of_fires_in_sms': 3,
                    'fire_data': ['power', 'observation_time'],
                    'unsubscribe': 'adress-to-send-unsubscribe-message@mydomain.se'}

        self.assertDictEqual(expected, this.options)

        assert this.smtp_server == 'smtp.mydomain.se'
        assert this.domain == 'mydomain.se'
        assert this.sender == 'active-fires@mydomain.se'
        self.assertListEqual(this.recipients,
                             ['list-of-mail-adresses@recipients.se'])
        self.assertListEqual(this.recipients_attachment,
                             ['list-of-mail-adresses@recipients-who-wants-jsonfiles-in-attachments.se'])
        assert this.subject == 'My subject'
        assert this.max_number_of_fires_in_sms == 3
        self.assertListEqual(this.fire_data, ['power', 'observation_time'])
        assert this.unsubscribe_address == 'adress-to-send-unsubscribe-message@mydomain.se'
        assert this.input_topic == 'VIIRS/L2/Fires/PP/National'
        assert this.output_topic == 'VIIRS/L2/MSB/National'

    @patch('activefires_pp.fire_notifications.netrc')
    @patch('activefires_pp.fire_notifications.socket.gethostname')
    @patch('activefires_pp.fire_notifications.read_config')
    @patch('activefires_pp.fire_notifications.EndUserNotifier._setup_and_start_communication')
    def test_get_options_regional_filtering(self, setup_comm, read_config, gethostname, netrc):

        secrets = MyNetrcMock()
        netrc.return_value = secrets
        gethostname.return_value = 'default'

        myconfigfile = "/my/config/file/path"
        regstream = io.StringIO(REG_CONFIG)

        read_config.return_value = yaml.load(regstream, Loader=yaml.UnsafeLoader)

        this = EndUserNotifier(myconfigfile)

        expected = {'publish_topic': 'VIIRS/L2/MSB/Regional',
                    'subscribe_topics': ['VIIRS/L2/Fires/PP/Regional'],
                    'smtp_server': 'smtp.mydomain.xx', 'domain': 'mydomain.xx',
                    'sender': 'active-fires@mydomain.xx',
                    'recipients': [
                        {'Area1-name': {'subject': 'My subject', 'knkod': '0999', 'name': 'Name of my area 1',
                                        'recipient': 'active-fires-sms-0999@mydomain.xx',
                                        'recipients_attachment': 'active-fires-0999@mydomain.xx'}},
                        {'Area2-name': {'subject': 'My subject', 'knkod': '0998', 'name': 'Name of my area 2',
                                        'recipient': 'active-fires-sms-0998@mydomain.xx',
                                        'recipients_attachment': 'active-fires-0998@mydomain.xx'}}],
                    'max_number_of_fires_in_sms': 3, 'fire_data': ['power', 'observation_time'],
                    'unsubscribe': 'adress-to-send-unsubscribe-message@mydomain.xx'}

        self.assertDictEqual(expected, this.options)

        assert this.smtp_server == 'smtp.mydomain.xx'
        assert this.domain == 'mydomain.xx'
        assert this.sender == 'active-fires@mydomain.xx'

        assert len(this.recipients) == 2
        recipients = this.recipients[0]['Area1-name']
        expected = {'subject': 'My subject', 'knkod': '0999', 'name': 'Name of my area 1',
                    'recipient': 'active-fires-sms-0999@mydomain.xx',
                    'recipients_attachment': 'active-fires-0999@mydomain.xx'}
        self.assertDictEqual(expected, recipients)

        recipients = this.recipients[1]['Area2-name']
        expected = {'subject': 'My subject', 'knkod': '0998', 'name': 'Name of my area 2',
                    'recipient': 'active-fires-sms-0998@mydomain.xx',
                    'recipients_attachment': 'active-fires-0998@mydomain.xx'}
        self.assertDictEqual(expected, recipients)

        assert this.subject is None
        assert this.max_number_of_fires_in_sms == 3
        self.assertListEqual(this.fire_data, ['power', 'observation_time'])
        assert this.unsubscribe_address == 'adress-to-send-unsubscribe-message@mydomain.xx'
        assert this.input_topic == 'VIIRS/L2/Fires/PP/Regional'
        assert this.output_topic == 'VIIRS/L2/MSB/Regional'
