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

"""Creating and sending notifications for detected forest fires.
"""

import socket
from netrc import netrc
from datetime import datetime
import os
import logging
import signal
from queue import Empty
from threading import Thread
from posttroll.listener import ListenerContainer
from posttroll.message import Message
from posttroll.publisher import NoisyPublisher
import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

from activefires_pp.config import read_config
from activefires_pp.utils import get_filename_from_posttroll_message
from activefires_pp.geojson_utils import read_geojson_data


HOME = os.environ.get('HOME')
if HOME:
    NETRCFILE = os.path.join(HOME, '.netrc')
else:
    NETRCFILE = None

LOG = logging.getLogger(__name__)


class RecipientDataStruct(object):
    def __init__(self):

        self.recipients_with_attachment = []
        self.recipients_without_attachment = []
        self.recipients_all = []
        self.region_name = None
        self.region_code = None
        self.subject = None

    def _set_recipients(self, recipients, recipients_attachment):
        """Set the lists of recipients.

        One for those that should have an attachement (geojson file) and those without.
        """
        self.recipients_all = list(set().union(recipients, recipients_attachment))
        self.recipients_all.sort()
        for recip in self.recipients_all:
            if recip in recipients_attachment:
                self.recipients_with_attachment.append(recip)
            else:
                self.recipients_without_attachment.append(recip)


class EndUserNotifier(Thread):
    """The Notifier class - sending mails or text messages to end users upon incoming messages."""

    def __init__(self, configfile, netrcfile=NETRCFILE):
        """Initialize the EndUserNotifier class."""
        super().__init__()
        self.configfile = configfile
        self._netrcfile = netrcfile
        self.options = {}

        config = read_config(self.configfile)
        self._set_options_from_config(config)

        self.host = socket.gethostname()
        LOG.debug("netrc file path = %s", self._netrcfile)
        self.secrets = netrc(self._netrcfile)

        self.smtp_server = self.options.get('smtp_server')
        self.domain = self.options.get('domain')
        self.sender = self.options.get('sender')
        self.subject = self.options.get('subject')

        self.recipients = RecipientDataStruct()
        self._set_recipients()

        self.max_number_of_fires_in_sms = self.options.get('max_number_of_fires_in_sms', 2)
        LOG.debug("Max number of fires in SMS: %d", self.max_number_of_fires_in_sms)

        self.fire_data = self.options.get('fire_data')
        self.unsubscribe_address = self.options.get('unsubscribe_address')
        self.unsubscribe_text = self.options.get('unsubscribe_text')

        if not self.domain:
            raise IOError('Missing domain specification in config!')

        self.input_topic = self.options['subscribe_topics'][0]
        LOG.debug("Input topic: %s", self.input_topic)

        self.output_topic = self.options['publish_topic']

        self.listener = None
        self.publisher = None
        self.loop = False
        self._setup_and_start_communication()

    def _set_recipients(self):
        """Set the recipients lists."""
        self.recipients._set_recipients(self.options.get('recipients'), self.options.get('recipients_attachment'))
        self.recipients.subject = self.subject

    def _setup_and_start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        LOG.debug("Input topic: %s", self.input_topic)
        self.listener = ListenerContainer(topics=[self.input_topic])
        self.publisher = NoisyPublisher("end_user_notifier")
        self.publisher.start()
        self.loop = True
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def _set_options_from_config(self, config):
        """From the configuration on disk set the option dictionary, holding all metadata for processing."""

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

        unsubscribe = config.get('unsubscribe')
        if unsubscribe:
            for key in unsubscribe:
                self.options['unsubscribe_' + key] = unsubscribe[key]

    def signal_shutdown(self, *args, **kwargs):
        """Shutdown the Notifier process."""
        self.close()

    def run(self):
        """Run the Notifier."""
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

                output_msg = self.notify_end_users(msg)
                if output_msg:
                    LOG.debug("Sending message: %s", str(output_msg))
                    self.publisher.send(str(output_msg))
                else:
                    LOG.debug("No message to send")

    def notify_end_users(self, msg):
        """Send notifications to configured end users (mail and text messages)."""
        LOG.debug("Start sending notifications to configured end users.")

        filename = get_filename_from_posttroll_message(msg)
        ffdata = read_geojson_data(filename)
        if not ffdata:
            return None

        platform_name = msg.data.get("platform_name")

        # Create the message(s).
        # Some recipients (typically via e-mail) should have the full message and an attachment
        # Other recipients (typically via SMS) should have several smaller messages and no attachment
        #
        full_message, sub_messages = self.create_message_content(ffdata['features'], "\n" + self.unsubscribe_text)

        username, password = self._get_mailserver_login_credentials()
        server = self._start_smtp_server(username, password, self.recipients)

        self._send_notifications_without_attachments(server, self.recipients, sub_messages, platform_name)
        self._send_notifications_with_attachments(server, self.recipients, full_message, filename, platform_name)

        return _create_output_message(msg, self.output_topic, self.recipients.recipients_all)

    def _send_notifications_with_attachments(self, server, recipients, full_message, filename, platform_name):
        """Send notifications with attachments."""

        notification = MIMEMultipart()
        notification['From'] = self.sender
        if platform_name:
            notification['Subject'] = recipients.subject + ' Satellit = %s' % platform_name
        else:
            notification['Subject'] = recipients.subject

        if recipients.region_name:
            full_message = recipients.region_name + ":\n" + full_message

        notification.attach(MIMEText(full_message, 'plain', 'UTF-8'))
        LOG.debug("Length of message: %d", len(full_message))

        part = MIMEBase('application', "octet-stream")
        with open(filename, 'rb') as file:
            part.set_payload(file.read())
            encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(Path(filename).name))
        notification.attach(part)

        for recip in recipients.recipients_with_attachment:
            notification['To'] = recip
            LOG.info("Send fire notification to %s", str(recip))
            LOG.debug("Subject: %s", str(recipients.subject))
            txt = notification.as_string()
            server.sendmail(self.sender, recip, txt)
            LOG.debug("Text sent: %s", txt)

        server.quit()

    def _send_notifications_without_attachments(self, server, recipients, sub_messages, platform_name):
        """Send notifications without attachments."""

        for submsg in sub_messages:
            notification = MIMEMultipart()
            notification['From'] = self.sender
            if platform_name:
                notification['Subject'] = recipients.subject + ' Satellit = %s' % platform_name
            else:
                notification['Subject'] = recipients.subject

            notification.attach(MIMEText(submsg, 'plain', 'UTF-8'))

            for recip in recipients.recipients_without_attachment:
                notification['To'] = recip
                LOG.info("Send fire notification to %s", str(recip))
                LOG.debug("Subject: %s", str(recipients.subject))
                txt = notification.as_string()
                server.sendmail(self.sender, recip, txt)
                LOG.debug("Text sent: %s", txt)

    def _get_mailserver_login_credentials(self):
        """Get the login credentials for the mail server."""
        host_secrets = self.secrets.authenticators(self.host)
        if host_secrets is None:
            LOG.error("Failed getting authentication secrets for host: %s", self.host)
            raise IOError("Check out the details in the netrc file: %s", self._netrcfile)

        username, _, password = host_secrets

        return username, password

    def _start_smtp_server(self, username, password, recipients):
        """Start the smtp server and loging."""
        server = smtplib.SMTP(self.smtp_server)
        server.starttls()
        server.ehlo(self.domain)
        server.rcpt(recipients.recipients_all)
        server.login(username, password)

        return server

    def create_message_content(self, gjson_features, unsubscr):
        """Create the full message string and the list of sub-messages."""
        full_msg = ''
        msg_list = []
        outstr = ''
        for idx, firespot in enumerate(gjson_features):
            if idx % self.max_number_of_fires_in_sms == 0 and idx > 0:
                full_msg = full_msg + outstr
                if len(unsubscr) > 0:
                    outstr = outstr + unsubscr

                LOG.debug('%d: Sub message = <%s>', idx, outstr)
                msg_list.append(outstr)
                outstr = ''

            lonlats = firespot['geometry']['coordinates']
            outstr = outstr + '%f N, %f E\n' % (lonlats[1], lonlats[0])
            if ('observation_time' in self.fire_data and
                    'observation_time' in firespot['properties']):
                timestr = firespot['properties']['observation_time']
                LOG.debug("Time string: %s", str(timestr))
                try:
                    dtobj = datetime.fromisoformat(timestr)
                    # Python > 3.6
                except AttributeError:
                    dtobj = datetime.strptime(timestr.split('.')[0], '%Y-%m-%dT%H:%M:%S')

                outstr = outstr + '  %s\n' % dtobj.strftime('%d %b %H:%M')

            for prop in firespot['properties']:
                if prop in self.fire_data and prop not in ['observation_time']:
                    if prop in ['power', 'Power']:
                        outstr = outstr + '  FRP: %7.3f MW\n' % (firespot['properties'][prop])
                    else:
                        outstr = outstr + ' FRP: %s\n' % (str(firespot['properties'][prop]))

            LOG.debug("Message length so far: %d", len(outstr))
            LOG.debug("Max number of fires in sub message: %d", self.max_number_of_fires_in_sms)

        if len(outstr) > 0:
            if len(unsubscr) > 0:
                outstr = outstr + unsubscr
            LOG.debug('%d: Sub message = <%s>', idx, outstr)
            msg_list.append(outstr)

        full_msg = full_msg + outstr

        LOG.debug("Full message: <%s>", full_msg)
        LOG.debug("Sub-messages: <%s>", str(msg_list))

        return full_msg, msg_list

    def close(self):
        """Shutdown the Notifier process."""
        LOG.info('Terminating the End User Notifier process.')
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


class EndUserNotifierRegional(EndUserNotifier):
    """The Notifier class for regional notifications.

    Sending mails or text (SMS) messages to end users upon incoming messages.
    """

    def __init__(self, configfile, netrcfile=NETRCFILE):
        """Initialize the EndUserNotifierRegional class."""
        super(EndUserNotifierRegional, self).__init__(configfile, netrcfile=NETRCFILE)

    def _set_recipients(self):
        """Set the recipients lists."""
        self.recipients = self.options.get('recipients')

    def notify_end_users(self, msg):
        """Send notifications to configured end users (mail and text messages)."""
        LOG.debug("Start sending notifications to configured end users.")

        filename = get_filename_from_posttroll_message(msg)
        ffdata = read_geojson_data(filename)
        if not ffdata:
            return None

        platform_name = msg.data.get("platform_name")

        # Create the message(s).
        # Some recipients (typically via e-mail) should have the full message and an attachment
        # Other recipients (typically via SMS) should have several smaller messages and no attachment
        #
        full_message, sub_messages = self.create_message_content(ffdata['features'], "\n" + self.unsubscribe_text)

        region_code = msg.data.get("region_code")
        recipients = get_recipients_for_region(self.recipients, region_code)
        if not recipients:
            LOG.warning("No recipients configured for this region! Region code = ", str(region_code))
            return

        regional_output_topic = self.output_topic + '/' + recipients.region_code

        username, password = self._get_mailserver_login_credentials()
        server = self._start_smtp_server(username, password, recipients)

        self._send_notifications_without_attachments(server, recipients, sub_messages, platform_name)
        self._send_notifications_with_attachments(server, recipients, full_message, filename, platform_name)

        return _create_output_message(msg, regional_output_topic, recipients.recipients_all)


def _create_output_message(msg, topic, recipients):
    """Create the output message from the input message."""
    to_send = msg.data.copy()
    to_send.pop('file', None)
    to_send.pop('uri', None)
    to_send.pop('uid', None)
    to_send.pop('format', None)
    to_send.pop('type', None)
    to_send['info'] = "Notifications sent to the following recipients: %s" % str(recipients)

    return Message(topic, 'info', to_send)


def get_recipients_for_region(recipients, region_code):
    """Get the recipients lists applicable to the region."""
    for region_id in recipients:
        rcode = recipients[region_id]['Kod_omr']
        if rcode == region_code:
            recpt = RecipientDataStruct()
            recpt._set_recipients(recipients[region_id]['recipients'],
                                  recipients[region_id]['recipients_attachment'])
            recpt.region_name = recipients[region_id]['name']
            recpt.region_code = rcode
            recpt.subject = recipients[region_id]['subject']
            return recpt

    return None
