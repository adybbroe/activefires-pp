#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam Dybbroe

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

import socket
from netrc import netrc
from datetime import datetime
import os
from six.moves.urllib.parse import urlparse
import geojson

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

from activefires_pp.utils import read_config


HOME = os.environ.get('HOME')
if HOME:
    NETRCFILE = os.path.join(HOME, '.netrc')
else:
    NETRCFILE = None

LOG = logging.getLogger(__name__)


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
        self.recipients = self.options.get('recipients')
        self.recipients_attachment = self.options.get('recipients_attachment')
        self.subject = self.options.get('subject')

        self.max_number_of_fires_in_sms = self.options.get('max_number_of_fires_in_sms', 2)
        LOG.debug("Max number of fires in SMS: %d", self.max_number_of_fires_in_sms)

        self.fire_data = self.options.get('fire_data')
        self.unsubscribe_address = self.options.get('unsubscribe')

        if not self.domain:
            raise IOError('Missing domain specification in config!')

        self.input_topic = self.options['subscribe_topics'][0]
        LOG.debug("Input topic: %s", self.input_topic)

        self.output_topic = self.options['publish_topic']

        self.listener = None
        self.publisher = None
        self.loop = False
        self._setup_and_start_communication()

    def _setup_and_start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        logger.debug("Input topic: %s", self.input_topic)
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

        host_secrets = self.secrets.authenticators(self.host)
        if host_secrets is None:
            LOG.error("Failed getting authentication secrets for host: %s", self.host)
            LOG.error("Check out the details in the netrc file: %s", self._netrcfile)
            return

        username, account, password = host_secrets

        server = smtplib.SMTP(self.smtp_server)
        server.starttls()
        server.ehlo(self.domain)
        recipients = list(set().union(self.recipients, self.recipients_attachment))
        recipients_attachment = []
        recipients_noattachment = []
        for recip in recipients:
            if recip in self.recipients_attachment:
                recipients_attachment.append(recip)
            else:
                recipients_noattachment.append(recip)

        server.rcpt(recipients)
        server.login(username, password)

        outmsg = None
        urlstr = msg.data.get('uri')
        url = urlparse(urlstr)

        LOG.info('File path: %s', str(url.path))
        platform_name = msg.data.get("platform_name")
        filename = url.path
        if filename.endswith('.geojson') and os.path.exists(filename):
            # Read the file:
            with open(filename, "r") as fpt:
                ffdata = geojson.load(fpt)
        else:
            LOG.warning("No filename to read: %s", filename)
            return None

        # Unsubscribe text:
        unsubscr = ""
        if self.unsubscribe_address:
            unsubscr = "\nSluta få detta meddelande: Mejla %s med subject=STOPP" % self.unsubscribe_address

        # Create the message(s).
        # Some recipients should have the full message and an attachment
        # Other recipients should have several smaller messages and no attachment
        #
        full_message, sub_messages = self.create_message_content(ffdata['features'], unsubscr)

        for submsg in sub_messages:
            notification = MIMEMultipart()
            notification['From'] = self.sender
            if platform_name:
                notification['Subject'] = self.subject + ' Satellit = %s' % platform_name
            else:
                notification['Subject'] = self.subject

            notification.attach(MIMEText(submsg, 'plain', 'UTF-8'))

            for recip in recipients_noattachment:
                notification['To'] = recip
                LOG.info("Send fire notification to %s", str(recip))
                LOG.debug("Subject: %s", str(self.subject))
                txt = notification.as_string()
                server.sendmail(self.sender, recip, txt)
                LOG.debug("Text sent: %s", txt)

        notification = MIMEMultipart()
        notification['From'] = self.sender
        if platform_name:
            notification['Subject'] = self.subject + ' Satellit = %s' % platform_name
        else:
            notification['Subject'] = self.subject

        notification.attach(MIMEText(full_message, 'plain', 'UTF-8'))
        LOG.debug("Length of message: %d", len(full_message))

        part = MIMEBase('application', "octet-stream")
        with open(filename, 'rb') as file:
            part.set_payload(file.read())
            encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(Path(filename).name))
        notification.attach(part)

        for recip in recipients_attachment:
            notification['To'] = recip
            LOG.info("Send fire notification to %s", str(recip))
            LOG.debug("Subject: %s", str(self.subject))
            txt = notification.as_string()
            server.sendmail(self.sender, recip, txt)
            LOG.debug("Text sent: %s", txt)

        server.quit()
        to_send = msg.data.copy()
        to_send.pop('file', None)
        to_send.pop('uri', None)
        to_send.pop('uid', None)
        to_send.pop('format', None)
        to_send.pop('type', None)
        to_send['info'] = "Notifications sent to the following recipients: %s" % str(recipients)
        outmsg = Message(self.output_topic, 'info', to_send)

        return outmsg

    def create_message_content(self, gjson_features, unsubscr):
        """Create the full message string and the list of sub-messages.

        """

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
            if 'observation_time' in self.fire_data and 'observation_time' in firespot['properties']:
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
