# Copyright (c) 2025 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
SMTP Helper classes
===================

A message class specialized to handle MIME encoded email messages.
This Module can be used to create a MIME message that have serveral
files attached to the mail.

This module defines the following classes:

- `SmtpMessage()`, a MIME encoded message
- `ConfiguredSmtpMessage()`, configure itself from provided Config instance

"""

from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
import os
import smtplib
from pydantic import BaseModel
# Default smtp timeout of 10 seconds.
DEFAULT_SMTP_TIMEOUT = 300


class SmtpMessage:
    """
    SMTP Message

    This class encapsulates an email message
    """

    def __init__(self, subject: str, sender: str, recipients: list[str],
                 cc: list[str] = None, body: str = "",
                 body_type: str = "text"):
        self.subject = subject
        self.sender = sender
        self.recipients = recipients
        self.cc = cc if cc else []
        self.body = body
        self.body_type = body_type
        self.files = []

    def get_mime_content(self):
        """Return the MIME text for the message."""
        if self.body.__class__.__name__ == "MIMEMultipart":
            msg = self.body
        else:
            if isinstance(self.body, str):
                msg = MIMEText(self.body)
            else:
                msg = MIMEMultipart()
                msg.attach(MIMEText(self.body, self.body_type))

        msg['From'] = self.sender
        msg['To'] = COMMASPACE.join(self.recipients)
        if len(self.cc) > 0:
            msg['Cc'] = COMMASPACE.join(self.cc)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = self.subject

        for filename in self.files:
            handle = open(filename, 'rb')
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(handle.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"'
                            % os.path.basename(filename))
            msg.attach(part)
            handle.close()

        return msg.as_string()


class SmtpClient:

    class Config(BaseModel):
        """Used to instantiate e.g. from config file"""
        server: str
        username: str = None
        password: str = None
        port: int = 25
        authenticate: bool = False
        use_tls: bool = False
        smtp_debug_level: int = 0

    def __init__(self, server, port: int = 25, username: str = None, password: str = None,
                 authenticate: bool = False, use_tls: bool = False,
                 smtp_debug_level: int = 0,
                 timeout: int = DEFAULT_SMTP_TIMEOUT, **kwargs):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.authenticate = False
        self.use_tls = use_tls
        self.smtp_debug_level = smtp_debug_level
        self.timeout = timeout

    def send(self, message: SmtpMessage):
        """Send the email"""
        smtp = smtplib.SMTP(self.server, self.port, timeout=self.timeout)
        recipients = message.recipients + message.cc
        if self.use_tls:
            if smtp.has_extn("STARTTLS"):
                smtp.starttls()
                smtp.ehlo()
                if self.authenticate:
                    smtp.login(self.username, self.password)
            else:
                if self.authenticate:
                    smtp.login(self.username, self.password)
        else:
            if self.authenticate:
                smtp.login(self.username, self.password)
        smtp.sendmail(message.sender, recipients, message.get_mime_content())
        smtp.close()
