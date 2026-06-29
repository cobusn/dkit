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
This Module can be used to create a MIME message that have several
files attached to the mail.

This module defines the following classes:

- `SmtpMessage()`, a MIME encoded message
- `SmtpClient()`, sends an SmtpMessage via a configured SMTP server

"""

from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
import logging
import os
import smtplib
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Default smtp timeout of 300 seconds (5 minutes).
DEFAULT_SMTP_TIMEOUT = 300


class SmtpMessage:
    """MIME-encoded email message.

    Attach files by appending paths to the `files` list after construction.
    When `html_body` is supplied the message is sent as multipart/alternative
    with a plain-text fallback, which is the recommended format for HTML mail.
    """

    class Config(BaseModel):
        """Pydantic config schema, suitable for loading from a config file.

        Example:
            cfg = SmtpMessage.Config.model_validate(yaml_dict)
            msg = SmtpMessage(**cfg.model_dump())
        """

        subject: str
        sender: str
        recipients: list[str]
        cc: list[str] = []
        body: str = ""
        body_type: str = "plain"
        html_body: str | None = None
        files: list[str] = []

    def __init__(self, subject: str, sender: str, recipients: list[str],
                 cc: list[str] | None = None, body: str = "",
                 body_type: str = "plain", html_body: str | None = None):
        """Initialise an SMTP message.

        Args:
            subject: Email subject line.
            sender: RFC 5321 envelope sender address.
            recipients: List of primary recipient addresses.
            cc: Optional list of CC addresses.
            body: Plain-text message body.
            body_type: MIME subtype for the plain body (default: "plain").
            html_body: Optional HTML body. When set, the message is assembled
                as multipart/alternative containing both the plain and HTML
                parts so that clients without HTML support fall back gracefully.
        """
        self.subject = subject
        self.sender = sender
        self.recipients = recipients
        self.cc = cc if cc else []
        self.body = body
        self.body_type = body_type
        self.html_body = html_body
        self.files = []

    def _build_body_part(self):
        """Construct the body MIME part.

        Returns:
            A MIMEMultipart("alternative") when html_body is set, a pre-built
            MIMEMultipart when body is already a MIMEMultipart instance, or a
            plain MIMEText for string bodies.
        """
        if self.html_body is not None:
            logger.debug("building multipart/alternative body with plain and html parts")
            alternative = MIMEMultipart("alternative")
            alternative.attach(MIMEText(self.body, "plain"))
            alternative.attach(MIMEText(self.html_body, "html"))
            return alternative
        if isinstance(self.body, MIMEMultipart):
            logger.debug("using pre-built MIMEMultipart body")
            return self.body
        logger.debug("building plain text body")
        return MIMEText(self.body, self.body_type)

    def get_mime_content(self):
        """Assemble and return the complete MIME message as a string.

        Returns:
            A fully-formed RFC 2822 message string ready for transmission.
        """
        if self.files:
            logger.debug("building multipart message with %d attachment(s)", len(self.files))
            msg = MIMEMultipart("mixed")
            msg.attach(self._build_body_part())
        else:
            msg = self._build_body_part()

        msg['From'] = self.sender
        msg['To'] = COMMASPACE.join(self.recipients)
        if len(self.cc) > 0:
            msg['Cc'] = COMMASPACE.join(self.cc)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = self.subject

        for filename in self.files:
            logger.debug("attaching file: %s", filename)
            with open(filename, 'rb') as handle:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(handle.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename="{os.path.basename(filename)}"'
            )
            msg.attach(part)

        return msg.as_string()


class SmtpClient:
    """SMTP client that delivers SmtpMessage instances via a configured server."""

    class Config(BaseModel):
        """Pydantic config schema, suitable for loading from a config file."""

        server: str
        username: str | None = None
        password: str | None = None
        port: int = 25
        authenticate: bool = False
        use_tls: bool = False
        smtp_debug_level: int = 0

    def __init__(self, server: str, port: int = 25,
                 username: str | None = None, password: str | None = None,
                 authenticate: bool = False, use_tls: bool = False,
                 smtp_debug_level: int = 0,
                 timeout: int = DEFAULT_SMTP_TIMEOUT, **kwargs):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.authenticate = authenticate
        self.use_tls = use_tls
        self.smtp_debug_level = smtp_debug_level
        self.timeout = timeout

    def send(self, message: SmtpMessage):
        """Send the email.

        Args:
            message: The SmtpMessage to transmit.
        """
        logger.debug("connecting to smtp server %s:%d", self.server, self.port)
        recipients = message.recipients + message.cc
        with smtplib.SMTP(self.server, self.port, timeout=self.timeout) as smtp:
            if self.use_tls:
                if smtp.has_extn("STARTTLS"):
                    logger.debug("starting tls")
                    smtp.starttls()
                    smtp.ehlo()
                if self.authenticate:
                    logger.debug("authenticating as %s", self.username)
                    smtp.login(self.username, self.password)
            elif self.authenticate:
                logger.debug("authenticating as %s", self.username)
                smtp.login(self.username, self.password)
            smtp.sendmail(message.sender, recipients, message.get_mime_content())
        logger.info(
            "sent message '%s' from %s to %s",
            message.subject, message.sender, COMMASPACE.join(recipients)
        )
