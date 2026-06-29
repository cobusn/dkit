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
- `DocumentMessage()`, a SmtpMessage that renders from a dkit Document
- `SmtpClient()`, sends an SmtpMessage via a configured SMTP server

"""

from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate, make_msgid
import logging
import mimetypes
import os
import smtplib
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Default smtp timeout of 300 seconds (5 minutes).
DEFAULT_SMTP_TIMEOUT = 300


class SmtpMessage:
    """MIME-encoded email message.

    Attach files by appending paths to the ``files`` list after construction.
    When ``html_body`` is supplied the message is sent as multipart/alternative
    with a plain-text fallback, which is the recommended format for HTML mail.

    Inline images (CID embedding) are supported for Outlook compatibility.
    Add image paths via ``add_inline_image()``, which returns the CID token to
    embed in the HTML as ``<img src="cid:<token>">``.  This is the only image
    format reliably displayed by Outlook without remote-image warnings.
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
        # list of (cid_token, file_path) for CID-embedded images
        self._inline_images: list[tuple[str, str]] = []

    def add_inline_image(self, file_path: str) -> str:
        """Register a local image file for CID embedding.

        The returned token should be used as the ``src`` attribute value in
        the HTML body: ``<img src="cid:<token>">``.  Outlook and most other
        clients display CID-embedded images without remote-image blocking.

        Args:
            file_path: path to the image file on disk.

        Returns:
            CID token string (without the ``cid:`` prefix).
        """
        cid = make_msgid(domain="dkit")[1:-1]  # strip surrounding < >
        self._inline_images.append((cid, file_path))
        logger.debug("registered inline image %s as cid:%s", file_path, cid)
        return cid

    def _build_body_part(self):
        """Construct the body MIME part.

        When inline images are registered the HTML part is wrapped in a
        multipart/related container so that CID references resolve correctly.

        Returns:
            A MIME part representing the message body.
        """
        if self.html_body is not None:
            logger.debug("building multipart/alternative body with plain and html parts")

            html_part = MIMEText(self.html_body, "html")

            if self._inline_images:
                logger.debug("wrapping html in multipart/related for %d inline image(s)",
                             len(self._inline_images))
                related = MIMEMultipart("related")
                related.attach(html_part)
                for cid, path in self._inline_images:
                    mime_type, _ = mimetypes.guess_type(path)
                    subtype = (mime_type or "image/png").split("/")[1]
                    with open(path, "rb") as fh:
                        img = MIMEImage(fh.read(), _subtype=subtype)
                    img.add_header("Content-ID", f"<{cid}>")
                    img.add_header("Content-Disposition", "inline",
                                   filename=os.path.basename(path))
                    related.attach(img)
                html_part = related

            alternative = MIMEMultipart("alternative")
            alternative.attach(MIMEText(self.body, "plain"))
            alternative.attach(html_part)
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


class DocumentMessage(SmtpMessage):
    """SmtpMessage that renders its body from a dkit Document.

    Plain-text and HTML bodies are generated automatically.  Local images
    in the document are attached as CID MIME parts so they display in
    Outlook without remote-image warnings.

    Args:
        subject: Email subject line.
        sender: RFC 5321 envelope sender address.
        recipients: List of primary recipient addresses.
        cc: Optional list of CC addresses.
        document: Source Document to render.
        css: Path to a ``.css`` file or a raw CSS string to apply to the
            HTML output.
    """

    def __init__(
        self,
        subject: str,
        sender: str,
        recipients: list[str],
        cc: list[str] | None = None,
        document=None,
        css: str | None = None,
    ):
        from ..doc2.html_renderer import HtmlRenderer
        from ..doc2.md_renderer import MarkdownRenderer
        from ..doc2 import document as doc_module

        text_body = MarkdownRenderer(document).render_string() if document else ""
        super().__init__(
            subject=subject,
            sender=sender,
            recipients=recipients,
            cc=cc,
            body=text_body,
        )
        if document is not None:
            html = HtmlRenderer(document, css=css).render_email_string()
            for element in document.elements:
                if isinstance(element, doc_module.Image):
                    if not element.source.startswith(("http://", "https://", "cid:")):
                        cid = self.add_inline_image(element.source)
                        html = html.replace(
                            f'src="{element.source}"',
                            f'src="cid:{cid}"',
                        )
            self.html_body = html
