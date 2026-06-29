import os
import sys
import tempfile

sys.path.insert(0, "..")  # noqa
import unittest

from aiosmtpd.controller import Controller

from dkit.utilities.smtp_helper import SmtpClient, SmtpMessage


class RecordingSMTPServer:

    def __init__(self):
        self.messages = []

    async def handle_DATA(self, server, session, envelope):
        self.messages.append(
            {
                "peer": session.peer,
                "mail_from": envelope.mail_from,
                "rcpt_tos": list(envelope.rcpt_tos),
                "content": envelope.content,
            }
        )
        return "250 OK"


class TestSMTPHelper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.handler = RecordingSMTPServer()
        try:
            cls.controller = Controller(cls.handler, hostname="127.0.0.1", port=2525)
            cls.controller.start()
        except PermissionError as exc:
            raise unittest.SkipTest(f"local SMTP sockets unavailable: {exc}")

    @classmethod
    def tearDownClass(cls):
        cls.controller.stop()

    def test_send_message(self):
        """send a message to the in-process SMTP test server"""
        client = SmtpClient("127.0.0.1", 2525)
        message = SmtpMessage(
            subject="test",
            sender="sender@nowhere.com",
            recipients=["recipient@nobody.com"],
            body="hello world",
        )

        client.send(message)

        self.assertEqual(len(self.handler.messages), 1)
        received = self.handler.messages[0]
        self.assertEqual(received["mail_from"], "sender@nowhere.com")
        self.assertEqual(received["rcpt_tos"], ["recipient@nobody.com"])
        self.assertIn(b"Subject: test", received["content"])
        self.assertIn(b"hello world", received["content"])


class TestSmtpMessageMimeContent(unittest.TestCase):

    def _make_message(self, files=None):
        msg = SmtpMessage(
            subject="Test",
            sender="sender@example.com",
            recipients=["recipient@example.com"],
            body="hello world",
            body_type="plain",
        )
        if files:
            msg.files = files
        return msg

    def test_no_attachments_produces_plain_text(self):
        """plain string body with no files should not produce multipart output"""
        mime = self._make_message().get_mime_content()
        self.assertIn("Content-Type: text/plain", mime)
        self.assertNotIn("multipart", mime)

    def test_attachment_produces_multipart(self):
        """plain string body with a file attachment must produce a multipart message"""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as f:
            f.write(b"a,b\n1,2\n")
            tmp_path = f.name
        try:
            mime = self._make_message(files=[tmp_path]).get_mime_content()
            self.assertIn("multipart", mime)
            self.assertIn(os.path.basename(tmp_path), mime)
            self.assertIn("Content-Type: text/plain", mime)
        finally:
            os.unlink(tmp_path)

    def test_attachment_does_not_raise(self):
        """get_mime_content must not raise MultipartConversionError when files are present"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"data")
            tmp_path = f.name
        try:
            self._make_message(files=[tmp_path]).get_mime_content()
        finally:
            os.unlink(tmp_path)

    def test_html_body_no_attachments_produces_alternative(self):
        """html_body without attachments produces a multipart/alternative message"""
        msg = SmtpMessage(
            subject="Test",
            sender="sender@example.com",
            recipients=["recipient@example.com"],
            body="plain text fallback",
            html_body="<p>html content</p>",
        )
        mime = msg.get_mime_content()
        self.assertIn("multipart/alternative", mime)
        self.assertIn("Content-Type: text/plain", mime)
        self.assertIn("Content-Type: text/html", mime)
        self.assertIn("plain text fallback", mime)
        self.assertIn("html content", mime)

    def test_html_body_with_attachment_produces_mixed_and_alternative(self):
        """html_body with attachments wraps multipart/alternative inside multipart/mixed"""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as f:
            f.write(b"%PDF")
            tmp_path = f.name
        try:
            msg = SmtpMessage(
                subject="Test",
                sender="sender@example.com",
                recipients=["recipient@example.com"],
                body="plain text fallback",
                html_body="<p>html content</p>",
            )
            msg.files = [tmp_path]
            mime = msg.get_mime_content()
            self.assertIn("multipart/mixed", mime)
            self.assertIn("multipart/alternative", mime)
            self.assertIn("Content-Type: text/plain", mime)
            self.assertIn("Content-Type: text/html", mime)
            self.assertIn(os.path.basename(tmp_path), mime)
        finally:
            os.unlink(tmp_path)


if __name__ == "__main__":
    unittest.main()
