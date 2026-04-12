import sys; sys.path.insert(0, "..")  # noqa
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


if __name__ == "__main__":
    unittest.main()
