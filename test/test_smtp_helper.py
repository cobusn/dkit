import sys; sys.path.insert(0, "..")  # noqa
from dkit.utilities.smtp_helper import SmtpClient, SmtpMessage


client = SmtpClient("127.0.0.1", 2525)
message = SmtpMessage(
    subject="test",
    sender="cobus@nel.org.za",
    recipients=["cobus.nel@dimensiondata.com"],
)
client.send(message)
