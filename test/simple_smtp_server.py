"""
Simple SMTP Server used for testing purposes.
This server will listen on port 25 on the local server
"""
import asyncio
from aiosmtpd.controller import Controller


class MySMTPServer:

    async def handle_DATA(self, server, session, envelope):
        print(f"Peer: {session.peer}")
        print(f"From: {envelope.mail_from}")
        print(f"To: {envelope.rcpt_tos}")
        print(f"Message Length: {len(envelope.content)} bytes")
        return "250 OK"


async def main():
    controller = Controller(MySMTPServer(), hostname='localhost', port=2525)
    controller.start()
    print("started")
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        controller.stop()
        print("SMTP server stopped.")


if __name__ == "__main__":
    asyncio.run(main())
