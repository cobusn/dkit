"""
Example of using the Async version of CMDApplication with
prompt toolkit
"""
import sys; sys.path.insert(0, "..")  # noqa
import asyncio
from dkit.shell.ptk_aio import (
    ACmdApplication, AHelpCmd, AClearCmd, ProxyCmd, echo
)


class EchoCmd(ProxyCmd):
    """
    Echo Parameters
    """
    cmd = "echo"

    async def run(self, args):
        echo(" ".join(args[1:]))


async def main():
    app = ACmdApplication()
    app.add_commands([
        EchoCmd(),
        AHelpCmd(app.completer),
        AClearCmd(),
    ])
    await app.run()


if __name__ == "__main__":
    asyncio.run(main())
