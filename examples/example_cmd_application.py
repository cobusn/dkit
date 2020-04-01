import asyncio
import sys; sys.path.insert(0, "..")  # noqa

from dkit.shell.ptk import AsyncCmdApplication, ProxyCmd, Completion


class CmdPrint(ProxyCmd):

    cmd = "print"

    async def run(self, args):
        print(" ".join(args[1:]))

    def complete(self, tokens, document):
        token = tokens[-1]
        length = len(token)
        yield from (Completion(i, -length) for i in ["one", "two"])


def main_():
    asyncio.run(AsyncCmdApplication([CmdPrint()]).run())


if __name__ == "__main__":
    main_()
