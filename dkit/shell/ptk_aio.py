from . import ptk
from .ptk import ProxyCmd, echo  # NoQA
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts import PromptSession
from ..exceptions import (
    DKitApplicationException, DKitArgumentException, DKitShellException
)
import argparse
import shlex


class AHelpCmd(ptk.HelpCmd):
    """
    show help

    example usage:
    > help
    > help mv
    """

    async def run(self, args):
        super().run(args)


class AClearCmd(ptk.ClearCmd):
    """Clear Screen"""
    async def run(self, args):
        super().run(args)


class ACmdApplication(ptk.CmdApplication):

    async def _process_line(self):
        # Complete command and options
        str_line = await self.session.prompt_async(
            "$ ",
            completer=self.completer,
            mouse_support=False,
            # history=history,
        )

        try:
            line = shlex.split(str_line)
        except ValueError:
            return

        if len(line) > 0:
            command = line[0]

            # exit
            if command.lower() == 'exit':
                if self.on_exit:
                    self.on_exit()
                print("Good bye..")
                self.quit = True
                return

            # run registered command
            if command in self.completer.cmd_map:
                # print(command)
                runner = self.completer.cmd_map[command]
                await runner.run(line)
            else:
                raise DKitShellException(f"Invalid command: {command}")

    async def run(self):
        """
        Run application
        """
        self.session = PromptSession()
        # history = InMemoryHistory()
        if self.debug:
            while not self.quit:
                await self._process_line()
        else:
            while not self.quit:
                try:
                    with patch_stdout():
                        await self._process_line()
                except (
                    AssertionError,
                    FileNotFoundError,
                    IndexError,
                    ValueError,
                    argparse.ArgumentError,
                    DKitApplicationException,
                    DKitArgumentException,
                    DKitShellException,
                ) as E:
                    print(str(E))
                except KeyError as E:
                    print("Invalid Key: {}".format(E))
                except (EOFError, KeyboardInterrupt):
                    return
