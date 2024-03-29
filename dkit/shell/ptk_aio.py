# Copyright (c) 2024 Cobus Nel
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
Async version of the prompt toolkit utilities

Refer to examples/example_ptk_aio.py
"""

from . import ptk
from .ptk import ProxyCmd #noqa
from prompt_toolkit import print_formatted_text as echo
from prompt_toolkit.patch_stdout import patch_stdout
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
                echo("Good bye..")
                self.quit = True
                return

            # run registered command
            if command in self.completer.cmd_map:
                runner = self.completer.cmd_map[command]
                await runner.run(line)
            else:
                raise DKitShellException(f"Invalid command: {command}")

    async def run(self):
        """
        Run application
        """
        self.session = self._make_session()
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
                    echo(str(E))
                except KeyError as E:
                    echo("Invalid Key: {}".format(E))
                except (EOFError, KeyboardInterrupt):
                    return
