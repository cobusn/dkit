# Copyright (c) 2018 Cobus Nel
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
import argparse
import shlex
import textwrap
from abc import ABCMeta, abstractmethod

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion
# from prompt_toolkit.eventloop.defaults import use_asyncio_event_loop
# from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.patch_stdout import patch_stdout

from .. import base
from ..exceptions import DKitApplicationException, DKitArgumentException, DKitShellException
from .console import echo


"""
Prompt Toolkit extensions
"""


class ProxyCmd(base.ConfiguredObject, metaclass=ABCMeta):

    cmd = ""

    @abstractmethod
    async def run(self, args):
        pass

    def complete(self, tokens, document):
        yield from []

    def get_help(self):
        return self.__doc__


class CmdCompleter(Completer):
    """
    Command dispatcher and completer
    """
    def __init__(self, lst_commands):
        self.cmd_map = {i.cmd: i for i in lst_commands}
        self.cmd_map["help"] = HelpCmd(self)

    @property
    def commands(self):
        return sorted(["exit"] + list(self.cmd_map.keys()))

    def get_completions(self, document, complete_event):
        """
        Main completer entry point
        """
        line = document.current_line_before_cursor

        if complete_event.completion_requested:

            try:
                tokens = shlex.split(line)
                if line.endswith(" "):
                    tokens.append("")
                # print(tokens)
            except ValueError:
                return

            # provide  a list of commands
            if len(tokens) == 0:
                yield from (Completion(i, 0) for i in self.commands)

            # complete a command
            if len(tokens) == 1:
                yield from self.complete_commands(line)

            # complete commands parameters
            elif len(tokens) > 1:
                cmd = tokens[0]
                if cmd in self.commands:
                    runner = self.cmd_map[cmd]
                    yield from runner.complete(tokens, document)

    def complete_commands(self, line):
        completions = [i for i in self.commands if i.startswith(line)]
        pos = len(line)
        yield from (Completion(i, start_position=-pos) for i in completions)


class AsyncCmdApplication(base.ConfiguredApplication, base.InitArgumentsMixin):
    """
    Abstract Base Class for Async Command Applications
    """
    def __init__(self, lst_commands=None, repository=None, **kwargs):
        super().__init__(repository, **kwargs)
        self.completer = CmdCompleter([])
        if lst_commands is not None:
            self.add_commands(lst_commands)

    async def run(self):
        """
        Run application
        """
        # use_asyncio_event_loop()
        session = PromptSession()
        print("xx")
        # history = InMemoryHistory()
        while True:
            try:
                with patch_stdout():
                    # Complete command and options
                    str_line = await session.prompt_async(
                        "$ ",
                        completer=self.completer,
                        # history=history,
                        # async_=True,
                    )

                    try:
                        line = shlex.split(str_line)
                    except ValueError:
                        return

                    if len(line) > 0:
                        command = line[0]

                        # run registered command
                        if command in self.completer.cmd_map:
                            # print(command)
                            runner = self.completer.cmd_map[command]
                            await runner.run(line)

                        # exit
                        if command.lower() == 'exit':
                            print("Good bye..")
                            return

            except DKitApplicationException as E:
                print(E)
            except DKitShellException as E:
                print(E)
            except IndexError as E:
                print(E)
            except KeyError as E:
                print("Invalid Key: {}".format(E))
            except FileNotFoundError as E:
                print(E)
            except (EOFError, KeyboardInterrupt):
                return
            except (DKitArgumentException) as E:
                print(E)
            except argparse.ArgumentError as E:
                print(E)
            except ValueError as E:
                print(E)

    def add_commands(self, lst_commands):
        self.completer.cmd_map.update({i.cmd: i for i in lst_commands})


class HelpCmd(ProxyCmd):
    """
    show help

    example usage:
    > help
    > help mv
    """
    cmd = "help"

    def __init__(self, completer, *kwds, **kwargs):
        super().__init__(*kwds, **kwargs)
        self.completer = completer
        self.map_commands = completer.cmd_map

    def complete(self, tokens, document):
        token = tokens[-1]
        length = len(token)
        yield from (
            Completion(i, -length)
            for i in sorted(self.map_commands.keys())
            if i.startswith(token)
        )

    async def run(self, args):
        if len(args) == 1:
            for command in self.map_commands.keys():
                echo(command)
        else:
            help_text = self.map_commands[args[-1]].get_help()
            if help_text is not None:
                help_text = textwrap.dedent(help_text).strip()
                echo(help_text)
            else:
                echo("No help available")
