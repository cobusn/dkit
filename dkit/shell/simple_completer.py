'''
Created on 20 May 2011

@author: Cobus

'''
import readline
"""
import os
if os.name == 'nt':
    from pyreadline import *
"""


class SimpleCompleter(object):
    """Simple readline completer class.

    This class define a simple list base completer as well as provide
    the ability to insert text into the command line.

    .. warning::

        Always include the parse_and_bind construct in a calling application. Failure to do so
        will result in the completer not functioning.::

            readline.parse_and_bind('tab: complete')
    """

    prompt = "> "

    def __init__(self, options=None):
        super(SimpleCompleter, self).__init__()
        self.__insert_text = ''
        self.__matches = None
        self.__old_completer = None
        if options:
            self.__options = sorted(options)
        else:
            self.__options = []

    def __del__(self):
        self.__restore_state()

    #
    # Properties
    #

    # Insert Text
    def __get_insert_text(self):
        """Return the current readline insert text"""
        return self.__insert_text

    def __set_insert_text(self, text):
        """Set the current readline insert text"""
        if text is not None:
            self.__insert_text = str(text)
        else:
            self.__insert_text = str('')

    insert_text = property(__get_insert_text, __set_insert_text)

    # Options
    def __get_options(self):
        """Return current completion list"""
        return self.__options

    def __set_options(self, the_list):
        """Set current completions"""
        self.__options = sorted(the_list)

    options = property(
        __get_options, __set_options,
        None, "List of options available to the completer"
    )

    #
    # Methods
    #
    def complete(self, text, state):
        """Simple Completion method.
        This method will complete items in a list provided"""
        response = None
        if state == 0:
            if text:
                self.__matches = [
                    s for s in self.options
                    if s and s.startswith(text)
                ]
            else:
                self.__matches = self.options[:]
        try:
            response = self.__matches[state]
        except IndexError:
            response = None

        return response

    def __save_state(self):
        """Save the current readline state"""
        self.__old_completer = readline.get_completer()

    def __restore_state(self):
        """Restore the current readline state"""
        self.__register(self.__old_completer)
        self.insert_text = ''
        readline.set_pre_input_hook(None)

    def edit(self, prompt, _insert_text=""):
        """Edit a insert_text and return the answer.

        prompt
            line prompt.

        insert_text
            text inserted into line being edited.  If insert_text is provided
            here, it will override the class value and this is used instead.

        returns
            The line text after enter is pressed.
        """
        self.__save_state()
        readline.set_pre_input_hook(self.__pre_input_hook)
        self.__register()
        self.insert_text = _insert_text
        _answer = input(prompt + self.prompt)
        self.__restore_state()
        return _answer

    def __register(self, function=None):
        """Register a completer function.
        Default is self.complete.
        """
        if function:
            readline.set_completer(function)
        else:
            readline.set_completer(self.complete)

    def __pre_input_hook(self):
        """This is the pre_input_hook"""
        readline.insert_text(self.insert_text)
        readline.redisplay()
