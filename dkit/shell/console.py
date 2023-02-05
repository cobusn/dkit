import argparse
import os
import struct
import platform


class suppress_output(object):
    '''
    A context manager for doing a "deep suppression" of stdout and stderr in
    Python, i.e. will suppress all print, even if the print originates in a
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).

    '''
    def __init__(self):
        # Open a pair of null files
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = (os.dup(1), os.dup(2))

    def __enter__(self):
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0], 1)
        os.dup2(self.null_fds[1], 2)

    def __exit__(self, *_):
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0], 1)
        os.dup2(self.save_fds[1], 2)
        # Close the null files
        os.close(self.null_fds[0])
        os.close(self.null_fds[1])


class ArgumentParser(argparse.ArgumentParser):
    """
    Change default behaviour of argparse:

    * instead of exiting, Raise an Exception instead that
    will be handled by the shell.

    * disable built in help
    """
    def __init__(self, *kwds, **kwargs):
        super().__init__(add_help=False, *kwds, **kwargs)

    def error(self, message):
        raise argparse.ArgumentError(None, message)


def echo(*kwds, **kwargs):
    """
    substitute print function
    """
    print(*kwds, **kwargs)


def to_columns(list, displaywidth=None):
    """
    Display a list of strings as a compact set of columns.

    Each column is only as wide as necessary. Columns are separated by two spaces.

    borrowed from cmd.Cmd python library
    """
    displaywidth = get_terminal_size()[0] if displaywidth is None else displaywidth

    if not list:
        return "<empty>\n"
    nonstrings = [i for i in range(len(list)) if not isinstance(list[i], str)]
    if nonstrings:
        raise TypeError("list[i] not a string for i in %s" % ", ".join(map(str, nonstrings)))
    size = len(list)
    if size == 1:
        # return '%s\n' % str(list[0])
        return '%s' % str(list[0])
    # Try every row count from 1 upwards
    for nrows in range(1, len(list)):
        ncols = (size+nrows-1) // nrows
        colwidths = []
        totwidth = -2
        for col in range(ncols):
            colwidth = 0
            for row in range(nrows):
                i = row + nrows*col
                if i >= size:
                    break
                x = list[i]
                colwidth = max(colwidth, len(x))
            colwidths.append(colwidth)
            totwidth += colwidth + 2
            if totwidth > displaywidth:
                break
        if totwidth <= displaywidth:
            break
    else:
        nrows = len(list)
        ncols = 1
        colwidths = [0]
    retval = ""
    for row in range(nrows):
        texts = []
        for col in range(ncols):
            i = row + nrows*col
            if i >= size:
                x = ""
            else:
                x = list[i]
            texts.append(x)
        while texts and not texts[-1]:
            del texts[-1]
        for col in range(len(texts)):
            texts[col] = texts[col].ljust(colwidths[col])
        if row > 0:
            retval += "\n%s" % str("  ".join(texts))
        else:
            retval += "%s" % str("  ".join(texts))
    return retval


def columnize(list, displaywidth=None, print_fn=print):
    print_fn(to_columns(list, displaywidth))


#
# Borrowed from https://gist.github.com/jtriley/1108174#file-terminalsize-py
#
def get_terminal_size():
    """
    get width and height of console

    works on linux,os x,windows,cygwin(windows)
    adapted retrieved from:
    http://stackoverflow.com/questions/566746/how-to-get-console-window-width-in-python

    Will not work in cygwin

    Returns:
        tuple with width, height
    """

    def _get_terminal_size_windows():
        try:
            from ctypes import windll, create_string_buffer
            # stdin handle is -10
            # stdout handle is -11
            # stderr handle is -12
            h = windll.kernel32.GetStdHandle(-12)
            csbi = create_string_buffer(22)
            res = windll.kernel32.GetConsoleScreenBufferInfo(h, csbi)
            if res:
                (bufx, bufy, curx, cury, wattr, left, top,
                 right, bottom, maxx, maxy) = struct.unpack("hhhhHhhhhhh", csbi.raw)
                sizex = right - left + 1
                sizey = bottom - top + 1
                return sizex, sizey
        except Exception:
            pass

    def _get_terminal_size_linux():
        def ioctl_GWINSZ(fd):
            try:
                import fcntl
                import termios
                cr = struct.unpack('hh',
                                   fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
                return cr
            except Exception:
                pass
        cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
        if not cr:
            try:
                fd = os.open(os.ctermid(), os.O_RDONLY)
                cr = ioctl_GWINSZ(fd)
                os.close(fd)
            except Exception:
                pass
        if not cr:
            try:
                cr = (os.environ['LINES'], os.environ['COLUMNS'])
            except Exception:
                return None
        return int(cr[1]), int(cr[0])

    current_os = platform.system()
    tuple_xy = None
    if current_os == 'Windows':
        tuple_xy = _get_terminal_size_windows()
        if tuple_xy is None:
            tuple_xy = (80, 25)
    if current_os in ['Linux', 'Darwin'] or current_os.startswith('CYGWIN'):
        tuple_xy = _get_terminal_size_linux()
    if tuple_xy is None:
        tuple_xy = (80, 25)      # default value
    return tuple_xy


# Python program to print
# colored text and background
class Colors(object):
    """Terminal colors

    sourced from https://www.geeksforgeeks.org/print-colors-python-terminal/

    reset all colors with colors.reset
    """
    reset = '\033[0m'
    bold = '\033[01m'
    disable = '\033[02m'
    underline = '\033[04m'
    reverse = '\033[07m'
    strikethrough = '\033[09m'
    invisible = '\033[08m'

    class fg:
        black = '\033[30m'
        red = '\033[31m'
        green = '\033[32m'
        orange = '\033[33m'
        blue = '\033[34m'
        purple = '\033[35m'
        cyan = '\033[36m'
        lightgrey = '\033[37m'
        darkgrey = '\033[90m'
        lightred = '\033[91m'
        lightgreen = '\033[92m'
        yellow = '\033[93m'
        lightblue = '\033[94m'
        pink = '\033[95m'
        lightcyan = '\033[96m'

    class bg:
        black = '\033[40m'
        red = '\033[41m'
        green = '\033[42m'
        orange = '\033[43m'
        blue = '\033[44m'
        purple = '\033[45m'
        cyan = '\033[46m'
        lightgrey = '\033[47m'


def runner(method, debug=False):
    """
