import argparse
import os
import struct
import platform


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
        raise argparse.ArgumentError(message)


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
