#
# Copyright (C) 2014  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#

"""
- Created on 11 Feb 2012
- Cythonised on 7 Sept 2018
"""
import string
import time
from enum import Enum
from . import time_helper
from . import log_helper as log

TIMEFN = time.perf_counter


class MESSAGES(Enum):
    NOT_STARTED = "Timer not started"
    NOT_STOPPED = "Timer not stopped"
    NOT_COUNTER = "Object should be of type Counter."
    WRONG_LOG = "log method should be one of: (error,warning,info,debug)"


class TimerException(Exception):
    """timer exception"""


class Timer(object):
    """
    Timer class

     .. include:: ../../examples/example_timer.py
        :literal:

    Produces:

      .. include:: ../../examples/example_timer.out
        :literal:

    """

    def __init__(self, **kwds):
        super(Timer, self).__init__(**kwds)
        self.__start_time: float = -1
        self.__end_time: float = None

    def __str__(self, *args, **kwargs) -> str:
        return str(self.str_elapsed)

    def start(self):
        """
        Start the timer.

        The function return the start time.

        :rtype: float time in seconds
        """
        self.__start_time = TIMEFN()
        self.__end_time = None
        return self

    def restart(self):
        """Alias for start"""
        return self.start()

    def stop(self):
        """
        Stop the timer.

        :rtype: float time in seconds
        :raises: TimerException
        """
        if self.__start_time >= 0.0:
            self.__end_time = TIMEFN()
            return self
        else:
            raise TimerException(MESSAGES.NOT_STARTED)

    @property
    def seconds_elapsed(self) -> float:
        """
        Return elapsed time in seconds.

        If the timer have been stopped, it will return the time elapsed
        between start and stop times.  Otherwise it will return the time
        elapsed since the timer was started.

        :rtype: float
        :raises: TimerException
        """
        if self.__start_time >= 0.0:
            if self.__end_time:
                return self.__end_time - self.__start_time
            else:
                return TIMEFN() - self.__start_time
        else:
            raise TimerException(MESSAGES.NOT_STARTED)

    @property
    def time_started(self) -> float:
        """
        Return the time when the timer was started.

        As defined by time.time()

        :rtype: float
        :raises: TimerException
        """
        if self.__start_time >= 0.0:
            return self.__start_time
        else:
            raise TimerException(MESSAGES.NOT_STARTED)

    @property
    def time_stopped(self) -> float:
        """
        Return the time when the timer was stopped.

        As defined by time.time()

        :rtype: float
        :raises: TimerException
        """
        if self.__end_time:
            return self.__end_time
        else:
            raise TimerException('Timer not stopped yet.')

    @property
    def hours_elapsed(self) -> float:
        """
        Return number of hours elapsed.

        This is equal to seseconds_elapsedlapsed/3600

        :rtype: float
        :raises: TimerException
        """
        return self.seconds_elapsed / 3600

    @property
    def minutes_elapsed(self) -> float:
        """
        Return number of minutes elapsed.

        This is equalseconds_elapsedonds_elapsed/60

        :rtype: float
        :raises: TimerException
        """
        return self.seconds_elapsed / 60

    @property
    def hms_elapsed(self) -> tuple:
        """
        Returns Hours, Minutes, Seconds and Milli-Seconds elapsed

        returns (hours, minutes, seconds, milliseconds)

        :rtype: (int,int,int,int)
        :raises: TimerException
        """
        return time_helper.hms(self.seconds_elapsed)

    @property
    def str_elapsed(self) -> str:
        """
        Returns Hours, Minutes, Seconds and Milli-Seconds elapsed as a string

        returns  '(xx hours, xx minutes, xx seconds, xx milliseconds)'

        :rtype: str
        :raises: TimerException
        """
        _hms = time_helper.hms(self.seconds_elapsed)
        return "%d hours, %d minutes, %d seconds, %d milliseconds" % _hms


class Counter(object):
    """
    Simple Counter Class

    Sample usage:

     .. include:: ../../examples/example_counter.py
        :literal:

    Produces:

      .. include:: ../../examples/example_counter.out
        :literal:
    """

    def __init__(self, value=int(0), **kwds):
        '''
        Constructor
        '''
        super().__init__(**kwds)
        self._counter = value

    def increment(self, value: int = 1) -> int:
        """
        Increment counter

        :param value long: Value that the counter will be incremented by.
        :raises: exc

        :rtype: long
        """
        self._counter += value
        return self._counter

    def reset(self):
        """
        Reset the counter to 0.

        :rtype: long
        """
        self._counter = int(0)

    def __add__(self, right) -> "Counter":
        """
        Add two counters

        :rtype: long
        """
        if isinstance(right, Counter):
            return Counter(self.value.__add__(right.value))
        else:
            raise TypeError(MESSAGES.NOT_COUNTER)

    def __str__(self):
        """
        string representation

        :rtype: str
        """
        return self._counter.__str__()

    def __repr__(self):
        """
        __repr__

        :rtype: str
        """
        return "%s(%d)" % (self.__class__.__name__, self.value)

    def __get_value(self):
        """
        Return the current value

        :rtype: long
        """
        return self._counter

    value = property(__get_value)


class CounterLogger(Counter, Timer):
    """
    Provide convenience logging functions that log a quantity
    or time.

    This class have the following services:

    * Timer
    * Event counter
    * Ability to log these values
    * Customizable template for logging

    The template will be able to substitute the following values:

    * '${seconds}'
    * '${minutes}'
    * '${hours}'
    * '${strtime}'
    * '${counter}'

    The example below illustrate typical usage:

    .. include:: ../../examples/example_counter_logger.py
        :literal:

    Produces:

      .. include:: ../../examples/example_counter_logger.out
        :literal:

    :param logger: logger instance
    :param trigger: log trigger
    :param log_method: logging method (error, warning, info, debug)
    :param log_template: template, see above for variables
    """

    def __init__(self, logger=log.null_logger(), trigger=10000, log_method="info",
                 value: int = 1, log_template="Processed: ${counter} after ${seconds} seconds.",
                 **kwds):
        """
        Constructor
        """
        super().__init__(value, **kwds)
        self.log_methods = {
            "error": self.error,
            "warning": self.warning,
            "info": self.info,
            "debug": self.debug
            }
        self.__current_log_method = None
        self.logger = logger
        self.__next_trigger = self.__trigger = trigger
        self.__log_template = log_template
        self.__log_method = None
        self.log_method = log_method

    # ==========================================================================
    #  Private Methods
    # ==========================================================================

    def __substitute(self, log_template):
        """
        Populate template values with current counters.
        """
        _template_values = {
            'seconds': "%.2f" % self.seconds_elapsed,
            'hours':   "%.2f" % self.hours_elapsed,
            'minutes': "%.2f" % self.minutes_elapsed,
            'strtime': self.str_elapsed,
            'counter': "{:,}".format(self.value)
        }
        _template = string.Template(log_template)
        return _template.substitute(_template_values)

    def __do_log(self):
        """
        Perform log action.
        """
        self.__log_method()

    # ==========================================================================
    # Public Methods
    # ==========================================================================

    def info(self, log_template=None):
        """
        Perform 'info' log action.
        """
        if log_template:
            log_string = self.__substitute(log_template)
        else:
            log_string = self.__substitute(self.log_template)
        self.logger.info(log_string)

    def error(self, log_template=None):
        """
        Perform 'error' log action.
        """
        if log_template:
            log_string = self.__substitute(log_template)
        else:
            log_string = self.__substitute(self.log_template)
        self.logger.error(log_string)

    def warning(self, log_template=None):
        """
        Perform 'warning' log action.
        """
        if log_template:
            log_string = self.__substitute(log_template)
        else:
            log_string = self.__substitute(self.log_template)
        self.logger.warning(log_string)

    def debug(self, log_template=None):
        """
        Perform 'debug' log action.
        """
        if log_template:
            log_string = self.__substitute(log_template)
        else:
            log_string = self.__substitute(self.log_template)
        self.logger.debug(log_string)

    # ===========================================================================
    # Overloaded methods
    # ===========================================================================

    def increment(self, value: int = 1) -> int:
        """
        Increment the counter.

        Overrides the increment method on Counter()

        :param name type: description
        :rtype: retval
        :raises: exc
        """
        self._counter += value
        if self._counter >= self.__next_trigger:
            self.__do_log()
            self.__next_trigger = self._counter + self.__trigger
        return self._counter

    def stop(self):
        """
        Stop the Counter and write to the log.

        :raises: TimerException
        """
        super().stop()
        self.__do_log()
        return self

    def start(self):
        """
        Reset the counters.
        """
        self.reset()
        super().start()
        return self

    # ==========================================================================
    #  Overloaded Operators
    # ==========================================================================

    def __repr__(self):
        return "CounterLogger(...)"

    def __add__(self, other):
        raise NotImplementedError

    # ==========================================================================
    # Properties
    # ==========================================================================

    # trigger

    def __get_trigger(self):
        """Property, see below"""
        return self.__trigger

    def __set_trigger(self, value):
        """Property, see below"""
        self.__trigger = value

    trigger = property(__get_trigger, __set_trigger, None,
                       "Trigger for logger.")

    # log_string

    def __get_log_string(self):
        """Property, see below"""
        return self.__log_template

    def __set_log_string(self, value):
        """Property, see below"""
        self.__log_template = value

    log_template = property(
        __get_log_string, __set_log_string, None,
        "Template string used for logging."
    )

    # log_method

    def __get_log_method(self):
        """
        Log function to use for logging.

        should be one of the following:
         * error
         * warning
         * info
         * debug

        :param log_method str: log method to use.
        :rtype: str
        :raises: ValueError
        """
        return self.__current_log_method

    def __set_log_method(self, value):
        """Property, see below"""
        if value in self.log_methods.keys():
            self.__current_log_method = value
            self.__log_method = self.log_methods[value]
        else:
            raise ValueError(MESSAGES.WRONG_LOG)

    log_method = property(__get_log_method, __set_log_method)
