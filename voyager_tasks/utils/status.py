# (C) Copyright 2014 Voyager Search
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys


S_SEP = ">>"
S_FLAG = "%sSTATUS" % S_SEP

S_KEY_MSG = "M"
S_KEY_PCT = "P"
S_KEY_NAME = "N"
S_KEY_PIN = "I"
S_KEY_TYPE = "T"
S_KEY_VPID = "V"
S_KEY_JOBID = "J"
S_KEY_TIMEOUT = "S"
S_KEY_STATE = "X"

STAT_SUCCESS = "SUCCESS"
STAT_FAILED = "FAILED"
STAT_IDLE = "IDLE"
STAT_STOPPING = "STOPPING"
STAT_WARNING = "WARNING"


class Writer(object):
    """Writer object with functions to report task status."""
    def __init__(self):
        """Initialize Writer call."""
        self._io = sys.stdout

    def __w(self, msg):
        """Write to wrapped output thing. """
        self._io.write(msg)

    def __fl(self):
        """Send newline and flush wrapped output thing."""
        self._io.write("\n")
        self._io.flush()

    def __send(self, key, val):
        """Write a key value pair to output thing. """
        if val:
            self.__w("{0}{1}={2}".format(S_SEP, key, val))

    def format_output(func):
        """Decorator used to wrap the output with markers. """
        def inner(*args, **kwargs):
            inst = args[0]
            inst.__w(S_FLAG)
            func(*args, **kwargs)
            inst.__w(S_FLAG)
            inst.__fl()
        return inner

    @format_output
    def job_started(self, jobid, timeout, desc=None):
        self.__send(S_KEY_JOBID, jobid)
        if desc:
            self.__send(S_KEY_MSG, desc)
        self.__send(S_KEY_TIMEOUT, timeout)

    @format_output
    def send_status(self, msg):
        self.__send(S_KEY_MSG, msg)

    @format_output
    def send_vpid(self, vpid):
        self.__send(S_KEY_VPID, vpid)

    @format_output
    def send_percent(self, pct, msg, name):
        if pct > 1:
            pct = 1.0
        elif pct < 0:
            pct = 0

        self.__send(S_KEY_MSG, msg)
        self.__send(S_KEY_PCT, "%4.3f" % pct)
        self.__send(S_KEY_NAME, name)

    @format_output
    def send_state(self, statev, msg=None):
        self.__send(S_KEY_STATE, statev)
        if msg:
            self.__send(S_KEY_MSG, msg)
