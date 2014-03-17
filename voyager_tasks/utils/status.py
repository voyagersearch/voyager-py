"""Module status
Purpose is to emit log messages in a format that the Foreman can
parse.

Equivalent to the Java voyager.api.jobs.worker.WorkerStatusWriter.
"""

__author__ = 'VoyagerSearch'

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


class Writer:
    """Write class"""
    def __init__(self, wrappedio=None):
        """Create a new one.

        wrappedio - Something that behaves like sys.stdout, if not specified
        we use stdout.
        """
        if wrappedio:
            self._io = wrappedio
        else:
            self._io = sys.stdout


    def _w(self, msg):
        """Write to wrapped output thing. """
        self._io.write(msg)


    def _fl(self):
        """Send newline and flush wrapped output thing."""
        self._io.write("\n")
        self._io.flush()


    def _send(self, key, val):
        """Write a key value pair to output thing. """
        if val:
            self._w("%s%s=%s" % (S_SEP, key, val))


    def format(func):
        """Decorator used to wrap the output with markers. """
        def inner(*args, **kwargs):
            inst = args[0]
            inst._w(S_FLAG)
            func(*args, **kwargs)
            inst._w(S_FLAG)
            inst._fl()
        return inner



    @format
    def job_started(self, jobid, timeout, desc=None):
        self._send(S_KEY_JOBID, jobid)
        if desc:
            self._send(S_KEY_MSG, desc)
        self._send(S_KEY_TIMEOUT, timeout)


    @format
    def send_status(self, msg):
        self._send(S_KEY_MSG, msg)


    @format
    def send_vpid(self, vpid):
        self._send(S_KEY_VPID, vpid)


    @format
    def send_percent(self, pct, msg, name):
        if pct > 1:
            pct = 1.0
        elif pct < 0:
            pct = 0

        self._send(S_KEY_MSG, msg)
        self._send(S_KEY_PCT, "%4.3f" % pct)
        self._send(S_KEY_NAME, name)


    @format
    def send_state(self, statev, msg=None):
        self._send(S_KEY_STATE, statev)
        if msg:
            self._send(S_KEY_MSG, msg)

