# -*- coding: utf-8 -*-
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
"""Base -- A worker base class."""
import json
import os
import sys
import time
import logging
import socket
import _error
import _config
import _status
import _stdin
import zmq

class Base:
    """
    This is the base worker object. This runs the general purpose
    worker loop: grab next job from the tracker, do something, report
    result.

    Attributes:
    config      -- Current Config object.
    jobtype     -- Jobtype passed to construtor.
    status      -- status.Writer object
    log         -- a Logger
    procname    -- procname for use in vpid
    job_factory -- as passed to ctor
    """

    MAX_WAIT_FOR_JOB = 1      # unit: seconds
    INACTIVITY_TIMEOUT = 10   # unit: seconds

    def __init__(self, job_factory):
        """Create a new one.

        job_factory - Must have from_dict(d) and from_file(path) methods
        that return a Job or subclass of Job (or None).
        """
        self.procname = sys.argv[0]
        self.job_factory = job_factory
        self.status = _status.Writer()
        self.log = logging.getLogger("voyager")
        self.config = _config.Config(self)
        self.jobtype = self.config.jobtype()
        self.context = None
        self._chatsock = None
        self._sigsock = None
        self._keepgoing = True
        self._stdin = _stdin.new()
        self._cmdbuffer = []
        self._vpid = None


    def run_job(self, job):
        """Subclasses expected to implement this.

        Args:
        job -- A voyager.Job object.

        Should raise exception on error.  Otherwise we expect a result
        to be forwarded according to routing.

        @return True if no errors, False if errors (but result was forwarded)
        """
        raise _error.IllegalStateException("run_job must be defined")



    def add_args_hook(self, argparser):
        """Override if you want to add args. """
        pass



    def handle_command(self, command):
        """Subclasses can optionally override this.

        Do someting in response to a command over STDIN from the foreman.
        The default implementation understands "stop", and stops
        the inner processing loop if that is received.
        """
        if command == "stop":
            self.log.info("stop request recieved from foreman")
            self.halt()
        else:
            self.log.warning("unhandled command from foreman: '%s'" % command)


    def zmq_setup(self):
        """
        Setup the REQ socket we connect to tracker for chat and jobs.
        """
        self.context = zmq.Context()
        if self.config.has_tracker():
            self._chatsock = self.context.socket(zmq.REQ)
            self._chatsock.connect(self.config.chatsocket_addr())
        else:
            self._chatsock = None
        if self.config.has_foreman():
            self._sigsock = self.context.socket(zmq.REQ)
            self._sigsock.connect(self.config.foreman_addr())


    def zmq_teardown(self):
        if self._chatsock: self._chatsock.close()
        if self._sigsock: self._sigsock.close()
        # if self.context: self.context.term()


    def get_vpid(self):
        if self._vpid is None:
            t = time.localtime()
            self._vpid = "%s/%s_%s/%s_%s" % \
                        (time.strftime("%Y%m%d", t),
                         socket.gethostname(),
                         os.path.basename(self.procname),
                         time.strftime("%H%M%S", t),
                         os.getpid())
        return self._vpid



    def run(self):
        """ Main entry point. """
        if self.config.is_info():
            print json.dumps(self.run_info())
        else:
            self.run_loop()


    def run_info(self):
        """ Must return an info dict."""
        return {}

    def run_loop(self):
        """
        Main run loop, dispatches jobs to run_job method.

        Throws exception if something goes wrong.
        """
        self.log.info("starting")
        ok = True
        try:
            self.zmq_setup()
            if self.config.has_tracker():
                self.tracker_hello()
            self.status.send_vpid(self.get_vpid())
            lastjobtime = time.time()
            jobcount = 0
            poller = zmq.Poller()
            poller.register(sys.stdin, zmq.POLLIN)
            while self._keepgoing:
                if self.config.job() != None:
                    nextjob = self.job_factory.from_file(self.config.job())
                else:
                    # next line blocks:
                    nextjob = self.next_job()

                if nextjob is None:
                    # time to die?
                    self.status.send_state(_status.STAT_IDLE)
                    if (time.time() - lastjobtime) > Base.INACTIVITY_TIMEOUT:
                        self.log.info("timeout waiting for work")
                        break
                else:
                    lastjobtime = time.time()
                    self.log.info("start %s" % nextjob)
                    self.status.job_started(nextjob.id, nextjob.description, nextjob.get_timeout())
                    ok = False
                    try:
                        ok = self.run_job(nextjob)
                        if ok:
                            self.status.send_state(_status.STAT_SUCCESS)
                        else:
                            self.status.send_state(_status.STAT_FAILED)
                            self.log.critical("job run indicates failure")
                    except _error.Error, e:
                        self.log.critical("job run failed: %s" % e)
                        self.status.send_state(_status.STAT_FAILED)

                    if not ok:
                        raise _error.JobException("job failed")

                    jobcount += 1
                    if (self.config.count() > 0) and jobcount >= self.config.count():
                        break

                command = self.try_read_command()
                if command:
                    self.handle_command(command)
        finally:
            self.status.send_state(_status.STAT_STOPPING)
            try:
                self.shutdown_hook()
            except:
                pass
            try:
                self._stdin.close()
            except:
                pass
            self.zmq_teardown()


    def halt(self):
        """Arrange to exit the run loop at next iteration. """
        self._keepgoing = False


    def next_job(self):
        """Grab a job from the tracker.
        Returns a Job object (or whatever comes back from job_factory), or None.
        """
        job = None

        query = { 'owner' : self.get_vpid(), 'type' : self.jobtype }
        msg = { 'checkout' : query }

        self._chatsock.send(json.dumps(msg))

        # NOTE: next line blocks.
        obj = json.loads(self._chatsock.recv())

        # Expected:
        #   { ack:OK, jobs:[{a_job}] }
        if obj:
            if obj.has_key('ack') and obj['ack'] == 'OK':
                if obj.has_key('jobs'):
                    jobsarr = obj['jobs']
                    if len(jobsarr) == 1:
                        job = self.job_factory.from_dict(jobsarr[0])
                    else:
                        self.log.error("empty job array from tracker")
                else:
                    self.log.error("no jobs property in tracker response: %s" % obj)
            else:
                self.log.error("bad ack from tracker: %s" % obj)
        else:
            self.log.error("null payload from tracker job request")
        return job



    def pre_hello_hook(self, message):

        """
        Subclass may override if they want.

        message -- A dict object that subclass is free to embellish.
        """
        pass


    def post_hello_hook(self, obj):
        """
        Subclass may override if the want.

        obj -- json created response from tracker hello.
        """
        pass


    def shutdown_hook(self):
        """
        Subclasses may do someting here, this is called after run loop
        has completed, but before we shutdown ZMQ.
        """
        pass


    def tracker_hello(self):
        """
        Perform registration with the tracker, recieve additional configuration
        data.
        """
        #TODO: Real HELLO message
        message = {
            "jobType" : self.jobtype,
            "vpid" : self.get_vpid(),
            "lang" : "python",
            "encoding" : "json" }
        self.pre_hello_hook(message)
        self._chatsock.send(json.dumps(message))
        obj = json.loads(self._chatsock.recv())
        self.post_hello_hook(obj)
        self.config.hello_config(obj)
        self.log.info("configured")


    def try_read_command(self):
        """
        Try to read a command from the foreman over STDIN.

        Returns command recieved as a string or None.
        """
        if len(self._cmdbuffer) > 0:
            return self._cmdbuffer.pop(0)
        cmd = self._stdin.try_read()
        if cmd:
            cmds = cmd.strip().split("\n")
            if len(cmds) > 1:
                self._cmdbuffer += cmds[1:]
            return cmds[0]
        return None


    def exec_command(self, command, args):
        """
        Send a command with args to the foreman, return result.
        """
        if self._sigsock is None:
            raise _error.IOException('no foreman address configured')
        return self._sigsock.send("CMD %s %s" % (command, args))
