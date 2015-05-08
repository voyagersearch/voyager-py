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
import argparse
import logging
import _error


LOGFORMAT="%(asctime)s:%(name)s:%(process)d: %(message)s"

class Config:
    """
    Worker configuration helper. Takes care of parsing common options.
    These are:

    --type    : The jobtype
    --results : ZMQ connection string for pushing results to the tracker (deprecated?)
    --chat    : ZMQ connection string for chatting with tracker.
    --foreman : ZMQ connections tring for the foreman signal channel.
    --count   : Number of jobs to run.
    --level   : Set the log level.  Log levels are taken from org.apache.log4j.Level
                and are (python levels in parens):
                    OFF   (100)
                    FATAL (50)
                    ERROR (40)
                    WARN  (30)
                    INFO  (20)
                    DEBUG (10)
                    TRACE (0)
                    ALL   (0)

    --job     : Path to a job to run directly
    --info    : Get the json info string
    """

    def __init__(self, arg_callback=None):
        """ Create new Config object by parsing command line args.

        arg_callback -- If specidied this is expected to support a function
                        named add_args_hook(ArgumentParser).  Ars you add here
                        are available in the args property of this class.
        """

        parser = argparse.ArgumentParser()

        parser.add_argument("--job",
                            help="specify a job file (skips job queue, and implies --count 1)",
                            metavar="FILE")

        parser.add_argument("--results", help="ZMQ connection string for submitting results (not used)",
                            metavar="ZMQADDR")
        parser.add_argument("--chat", help="ZMQ connection string for chatting with tracker",
                            metavar="ZMQADDR")
        parser.add_argument("--foreman", help="ZMQ connection string for forman",
                            metavar="ZMQADDR")

        parser.add_argument("--count",
                            help="run N jobs, then exit (0, the default, means run forever)",
                            metavar="N", default=0, type=int)

        parser.add_argument("--level",
                            help="Set the log level (either numeric or named, default is 10 or DEBUG)",
                            metavar="N", default=10)

        parser.add_argument("--type", help="Set the job type used to requst jobs",
                            dest="jtype", default=None,
                            metavar="TYPE")

        parser.add_argument("--info", help="Generate json info message",
                            dest="is_info",
                            action="store_const", const=True, default=False)

        if arg_callback:
            arg_callback.add_args_hook(parser)

        args = parser.parse_args()

        # Either --job or (--type AND --chat) (er, or --info)
        if not args.is_info:
            if args.job == None and (args.jtype == None or args.chat == None):
                raise _error.IllegalStateException("must specify either --job or (--type and --chat)")

        # If you specify a job, then run once.
        if args.job:
            args.count = 1

        # logging
        logging.basicConfig(format=LOGFORMAT)
        levelmap = { "off":100,
                     "fatal":50,
                     "error": 40,
                     "warn": 30,
                     "info": 20,
                     "debug": 10,
                     "trace": 0,
                     "all": 0 }
        llevel = 10
        try:
            llevel = int(args.level)
        except ValueError, e:
            if levelmap.has_key(args.level.lower()):
                llevel = levelmap[args.level.lower()]
            else:
                raise _error.IllegalStateException("illegal log level: %s" % args.level)
        logging.getLogger("voyager").setLevel(llevel)
        self.args = args


    def is_info(self):
        return self.args.is_info


    def hello_config(self, data):
        """Process the registration configuration data from the tracker."""
        print "Tracker config data: %s" % data


    def count(self):
        """Get desired number of jobs to process before exit.

        A value of zero means run forever.
        """
        return self.args.count


    def jobtype(self):
        """Return jobtype arg."""
        return self.args.jtype


    def job(self):
        """Return path to local job file.  If specified."""
        return self.args.job


    def has_tracker(self):
        """Return true if we have a tracker configured."""
        return self.args.chat != None


    def has_result_queue(self):
        """Return true if there is a result queue configured."""
        return self.args.results != None

    def has_foreman(self):
        """True if we have foreman/signal address. """
        return self.args.foreman != None


    def resqueue_addr(self):
        """Return the zmq style socket address for the results queue.

        None is returned if this is not set.
        """
        return self.args.results


    def chatsocket_addr(self):
        """Return the zmq style socket address for the traker REP socket."""
        return self.args.chat


    def foreman_addr(self):
        """Return the foreman signal address if provided."""
        return self.args.foreman
