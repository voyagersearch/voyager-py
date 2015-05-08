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
import select
import sys
import os
import thread
import socket


class StdinPosix:

    def try_read(self):
        (ins, outs, errs) = select.select([sys.stdin], [], [], 0.1)
        if len(ins) > 0:
            return sys.stdin.readline()

    def close(self):
        pass


class StdinWin:
    """
    Adapted from code by ideawu@165.com
    """

    @staticmethod
    def stdin_thread(sock):
        """ read data from stdin, and write the data to sock
        """
        try:
            fd = sys.stdin.fileno()
            while True:
                # DO NOT use sys.stdin.read(), it is buffered
                data = os.read(fd, 1024)
                #print 'stdin read: ' + repr(data)
                if not data:
                    break
                while True:
                    nleft = len(data)
                    nleft -= sock.send(data)
                    if nleft == 0:
                        break
        except:
            pass
        sock.close()

    def __init__(self):
        self._serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._serv.bind(('127.0.0.1', 0))
        self._serv.listen(5)
        port = self._serv.getsockname()[1]

        # Thread writes to _stdin_sock
        self._stdin_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._stdin_sock.connect(('127.0.0.1', port))

        # Caller reads from _stdin
        self._stdin, addr = self._serv.accept()
        thread.start_new_thread(StdinWin.stdin_thread, (self._stdin_sock,))
        self._rbuf = ''

    def try_read(self):
        """If data is avail, then return it. Non blocking. """
        (ins, outs, errs) = select.select([self._stdin], [], [], 0.1)
        if len(ins) > 0:
            try:
                data = self._stdin.recv(4096)
            except:
                return None
            return self._rbuf + data
        else:
            return None


    def close(self):
        self._stdin.close()
        self._stdin_sock.close()
        self._serv.close()



def new():
    """Construct new instance of platform appropriate implementation.

    Caller should call close on the returned object when done.
    """
    if os.name == 'posix':
        return StdinPosix()
    else:
        return StdinWin()
