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
import time


class RoutingStage:
    """Has an address (a zmq address), and a name (a routing stage type)"""
    def __init__(self, adict=None):
        self.address = None
        self.name = None
        if adict:
            if adict.has_key('addr'):
                self.address = adict['addr']
            if adict.has_key('type'):
                self.name = adict['type']
            self.adict = adict

    def is_routable(self):
        return (not self.address is None) and (len(self.address) > 0)

    def get(self, name, notfound=None):
        """
        Get a stage property.
        Return notfound value if not found.
        """
        if self.adict and self.adict.has_key(name):
            return self.adict[name]
        return notfound



class Router:
    def __init__(self, job, vpid):
        self.vpid = vpid
        self.route = job.get('route', [])
        self.currentidx = -1


    def started(self):
        """True if a stage is in progress."""
        return self.currentidx >= 0


    def has_current(self):
        return self.started()


    def start(self, stagetype):
        """Start routing stage.

        Sets pid and start time on the stage.
        """
        for x in range(len(self.route)):
            r = self.route[x]
            if r.has_key('type') and r['type'] == stagetype:
                if not r.has_key('finished'):
                    r['pid'] = self.vpid
                    r['start'] = int(time.time() * 1000)
                    self.currentidx = x
                    break


    def current_stage(self):
        """Get current routing stage or None"""
        if self.currentidx >= 0:
            return RoutingStage(self.route[self.currentidx])
        return None


    def get_stage(self, name):
        """Get first stage with type name, or None"""
        for route in self.route:
            if route.has_key('type') and route['type'] == name:
                return RoutingStage(route)
        return None


    def complete(self):
        """Return next stage, or None. """
        nextstage = None
        if self.currentidx >= 0:
            self.route[self.currentidx]['finished'] = time.time() * 1000
            nextidx = self.currentidx + 1
            if nextidx < len(self.route):
                nextstage = RoutingStage(self.route[nextidx])
        return nextstage
