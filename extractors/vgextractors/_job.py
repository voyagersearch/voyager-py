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
"""Module worker.Job

Includes:
  - Job
  - ExtractionJob
"""
import json
from _vgdexfield import VgDexField


class Job:
    """
    A Job is whatever data is necessary to get some work done.

    Instance attributes:
    id   -- The job id (also available as jobID)
    description -- Task description
    """

    @staticmethod
    def from_file(path):
        with open(path, 'r') as inputfile:
            obj = json.load(inputfile)
            return Job(obj)
        raise IOException('failed to open %s' % path)


    @staticmethod
    def from_dict(d):
        return Job(d)


    def __init__(self, props):
        """
        props - a dict
        """
        self.props = props
        self.id = props[VgDexField.ID]
        if props.has_key('path'):
            self.description = props['path']
        elif props.has_key('file'):
            self.description = props['file']
        elif props.has_key('location'):
            self.description = props['location']
        else:
            self.description = "id:%s" % self.id


    def __str__(self):
        return "Job %s: %s" % (self.id, self.description)


    def get(self, name, notfound=None):
        """Get named prop or return the notfound value."""
        if self.props.has_key(name):
            return self.props[name]
        return notfound


    def has(self, name):
        return self.props.has_key(name)


    def get_timeout(self):

        """Get timeout attr or 0"""
        t = 0
        try:
            if self.props.has_key('timeout'):
                t = int(self.props['timeout'])
        except ValueError, e:
            t = 0
        return t


    def to_json(self, indent=None):
        return json.dumps(self.props, indent=indent)


    def warning(self, msg):
        self.append_field(VgDexField.INDEXING_WARNING, msg)


    def error_trace(self, msg):
        self.append_field(VgDexField.INDEXING_ERROR_TRACE, msg)


    def error(self, code, msg):
        """ Report indexing error. """
        self.set_field(VgDexField.INDEXING_ERROR_CODE, code)
        self.set_field(VgDexField.INDEXING_ERROR, msg)


    def append_field(self, name, val):
        self.set_field(name, val, True)


    def set_field(self, name, value, append=False):
        if not self.has('entry'):
            self.props['entry'] = {}
        entry = self.props['entry']
        if not entry.has_key('fields'):
            entry['fields'] = {}
        fields = entry['fields']
        if append and  fields.has_key(name):
            fields[name] = "%s\n%s" % (fields[name], value)
        else:
            fields[name] = value


    def get_field(self, name, notfound=None):
        if self.has('entry'):
            if self.props['entry'].has_key('fields'):
                if self.props['entry']['fields'].has_key(name):
                    return self.props['entry']['fields'][name]
        return notfound


# Not sure if we need this but, ....
class ExtractionJob(Job):

    @staticmethod
    def from_file(path):
        with open(path, 'r') as inputfile:
            obj = json.load(inputfile)
            return ExtractionJob(obj)
        raise IOException('failed to open %s' % path)

    @staticmethod
    def from_dict(d):
        return ExtractionJob(d)


    def __init__(self, props):
        Job.__init__(self, props)
