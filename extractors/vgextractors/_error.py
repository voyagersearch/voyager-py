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
"""Module - Voyager errors."""


class VgErr:
    """ error codes """
    UNSUPPORTED_FILE      = 400
    UNSUPPORTED_OPERATION = 401
    ERROR_READING_FILE    = 402
    PATH_NOT_FOUND        = 403
    HANDLE_NOT_FOUND      = 404
    UNKNOWN_COMMAND       = 405
    UNKNOWN_ERROR         = 406
    INVALID_ARGUMENT      = 407
    EMPTY_ARGUMENTS       = 408
    COM_RPC_EXCEPTION     = 409
    RESOLUTION_FAIL       = 410
    NOMEM                 = 510


class Error(Exception):
    """Base error class."""
    pass


class IllegalStateException(Error):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class IOException(Error):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class JobSpecError(Error):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class JobException(Error):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class RouteException(Error):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg
