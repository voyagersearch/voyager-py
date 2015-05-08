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
# Run integration test on this worker.
#
import os
import sys
import logging
import subprocess
import platform


def diedie(msg):
    logging.fatal(msg)
    sys.exit(1)


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
logging.info("platform=%s     machine=%s" % (sys.platform, platform.machine()))



myenv = os.environ.copy()
myenv["LC_TYPE"] = "UTF-8"

pkgpath = os.path.abspath(os.path.join(os.getcwd(), "..", "..", "py", "voyager"))
extpath = os.path.abspath(os.getcwd())

paths=[pkgpath,extpath]

if myenv.has_key('PYTHONPATH'):
    myenv["PYTHONPATH"] = myenv["PYTHONPATH"] + os.path.pathsep + os.path.pathsep.join(paths)
else:
    myenv["PYTHONPATH"] = os.path.pathsep.join(paths)

logging.info("PYTHONPATH=%s" % myenv['PYTHONPATH'])

script = os.path.join(os.getcwd(), "..", "..", "test", "worker", "integration.py")
worker = os.path.join(os.getcwd(), "VoyagerWorkerPy.py")

ret = subprocess.call(["python", script, worker],
                      env=myenv,
                      cwd=os.getcwd())
if not ret == 0:
    diedie("ERROR test of worker %s failed" % worker)
else:
    logging.info("testing %s -- OK" % worker)
