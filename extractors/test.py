# Run integration test on this worker.
#


import os
import sys
import glob
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
