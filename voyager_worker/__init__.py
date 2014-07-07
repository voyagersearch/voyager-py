import os
import sys
import glob

__all__ = ['base_job', 'esri_worker', 'gdal_worker', 'odbc_worker', 'voyager_utils']

# Add dependent libraries to the system paths.
dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'arch', 'win32_x86'))
os.environ['PATH'] += os.pathsep + dll_path #r"C:\Voyager\server_1.9.4.1815\app\arch\win32_x86"
egg_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'arch', 'win32_x86', 'py')) #r"C:\Voyager\server_1.9.4.1815\app\arch\win32_x86\py"
libs = glob.glob(os.path.join(egg_path, '*.egg'))
for lib in libs:
    sys.path.append(lib)





