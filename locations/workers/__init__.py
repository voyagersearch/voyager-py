import os
import sys
import platform
import glob

__all__ = ['base_job', 'esri_worker', 'dynamodb_worker', 'gdal_worker', 'mongodb_worker',
           'sql_worker', 'oracle_worker', 'mysql_worker']

def append_or_set_path(path):
  try:
    p = os.environ['PATH']
    if len(p) > 0 and not p.endswith(os.pathsep):
        p += os.pathsep
    p += path
    os.environ['PATH'] = p
  except KeyError:
    os.environ['PATH'] = path

# Add dependent libraries to the system paths.
arch_dir = 'win32_x86'
if platform.system() == 'Darwin':
    arch_dir = 'darwin_x86_64'
elif platform.system() == 'Linux':
    arch_dir = 'linux_amd64'

dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'arch', arch_dir))
gdal_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'gdal'))

append_or_set_path(dll_path)

os.environ['GDAL_DATA'] = gdal_path
egg_path = os.path.join(dll_path, 'py')
sys.path.append(egg_path)
libs = glob.glob(os.path.join(egg_path, '*.egg'))
for lib in libs:
    sys.path.append(lib)
