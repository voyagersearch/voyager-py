import os
import sys
import glob


__all__ = []
for module in os.listdir(os.path.dirname(__file__)):
    if module == '__init__.py' or module[-3:] != '.py':
        continue
    __all__.append(module[:-3])

# dll_path = r"C:\Voyager\server_1.9.7.3244\app\arch\win32_x86" #os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'arch', 'win32_x86'))
# gdal_path = r"C:\Voyager\server_1.9.7.3244\app\gdal"
dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'arch', 'win32_x86'))
gdal_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '..', 'gdal'))
if os.environ['PATH'].endswith(';'):
    os.environ['PATH'] += dll_path
else:
    os.environ['PATH'] += os.pathsep + dll_path
os.environ['GDAL_DATA'] = gdal_path
egg_path = os.path.join(dll_path, 'py')
sys.path.append(egg_path)
libs = glob.glob(os.path.join(egg_path, '*.egg'))
for lib in libs:
    sys.path.append(lib)
