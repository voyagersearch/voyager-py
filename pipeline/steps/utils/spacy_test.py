"""
Simple script to test the existance of the spacy module
Called from the java plugin.
"""
import os
import glob
import sys
import platform


# Import required Python libraries required for NLP.
def append_or_set_path(path):
  try:
    p = os.environ['PATH']
    if len(p) > 0 and not p.endswith(os.pathsep):
        p += os.pathsep
    p += path
    os.environ['PATH'] = p
  except KeyError:
    os.environ['PATH'] = path

# Add Python dependent libraries to the system paths.
arch_dir = 'win32_x86'
if platform.system() == 'Darwin':
    arch_dir = 'darwin_x86_64'
elif platform.system() == 'Linux':
    arch_dir = 'linux_amd64'

dll_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), '..', 'arch', arch_dir))
append_or_set_path(dll_path)

egg_path = os.path.join(dll_path, 'py')
sys.path.append(egg_path)
sys.path.append(os.path.dirname(dll_path))
libs = glob.glob(os.path.join(egg_path, '*.egg'))
for lib in libs:
    sys.path.append(lib)

# Type to import spacy.
try:
    import spacy
    sys.stdout.write("%s" % True)
except Exception as e:
    sys.stdout.write("%s" % False)

