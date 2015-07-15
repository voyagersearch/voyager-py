import urllib, os, zipfile, glob
from os.path import join

root = '{}/data'.format(os.path.abspath(os.path.dirname(__file__)))

def download_file(url, file):
  f = join(root, file)
  if not os.path.exists(f):
    print('Downloading {} to {}'.format(url, f))
    urllib.urlretrieve(url, f)

def unzip_file(file):
  f = join(root, file)
  print('Unzipping {} to {}'.format(file, root))
  with zipfile.ZipFile(f) as zf:
    zf.extractall(root)

def rename_all(src, dst):
  for f in glob.glob('{}/{}.*'.format(root, src)):
    ext = os.path.splitext(f)[1]
    os.rename(f, '{}/{}{}'.format(root, dst, ext))

if __name__ == '__main__':
  f = download_file('ftp://ftp2.census.gov/geo/tiger/TIGER2014/STATE/tl_2014_us_state.zip', 'states.zip')
  unzip_file('states.zip')
  rename_all('tl_2014_us_state', 'states')