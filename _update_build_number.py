"""Updates the build number in the README.md."""
import os


build_num = os.environ['BUILD_NUMBER']
readme_dir = os.getcwd()
in_readme = open(os.path.join(readme_dir, 'README.md')).read()
out_readme = open(os.path.join(readme_dir, 'README.md'), 'w')
in_readme = in_readme.replace('BUILD_NUMBER', build_num)
out_readme.write(in_readme)
out_readme.close()
