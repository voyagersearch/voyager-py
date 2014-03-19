"""Updates the build number in the README.md."""
import os
#import re

readme_dir = os.getcwd()

# with open(os.path.join(readme_dir, 'README.md'), 'r') as readme:
#     for l in readme:
#         if l.startswith('Build:'):
#             result = re.findall(r'\d+', l)
#             if result and isinstance(int(result[0]), int):
#                 build_num = int(result[0])
#                 new_build_num = build_num + 1
#                 break
build_num = int(os.environ['BUILD_NUM'])
new_build_num = build_num + 1

in_readme = open(os.path.join(readme_dir, 'README.md')).read()
out_readme = open(os.path.join(readme_dir, 'README.md'), 'w')
in_readme = in_readme.replace('BUILD_NUMBER', str(new_build_num))
out_readme.write(in_readme)
out_readme.close()
