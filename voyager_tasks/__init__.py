import os

__tasks__ = []

for module in os.listdir(os.path.dirname(__file__)):
    if module == '__init__.py' or module[-3:] != '.py':
        continue
    __tasks__.append(module[:-3])
    #__import__(module[:-3])
del module
