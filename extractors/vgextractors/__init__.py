"""Voyager Extractor Modules"""


import os
import sys
import glob


here = os.path.dirname(__file__)

extractors = []
for ext in glob.glob(os.path.join(here, '*Extractor.py')):
    extractors.append(os.path.basename(ext)[:-3])

del os,sys,glob,here
__all__ = extractors
