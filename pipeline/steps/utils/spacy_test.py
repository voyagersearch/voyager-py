"""
Simple script to test the existance of the spacy module
Called from the java plugin.
"""
import os
import glob
import sys


# Import required Python libraries required for NLP.
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'nlp-libs'))
egg_files = glob.glob(os.path.join(path, '*.egg'))
egg_files += glob.glob(os.path.join(path, '*', '*.egg'))
for egg_file in egg_files:
    sys.path.append(egg_file)

try:
    import spacy
    sys.stdout.write("%s" % True)
except Exception as e:
    sys.stdout.write("%s" % False)

