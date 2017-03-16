"""
Simple script to test the existance of the spacy module
Called from the java plugin.
"""
import sys

try:
    import spacy
    sys.stdout.write("%s" % True)
except Exception as e:
    sys.stdout.write("%s" % False)

