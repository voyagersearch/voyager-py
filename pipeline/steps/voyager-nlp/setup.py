from setuptools import setup
import os


with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='voyager-nlp',
    version='0.1',
    install_requires=required,
    url='https://github.com/voyagersearch/voyager-nlp/tree/ngaddc-stage2-NLP-integration',
    license='MIT',
    author='Voyager Search',
    author_email='',
    description='Voyager Natural Language Processing'
)

string = "python -m spacy.en.download"
os.system(string)
