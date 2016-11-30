# Voyager Natural Language Processing

This folder contains code for supporting Voyager NLP pipeline step for extracting named entities (place names, etc...) from text through
the use of natural language processing (NLP). 

This folder (voyager-nlp) can actually be copied to a different location which includes a different computer where the service can be run.


## Dependencies

The primary dependency to perform NLP is the [spacy](https://spacy.io/) library. See [requirements.txt](requirements.txt) for the entire list of dependencies.


## Setup

To install the Python dependencies required for NLP:

1. Ensure Python 2.7.10, 2.7.11 or 2.7.12 is installed.
2. After confirming Python is installed, run the setup.bat file (on windows) or setup.sh (on osx/linux) located in the voyager-nlp folder. By running this file, the Python libraries within the requirements.txt will be installed as well as the English language model that spacy requires. Setup.bat may have to be run with Administrative privileges if errors occur. 
3. Edit the settings.py script to specify the valid service address and port number, and modify the path to the folder you would like the logs to be written to. 
4. Start the NLP service by running the nlp_service.py. (ie: python nlp_service.py)

## Contents

* <i>bottle.py</i> -- A fast and simple micro-framework for small web applications. Required for the nlp_service.py.

* <i>nlp_service.py</i> -- A web-based service for parsing text with spacy. 
 
* <i>setttings.py</i> -- Contains settings for service address (i.e. localhost), service port and log file location.

* <i>lingustic_features.py</i> -- Includes functions to parse text and tag the entities. 

## Named Entity Types

The following Named Entities types are extracted and tagged in Voyager:

* PERSON:  People, including fictional.
* NORP:    Nationalities or religious or political groups.
* FAC: Facilities, such as buildings, airports, highways, bridges, etc.
* ORG: Companies, agencies, institutions, etc.
* GPE: Countries, cities, states.
* LOC: Non-GPE locations, mountain ranges, bodies of water.
* PRODUCT: Vehicles, weapons, foods, etc. (Not services)
* EVENT:   Named hurricanes, battles, wars, sports events, etc.
* WORK_OF_ART: Titles of books, songs, etc.
* LAW: Named documents made into laws
* LANGUAGE:    Any named language
