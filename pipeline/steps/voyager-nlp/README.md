# Voyager Natural Language Processing

This repository contains code for extracting named entities (place names, etc...) from text through
the use of natural language processing (NLP)l.


## Dependencies

The primary dependency to perform NLP is the [spacy](https://spacy.io/) library. See [requirements.txt](requirements.txt) for the entire list of dependencies.


## Setup

To install the Python dependencies required for NLP:

1. Run the setup.bat file located in the voyager-nlp folder.

By running the setup.bat, the Python libaries with the requirements.text will be installed as well as the English language model that spacy requires:


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
