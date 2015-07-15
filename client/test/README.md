# Voyager Python Client Tests

This directory contains unit tests for the voyager python client.

## Setup

Before tests can be run a few requirements must be met.

1. Install the [pysolr](https://github.com/toastdriven/pysolr) library.

        pip install pysolr

1. Setup data required for the tests.

        python get_data.py

1. Start a voyager server instance and ensure it is accessible at http://localhost:8888 with the the default administration credentials.

## Running Tests

All tests can be run by executing `run.py`:

    python run.py

Individual tests can be run by invoking the unittest module directly:

    python -m unittest discover -p 'smoke_test.py'

