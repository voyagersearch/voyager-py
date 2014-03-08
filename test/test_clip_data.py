"""
A script to test the data access ListDomains.
"""
import os
import sys
sys.path.append(r"C:\NewfoundGEO\Clients\Voyager\voyager-processing\scripts")
import VoyagerTaskRunner
import logging
import unittest
import io
import datetime
import arcpy



class MyTest(unittest.TestCase):
    __owner__ = "Jason Pardy"

    @classmethod
    def setUpClass(self):
        self.layers_json = os.path.join(os.path.dirname(__file__), 'test_clip_layerfiles.json')
    # End setUpClass

    @classmethod
    def tearDownClass(self):
        pass

    def setUp(self):
        """Sets up the test logger to capture errors."""
        self.logger = logging.getLogger('test_logger')
        self.logger.setLevel(logging.ERROR)

        # Setup the console handler with a StringIO object
        self.log_capture_string = io.StringIO()
        self.ch = logging.StreamHandler(self.log_capture_string)
        self.ch.setLevel(logging.ERROR)

        # Add a formatter
        self.formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        self.ch.setFormatter(self.formatter)

        # Add the console handler to the logger
        self.logger.addHandler(self.ch)

    def tearDown(self):
        self.logger.removeHandler(self.ch)
        self.ch.close()
        del self.ch, self.logger

    def test_clip_data(self):
        """Testing clip_data"""
        VoyagerTaskRunner.run_task(self.layers_json)
        log_contents = self.log_capture_string.getvalue()
        self.assertEqual(log_contents, '')

if __name__=='__main__':
    unittest.main()