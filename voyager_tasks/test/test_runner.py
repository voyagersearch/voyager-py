"""Voyager unittest runner for tasks."""
import os
import glob
import unittest
import xmlrunner


if __name__ == '__main__':
    report_location = os.path.join(os.getcwd(), 'test-reports')
    if not os.path.exists(report_location):
        os.mkdir(report_location)
    test_file_strings = glob.glob('test_*.py')
    module_strings = [test_name[0:len(test_name)-3] for test_name in test_file_strings]
    suites = [unittest.defaultTestLoader.loadTestsFromName(test_name) for test_name in module_strings]
    testSuite = unittest.TestSuite(suites)
    for i, unit_test in enumerate(suites, 0):
        with open(os.path.join(report_location, '{0}.xml'.format(module_strings[i])), 'a') as results_log:
            xmlrunner.XMLTestRunner(results_log).run(unit_test)
