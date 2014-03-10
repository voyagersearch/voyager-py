"""Voyager unittest runner."""
import os
import glob
import unittest
import xmlrunner


if __name__ == '__main__':
    test_location = os.getcwd()
    report_location = os.path.join(test_location, 'test-reports')
    if not os.path.exists(report_location):
        os.mkdir(report_location)
    test_scripts = glob.glob('{0}/test_*.py'.format(test_location))
    scriptToModuleName = lambda f: os.path.splitext(os.path.basename(f))[0]
    module_names = map(scriptToModuleName, test_scripts)
    modules = map(__import__, module_names)
    load = unittest.defaultTestLoader.loadTestsFromModule

    suite = unittest.TestSuite(map(load, modules))
    for i, unit_test in enumerate(suite._tests, 0):
        print 'Running test: {0}'.format(test_scripts[i])
        test_case_name = unit_test._tests[0]._tests[0].__module__
        with open(os.path.join(report_location, '{0}.xml'.format(test_case_name)), 'w') as results_log:
            xmlrunner.XMLTestRunner(results_log).run(unit_test)


