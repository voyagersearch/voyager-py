import os
import glob
import re
import unittest
import xmlrunner


class TestSetup:
    def __init__(self):
        self.testLocation = os.path.dirname(__file__)

    def unitTests(self):
        testPath = os.path.abspath(self.testLocation)
        testScripts = glob.glob('{0}/test_*.py'.format(self.testLocation))
        scriptToModuleName = lambda f: os.path.splitext(os.path.basename(f))[0]
        moduleNames = map(scriptToModuleName, testScripts)
        modules = map(__import__, moduleNames)
        load = unittest.defaultTestLoader.loadTestsFromModule

        suite = unittest.TestSuite(map(load, modules))
        i = 0
        for uTest in suite._tests:
            print 'Running test: %s' %testScripts[i]
            with open(os.path.join(self.testLocation, 'test-reports', '{0}.xml'.format(uTest._tests[0]._tests[0].__module__)), 'w') as resultsLog:
                xmlrunner.XMLTestRunner(resultsLog).run(uTest)
            i+=1

if __name__ == '__main__':
    ts = TestSetup()
    ts.unitTests()
