import os
import sys
import glob
import json
import unittest
sys.path.append(os.path.dirname(os.path.dirname(os.getcwd())))
import voyager_tasks


class TestInfoFiles(unittest.TestCase):
    """Test case for checking info files exist
    for each task and have a valid structure.
    """
    @classmethod
    def setUpClass(self):
        self.tasks = set(voyager_tasks.__all__)
        self.tasks.remove('template_task')
        self.info_dir = os.path.abspath(os.path.join(os.path.dirname(os.getcwd()), '..', 'info'))
        self.json_files = set([os.path.basename(f).split('.')[0] for f in glob.glob(os.path.join(self.info_dir, '*.info.json'))])
        self.names = []
        self.runner = set()
        self.display = set()
        files_to_test = self.json_files.intersection(self.tasks)
        for name in files_to_test:
            test_file = os.path.join(self.info_dir, '{0}.info.json'.format(name))
            with open(test_file) as f:
                print test_file
                d = json.load(f)
                self.names.append(d['name'])
                self.runner.add(d['runner'])
                self.display.add(d['display'].keys()[0])

    def test_json_exists(self):
        """Ensure an info.json file exists for each task"""
        self.assertEqual(self.tasks.issubset(self.json_files), True)

    def test_json_names(self):
        """Verify each info.json has a valid name field and value"""
        self.assertEqual(sorted(list(self.tasks)), sorted(self.names))

    def test_json_runner(self):
        self.assertEqual(len(list(self.runner)) == 1 and list(self.runner)[0] == 'python', True)

    def test_json_display(self):
        """Default display should be set to 'en' for all info.json files"""
        self.assertEqual(len(list(self.display)) == 1 and list(self.display)[0] == 'en', True)

if __name__ == '__main__':
    unittest.main()
