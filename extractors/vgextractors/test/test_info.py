import os
import sys
import json
import subprocess
import tempfile
import unittest
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../..")))
import vgextractors


class TestExtractors(unittest.TestCase):
    """Test case for Python extractors."""
    @classmethod
    def setUpClass(self):
        self.working_folder = os.path.abspath(os.path.join(os.getcwd(), "../.."))
        self.voyager_worker = os.path.join(self.working_folder, 'VoyagerWorkerPy.py')
        self.extractors = set(vgextractors.__all__)

    def test_extractors_exists(self):
        """Ensure an info.json file exists for each task"""
        self.assertEqual(self.extractors, set(['AvroExtractor', 'JSONExtractor']))

    def test_info_argument(self):
        """Test the --info argument to the VoyagerWorker.py to ensure valid extractor info is returned"""
        out, err = subprocess.Popen('"{0}" --info'.format(self.voyager_worker), stdout=subprocess.PIPE, shell=True).communicate()
        expected_value = '{"extractors": [{"formats": [{"priority": 10, "mime": "application/json", "name": "text"}], "name": "json", "description": "Extract a JSON file using Python"}, {"name": "avro", "description": "extract avro file information"}], "properties": {"version.python": "2.7.12"}, "version": "1.9", "name": "VoyagerWorkerPy", "description": "Python worker"}\n'
        self.assertEqual(expected_value, out)

    def test_json_extractor(self):
        with open('json_entry.json', 'rb') as fp:
            test_json = json.load(fp)
        test_json['file'] = os.path.join(self.working_folder, 'vgextractors/test/test_file.json')
        test_json['path'] = os.path.join(self.working_folder, 'vgextractors/test/test_file.json')
        tmp_file = tempfile.NamedTemporaryFile('wb', suffix='json', delete=False)
        json.dump(test_json, tmp_file)
        tmp_file.close()
        out, err = subprocess.Popen('"{0}" --job "{1}"'.format(self.voyager_worker, tmp_file.name), stdout=subprocess.PIPE, shell=True).communicate()
        self.assertIn('clip_data', out)
        self.assertIn(">>STATUS>>X=SUCCESS>>STATUS", out)

if __name__ == '__main__':
    unittest.main()
