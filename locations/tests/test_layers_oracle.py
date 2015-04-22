import os
import sys
import unittest
sys.path.append(os.path.dirname(os.getcwd()))
from workers import base_job, oracle_worker


class TestOracleLayers(unittest.TestCase):
    """Test case for indexing Oracle layers."""
    @classmethod
    def setUpClass(self):
        pass

    def test_all_layers(self):
        """Test all layers are returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'all_layers_oracle.json'))
        job.connect_to_database()
        base_layers_to_keep = 490
        self.assertEqual(base_layers_to_keep, len(oracle_worker.get_layers(job)))

    def test_one_layer(self):
        """Test one layer is returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'one_layer_oracle.json'))
        job.connect_to_database()
        self.assertEqual([(u'COUNTRY', u'GDB1')], oracle_worker.get_layers(job))

    def test_layers_to_keep_wildcard(self):
        """Test layers using wildcards."""
        job = base_job.Job(os.path.join(os.getcwd(), 'layers_wildcards_oracle.json'))
        job.connect_to_database()
        self.assertEqual([(u'LOCK_A', u'GDB1'), (u'LOCK_L', u'GDB1')], sorted(oracle_worker.get_layers(job)))
