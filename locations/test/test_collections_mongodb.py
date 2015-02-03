import os
import sys
import unittest
sys.path.append(os.path.dirname(os.getcwd()))
from workers import base_job, mongodb_worker


class TestMongoDBCollections(unittest.TestCase):
    """Test case for indexing MongoDB collections."""
    @classmethod
    def setUpClass(self):
        pass

    def test_all_collections(self):
        """Test all collections are returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'mongodb_all.json'))
        job.connect_to_database()
        base_collections_to_keep = ['zips', 'fs.chunks', 'fs.files']
        self.assertEqual(base_collections_to_keep, mongodb_worker.get_collections(job))

    def test_one_collection(self):
        """Test one collection is returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'mongodb_one.json'))
        job.connect_to_database()
        self.assertEqual(['zips'], mongodb_worker.get_collections(job))

    def test_collections_to_keep_wildcard(self):
        """Test collections using wildcards."""
        job = base_job.Job(os.path.join(os.getcwd(), 'mongodb_include_exclude.json'))
        job.connect_to_database()
        self.assertEqual(['fs.chunks', 'fs.files'], mongodb_worker.get_collections(job))
