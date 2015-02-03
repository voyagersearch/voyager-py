import os
import sys
import unittest
sys.path.append(os.path.dirname(os.getcwd()))
from workers import base_job, sql_worker


class TestSQLServerTables(unittest.TestCase):
    """Test case for indexing SQL Server tables."""
    @classmethod
    def setUpClass(self):
        pass

    def test_all_tables(self):
        """Test all tables are returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'sql_server_all.json'))
        job.connect_to_database()
        base_tables_to_keep = [u'STATES_TABLE', u'STATES', u'CITIES', u'RIVERS', u'CITIESHI']
        self.assertEqual(base_tables_to_keep, sql_worker.get_tables(job))

    def test_one_table(self):
        """Test one table is returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'sql_server_one.json'))
        job.connect_to_database()
        self.assertEqual(['STATES'], sql_worker.get_tables(job))

    def test_tables_to_keep_wildcard(self):
        """Test tables using wildcards."""
        job = base_job.Job(os.path.join(os.getcwd(), 'sql_server_include_exclude.json'))
        job.connect_to_database()
        self.assertEqual([u'STATES_TABLE', u'STATES', u'CITIES', u'RIVERS'], sql_worker.get_tables(job))
