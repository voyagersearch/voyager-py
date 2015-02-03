import os
import sys
import unittest
sys.path.append(os.path.dirname(os.getcwd()))
from workers import base_job, mysql_worker


class TestMySQLTables(unittest.TestCase):
    """Test case for indexing MySQL tables."""
    @classmethod
    def setUpClass(self):
        pass

    def test_all_tables(self):
        """Test all tables are returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'my_sql_all.json'))
        job.connect_to_database()
        base_tables_to_keep = ['books', 'cities', 'states']
        self.assertEqual(base_tables_to_keep, mysql_worker.get_tables(job))

    def test_one_table(self):
        """Test one table is returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'my_sql_one.json'))
        job.connect_to_database()
        self.assertEqual(['states'], mysql_worker.get_tables(job))

    def test_tables_to_keep_wildcard(self):
        """Test tables using wildcards."""
        job = base_job.Job(os.path.join(os.getcwd(), 'my_sql_include_exclude.json'))
        job.connect_to_database()
        self.assertEqual(['cities', 'states'], mysql_worker.get_tables(job))
