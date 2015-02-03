import os
import sys
import unittest
sys.path.append(os.path.dirname(os.getcwd()))
from workers import base_job, oracle_worker


class TestOracleViews(unittest.TestCase):
    """Test case for indexing Oracle views."""
    @classmethod
    def setUpClass(self):
        pass

    def test_all_views(self):
        """Test all views are returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'all_views_oracle.json'))
        job.connect_to_database()
        base_views_to_keep = [('GDB_ITEMRELATIONSHIPS_VW', 'GDB1'), ('GDB_ITEMS_VW', 'GDB1'), ('RUNWAY_A_EVW', 'GDB1'),
                              ('RUNWAY_L_EVW', 'GDB1'), ('RUNWAY_P_EVW', 'GDB1'), ('SLIPWAY_A_EVW', 'GDB1'),
                              ('SLIPWAY_L_EVW', 'GDB1'), ('SLIPWAY_P_EVW', 'GDB1'), ('SUBWAY_L_EVW', 'GDB1')]
        self.assertEqual(base_views_to_keep, oracle_worker.get_views(job))

    def test_one_view(self):
        """Test one view is returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'one_view_oracle.json'))
        job.connect_to_database()
        self.assertEqual([('RUNWAY_A_EVW', u'GDB1')], oracle_worker.get_views(job))

    def test_views_schema_all(self):
        """Test views for schema=all."""
        job = base_job.Job(os.path.join(os.getcwd(), 'views_schema_all_oracle.json'))
        job.connect_to_database()
        base_views = [('RUNWAY_A_EVW', u'GDB1'), ('RUNWAY_L_EVW', u'GDB1'), ('RUNWAY_P_EVW', u'GDB1')]
        self.assertEqual(base_views, sorted(oracle_worker.get_views(job)))

    def test_views_schema_user(self):
        """Test views for schema=user."""
        job = base_job.Job(os.path.join(os.getcwd(), 'views_schema_user_oracle.json'))
        job.connect_to_database()
        base_views = ['RUNWAY_A_EVW', 'RUNWAY_L_EVW', 'RUNWAY_P_EVW']
        self.assertEqual(base_views, sorted(oracle_worker.get_views(job)))

    def test_views_wildcards(self):
        """Test including & excluding views with wildcards."""
        job = base_job.Job(os.path.join(os.getcwd(), 'views_wildcards_oracle.json'))
        job.connect_to_database()
        base_views = ['RUNWAY_A_EVW', 'RUNWAY_P_EVW']
        self.assertEqual(base_views, sorted(oracle_worker.get_views(job)))
