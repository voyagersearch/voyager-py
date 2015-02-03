import os
import sys
import unittest
sys.path.append(os.path.dirname(os.getcwd()))
from workers import base_job, oracle_worker


class TestOracleTables(unittest.TestCase):
    """Test case for indexing Oracle tables."""
    @classmethod
    def setUpClass(self):
        pass

    def test_all_tables(self):
        """Test all tables are returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'all_tables_oracle.json'))
        job.connect_to_database()
        base_tables_to_keep = ['AEROFACA', 'AEROFACP', 'AGRISTRA', 'AGRISTRP', 'AQUEDCTA', 'AQUEDCTL', 'AQUEDCTP',
                               'BARRIERL', 'BLUFFL', 'BRIDGEA', 'BRIDGEL', 'BUILDA', 'BUILDP', 'BUILTUPA', 'BUILTUPP',
                               'CISTERNP', 'COASTA', 'COASTL', 'COASTP', 'COMMA', 'COMMP', 'CROPA', 'DAMA', 'DAML',
                               'DAMP', 'DANGERA', 'DANGERL', 'DANGERP', 'DISPOSEA', 'EMBANKA', 'EMBANKL', 'EXTRACTA',
                               'EXTRACTP', 'FERRYL', 'FERRYP', 'FIREBRKA', 'FORDL', 'FORDP', 'FORTA', 'FORTP',
                               'GRASSA', 'GROUNDA', 'HARBORA', 'HARBORP', 'INDL', 'INUNDA', 'LAKERESA', 'LANDFRM1A',
                               'LANDFRM2A', 'LANDFRMA', 'LANDFRML', 'LANDFRMP', 'LANDICEA', 'LANDMRKA', 'LANDMRKL',
                               'LANDMRKP', 'LOCKA', 'LOCKL', 'LOCKP', 'MARKERSP', 'MILA', 'MILL', 'MILP', 'MISCAEROP',
                               'MISCL', 'MISCP', 'MISCPOPA', 'MISCPOPP', 'MTNP', 'NUCLEARA', 'OASISA', 'OBSTRP',
                               'PHYSA', 'PIERA', 'PIERL', 'PIPEL', 'PLAZAA', 'POWERA', 'POWERL', 'POWERP', 'PROCESSA',
                               'PROCESSP', 'PUMPINGA', 'PUMPINGP', 'RAILRDL', 'RAMPA', 'ROADL', 'RAPIDSA', 'RAPIDSL',
                               'RAPIDSP', 'RIGWELLP', 'ROADL_1', 'RRTURNP', 'RRYARDA', 'RUINSA', 'RUNWAYA', 'RUNWAYL',
                               'RUNWAYP', 'SEASTRTA', 'SEASTRTL', 'SHEDL', 'SHEDP', 'SPORTA', 'STORAGEA', 'STORAGEP',
                               'SUBSTATA', 'SUBSTATP', 'SWAMPA', 'TELEL', 'TESTA', 'TEXTP', 'THERMALA', 'THERMALP',
                               'TOWERP', 'TRACKL', 'TRAILL', 'TRANSA', 'TRANSL', 'TRANSP', 'TREATA', 'TREATP',
                               'TREESA', 'TREESL', 'TREESP', 'TUNDRAA', 'TUNNELA', 'TUNNELL', 'UTILP', 'VOIDA',
                               'WATRCRSA', 'WATRCRSL', 'WELLSPRP']
        self.assertEqual(base_tables_to_keep, oracle_worker.get_tables(job))

    def test_one_table(self):
        """Test one table is returned for indexing."""
        job = base_job.Job(os.path.join(os.getcwd(), 'one_table_oracle.json'))
        job.connect_to_database()
        base_table = ['RUNWAYL']
        self.assertEqual(base_table, oracle_worker.get_tables(job))

    def test_tables_to_keep_wildcard(self):
        """Test tables using wildcards."""
        job = base_job.Job(os.path.join(os.getcwd(), 'tables_wildcards_oracle.json'))
        job.connect_to_database()
        self.assertEqual(['RUNWAYA', 'RUNWAYL'], sorted(oracle_worker.get_tables(job)))
