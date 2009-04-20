import os
import unittest
from Ska.DBI import DBI

dbfile = 'ska_dbi_test.sql3'
dbcache = {}

class DBI_BaseTests(unittest.TestCase):
    def setUp(self):
        self.classname = str(self.__class__)
        self.db = dbcache.get(self.classname)

    def test_05_force_drop_table(self):
        try:
            self.db.execute('drop table ska_dbi_test_table')
        except self.db.Error:
            pass

    def test_10_create_table(self):
        # Test execute with multiple cmds separated by ';\n'
        self.db.execute(open('ska_dbi_test_table.sql').read().strip())

    def test_15_insert_data(self):
        for id_ in range(3):
            data = dict(id=id_, tstart=2.+id_, tstop=3.+id_, obsid=4+id_, pcad_mode='npnt',
                        aspect_mode='kalm', sim_mode='stop')
            self.db.insert(data, 'ska_dbi_test_table')

    def test_20_fetchall(self):
        self.rows = self.db.fetchall('select * from ska_dbi_test_table')
        self.assertEqual(len(self.rows), 3)
        self.assertEqual(self.rows[1]['id'], 1)

    def test_25_insert_row_from_db(self):
        rows = self.db.fetchall('select * from ska_dbi_test_table')
        row = rows[0]
        row['id'] += 10
        row['tstart'] = 5
        self.db.insert(row, 'ska_dbi_test_table')
        
    def test_30_fetchone(self):
        row = self.db.fetchone('select * from ska_dbi_test_table')
        self.assertEqual(row['obsid'], 4)

    def test_35_fetch(self):
        for i, row in enumerate(self.db.fetch('select * from ska_dbi_test_table')):
            self.assertAlmostEqual(row['tstart'], 2.+i)

    def test_40_fetch_null(self):
        for row in self.db.fetch('select * from ska_dbi_test_table where id=100000'):
            self.fail()

    def test_45_fetchone_null(self):
        row = self.db.fetchone('select * from ska_dbi_test_table where id=100000')
        self.assertEqual(row, None)

    def test_50_fetchall_null(self):
        rows = self.db.fetchall('select * from ska_dbi_test_table where id=100000')
        self.assertEqual(len(rows), 0)

    def test_55_drop_table(self):
        self.db.execute('drop table ska_dbi_test_table')

    def test_60_disconnect(self):
        self.db.cursor.close()
        self.db.conn.close()
        del dbcache[self.classname]

    def test_65_cleanup(self):
        if os.path.exists(dbfile):
            os.unlink(dbfile)

class SqliteWithNumpy(DBI_BaseTests):
    def test_00_connect(self):
        dbcache[self.classname] = DBI(dbi='sqlite', server=dbfile, numpy=True)

class SqliteWithoutNumpy(DBI_BaseTests):
    def test_00_connect(self):
        dbcache[self.classname] = DBI(dbi='sqlite', server=dbfile, numpy=False)

class SybaseWithNumpy(DBI_BaseTests):
    def test_00_connect(self):
        dbcache[self.classname] = DBI(dbi='sybase', numpy=True)

class SybaseWithoutNumpy(DBI_BaseTests):
    def test_00_connect(self):
        dbcache[self.classname] = DBI(dbi='sybase', server='sybase', user='aca_ops',
                                      database='aca', numpy=False)

def test_all():
    """Return a suite of all tests in the four test classes above"""
    suite = unittest.TestSuite()
    for testclass in (SqliteWithNumpy, SqliteWithoutNumpy, SybaseWithNumpy, SybaseWithoutNumpy):
        # Get all tests from each test class and add to suite
        tests = unittest.TestLoader().loadTestsFromTestCase(testclass)
        suite.addTest(tests)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(test_all())
    
