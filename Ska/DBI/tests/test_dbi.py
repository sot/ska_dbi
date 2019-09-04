# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
Usage:

  % py.test test.py
"""

import os
import pytest
import tempfile
import numpy as np
from Ska.DBI import DBI


# If the SYBASE_OCS environment variable is set (from flt_envs) and the module exists
# on the system, do the Sybase tests.
HAS_SYBASE = ('SYBASE_OCS' in os.environ and
              os.path.exists(
                os.path.join(os.environ['SYBASE'],
                             os.environ['SYBASE_OCS'],
                             'python', 'python34_64r', 'lib', 'sybpydb.so')))


with open(os.path.join(os.path.dirname(__file__), 'ska_dbi_test_table.sql')) as fh:
    TEST_TABLE_SQL = fh.read().strip()


class DBI_BaseTests(object):
    def setup_class(cls):
        cls.db = DBI(**cls.db_config)

    def teardown_class(cls):
        # No matter what try to drop the testing table.  Normally this should
        # fail as a result of test_55.
        try:
            cls.db.execute('drop table ska_dbi_test_table')
        except:
            pass
        cls.db.cursor.close()
        cls.db.conn.close()

    # Run an empty test to get the warning for no file (if running on a file)
    # Ignore that warning in these tests.
    @pytest.mark.filterwarnings("ignore:Database file missing")
    def test_00_warning(self):
        pass

    def test_05_force_drop_table(self):
        try:
            self.db.execute('drop table ska_dbi_test_table')
        except self.db.Error:
            pass

    def test_10_create_table(self):
        # Test execute with multiple cmds separated by ';\n'
        self.db.execute(TEST_TABLE_SQL)

    def test_15_insert_data(self):
        for id_ in range(3):
            data = dict(id=id_, tstart=2. + id_, tstop=3. + id_, obsid=4 + id_,
                        pcad_mode='npnt', aspect_mode='kalm', sim_mode='stop')
            self.db.insert(data, 'ska_dbi_test_table')

    def test_20_fetchall(self):
        self.rows = self.db.fetchall('select * from ska_dbi_test_table')
        assert len(self.rows) == 3
        assert self.rows[1]['id'] == 1

    def test_25_insert_row_from_db(self):
        rows = self.db.fetchall('select * from ska_dbi_test_table')
        row = rows[0]
        row['id'] += 10
        row['tstart'] = 5
        self.db.insert(row, 'ska_dbi_test_table')

    def test_30_fetchone(self):
        row = self.db.fetchone('select * from ska_dbi_test_table')
        assert row['obsid'] == 4

    def test_35_fetch(self):
        for i, row in enumerate(self.db.fetch('select * from ska_dbi_test_table')):
            assert np.allclose(row['tstart'], 2. + i)

    def test_40_fetch_null(self):
        for row in self.db.fetch('select * from ska_dbi_test_table where id=100000'):
            assert False

    def test_45_fetchone_null(self):
        row = self.db.fetchone('select * from ska_dbi_test_table where id=100000')
        assert row is None

    def test_50_fetchall_null(self):
        rows = self.db.fetchall('select * from ska_dbi_test_table where id=100000')
        assert len(rows) == 0

    def test_55_drop_table(self):
        self.db.execute('drop table ska_dbi_test_table')


class TestSqliteWithNumpyFile(DBI_BaseTests):
    fh, fn = tempfile.mkstemp(suffix='.db3')
    db_config = dict(dbi='sqlite', server=fn, numpy=True)
    os.unlink(fn)


class TestSqliteWithoutNumpyFile(DBI_BaseTests):
    fh, fn = tempfile.mkstemp(suffix='.db3')
    db_config = dict(dbi='sqlite', server=fn, numpy=False)
    os.unlink(fn)


class TestSqliteWithNumpyMemory(DBI_BaseTests):
    db_config = dict(dbi='sqlite', server=':memory:', numpy=True)


class TestSqliteWithoutNumpyMemory(DBI_BaseTests):
    db_config = dict(dbi='sqlite', server=':memory:', numpy=False)


@pytest.mark.skipif('not HAS_SYBASE', reason='No Sybase support for Python 3')
class TestSybaseWithNumpy(DBI_BaseTests):
    db_config = dict(dbi='sybase', server='sybase', user='aca_test',
                     database='aca_tstdb', numpy=True)


@pytest.mark.skipif('not HAS_SYBASE', reason='No Sybase support for Python 3')
class TestSybaseWithoutNumpy(DBI_BaseTests):
    db_config = dict(dbi='sybase', server='sybase', user='aca_test',
                     database='aca_tstdb', numpy=False)


def test_context_manager():
    with DBI(dbi='sqlite', server=':memory:') as db:
        db.execute(TEST_TABLE_SQL)
        for id_ in range(3):
            data = dict(id=id_, tstart=2. + id_, tstop=3. + id_, obsid=4 + id_,
                        pcad_mode='npnt', aspect_mode='kalm', sim_mode='stop')
            db.insert(data, 'ska_dbi_test_table')
        rows = db.fetchall('select * from ska_dbi_test_table')
        assert len(rows) == 3
        assert rows[1]['id'] == 1

    # check that access fails now
    with pytest.raises(Exception):
        rows = db.fetchall('select * from ska_dbi_test_table')
