"""
Ska.DBI provides simple methods for database access and data insertion.
Features:

- Sqlite and sybase connections are supported.
- Automatic fetching of Ska database account passwords.
- Integration with numpy record arrays.
- Verbose mode to show transaction information.
- Insert method smooths over syntax differences between sqlite and sybase.
"""
from __future__ import print_function, division, absolute_import

import os
from six.moves import zip
from six import next

supported_dbis = ('sqlite', 'sybase')


def _denumpy(x):
    """
    Try using the numpy.tolist() to convert to native python type.
    DBI's can't typically handle numpy vals."""
    try:
        return x.tolist()
    except:
        return x


class NoPasswordError(Exception):
    """
    Special Error for the case when password is neither supplied nor available
    from a file.
    """
    pass


class DBI(object):
    """
    Database interface class.

    Example usage::

      db = DBI(dbi='sqlite', server=dbfile, numpy=False, verbose=True)
      db = DBI(dbi='sybase', server='sybase', user='aca_ops', database='aca')
      db = DBI(dbi='sybase')   # Use defaults (same as above)

    :param dbi:  Database interface name (sqlite, sybase)
    :param server: Server name (or file name for sqlite)
    :param user: User name (optional)
    :param passwd: Password (optional).  Read from aspect authorization if required and not supplied.
    :param database: Database name for sybase (default = SKA_DATABASE env. or package default 'aca').
    :param autocommit: Automatically commit after each transaction.  Slower but easier to code.
    :param numpy:  Return multirow results as numpy.recarray; input vals can be numpy types
    :param verbose: Print transaction info
    :param authdir: Directory containing authorization files

    :rtype: DBI object
    """
    def __init__(self, dbi=None, server=None, user=None, passwd=None, database=None,
                 numpy=True, autocommit=True, verbose=False,
                 authdir='/proj/sot/ska/data/aspect_authorization',
                 **kwargs):

        DEFAULTS = {'sqlite': {'server': 'db.sql3'},
                    'sybase': {'server': 'sybase',
                               'user': 'aca_ops',
                               'database': 'aca'}}

        if dbi not in supported_dbis:
            raise ValueError('dbi = %s not supported - allowed = %s' % (dbi, supported_dbis))

        self.dbi = dbi
        self.server = server or DEFAULTS[dbi].get('server')
        self.user = user or DEFAULTS[dbi].get('user')
        self.database = (database
                         or os.environ.get('SKA_DATABASE')
                         or DEFAULTS[dbi].get('database'))
        self.passwd = passwd
        self.numpy = numpy
        self.autocommit = autocommit
        self.verbose = verbose

        if self.verbose:
            print('Connecting to', self.dbi, 'server', self.server)

        if dbi == 'sqlite':
            import sqlite3 as dbapi2
            self.conn = dbapi2.connect(self.server)

        elif dbi == 'sybase':
            # HEAD-specific initialization of SYBASE envvar if needed
            if 'SYBASE' not in os.environ:
                os.environ['SYBASE'] = '/soft/SYBASE_OCS15'

            import Sybase as dbapi2
            if self.passwd is None:
                try:
                    passwd_file = os.path.join(authdir, '%s-%s-%s' % (self.server, self.database, self.user))
                    self.passwd = open(passwd_file).read().strip()
                    if self.verbose:
                        print('Using password from', passwd_file)
                except IOError as e:
                    raise NoPasswordError("None supplied and unable to read password file %s" % e)


            self.conn = dbapi2.connect(self.server, self.user, self.passwd, self.database, **kwargs)

        self.Error = dbapi2.Error

    def __enter__(self):
        """Context manager enter runtime context.  No action required, just return self."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit run time context.

        Close connection.  By the implicit "return None" this will raise any exceptions
        after closing.
        """
        self.conn.close()

    def commit(self):
        """Commit transactions"""
        self.conn.commit()

    def execute(self, expr, vals=None, commit=None):
        """
        Run ``self.cursor.execute(expr, vals)`` with possibility of verbose output and commit.

        Multiple commands can by executed by separating them with a semicolon at
        the end of a line.  If ``vals`` are supplied they will be applied to
        each of the commands.

        :param expr: SQL expression to execute
        :param vals: Values associated with the expression (optional)
        :param commit: Commit after executing C{expr} (default = self.autocommit)

        :rtype: None
        """
        # Get a new cursor (implicitly closing any previous cursor)
        self.cursor = self.conn.cursor()

        for subexpr in expr.split(';\n'):
            if vals is not None:
                args = (subexpr, vals)
            else:
                args = (subexpr,)

            if self.verbose:
                print('Running:', args)
            self.cursor.execute(*args)

        if (commit is None and self.autocommit) or commit:
            self.commit()

    def fetch(self, expr, vals=None,):
        """
        Return a generator that will fetch one row at a time after executing with args.

        Example usage::

          for row in db.fetch(expr, vals):
              print row['column']

        :param expr: SQL expression to execute
        :param vals: Values associated with the expression (optional)

        :rtype: Generator that will get one row of database as dict() via next()
        """
        self.execute(expr, vals)
        cols = [x[0] for x in self.cursor.description]
        while True:
            vals = self.cursor.fetchone()
            if vals:
                yield dict(zip(cols, vals))
            else:
                if self.autocommit:
                    self.commit()
                self.cursor.close()
                break

    def fetchone(self, expr, vals=None,):
        """Fetch one row after executing args.  This always gets the first row of the
        SQL query.  Use Ska.DBI.fetch() to get multiple rows one at a time.

        Example usage::

          row = db.fetchone(expr, vals)
          print row['column']

        :param expr: SQL expression to execute
        :param vals: Values associated with the expression (optional)

        :rtype: One row of database as dict()
        """
        try:
            val = next(self.fetch(expr, vals))
            self.cursor.close()
            return val
        except StopIteration:
            return None

    def fetchall(self, expr, vals=None):
        """Fetch all rows after executing args.

        Example usage::

          rows = db.fetchall(expr, vals)
          print rows[1:5]['column']

        :param expr: SQL expression to execute
        :param vals: Values associated with the expression (optional)

        :rtype: All rows of database as numpy.rec.recarray or list of dicts, depending on self.numpy
        """
        self.execute(expr, vals)
        cols = [x[0] for x in self.cursor.description]
        vals = self.cursor.fetchall()

        if self.autocommit:
            self.commit()

        self.cursor.close()

        if self.numpy and vals:
            import numpy
            # Would be good to set dtype explicitly from database info instead of
            # having numpy auto-determine types
            return numpy.rec.fromrecords(vals, names=cols)
        else:
            return [dict(zip(cols, x)) for x in vals]

    def insert(self, row, tablename, replace=False, commit=None):
        """Insert data row into table tablename.

        :param row: Data row for insertion (dict or numpy.record)
        :param tablename: Table name
        :param replace: If true then replace database record if it already exists
        :param commit: Commit insertion (default = self.autocommit)

        :rtype: None
        """

        # Get the column names, either from numpy methods or from dict keys
        try:
            cols = sorted(row.dtype.names)
        except AttributeError:
            cols = sorted(row.keys())

        # Make a tuple of the values to insert
        if self.numpy:
            vals = tuple(_denumpy(row[x]) for x in cols)
        else:
            vals = tuple(row[x] for x in cols)

        # Create the insert command depending on dbi.  Start with the column
        # value replacement strings
        if self.dbi == 'sqlite':
            colrepls = ('?',) * len(cols)
        elif self.dbi == 'sybase':
            if replace:
                raise ValueError('Using replace=True not allowed for Sybase DBI')
            colrepls = tuple('@'+x for x in cols)
            vals = dict(zip(colrepls, vals))

        insert_str = "INSERT %s INTO %s (%s) VALUES (%s)"
        replace_str = replace and 'OR REPLACE' or ''
        cmd = insert_str % (replace_str, tablename,
                            ','.join(cols),
                            ','.join(colrepls))

        # Finally run the insert command
        self.execute(cmd, vals, commit=commit)
        self.cursor.close()
