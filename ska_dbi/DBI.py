# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
ska_dbi provides simple methods for database access and data insertion.
Features:

- Sqlite connections are supported.
- Integration with numpy record arrays.
- Verbose mode to show transaction information.
"""

import sqlite3 as dbapi2

from ska_dbi.common import DEFAULT_CONFIG


def _denumpy(x):
    """
    Try using the numpy.tolist() to convert to native python type.
    DBI's can't typically handle numpy vals."""
    try:
        return x.tolist()
    except:
        return x


class DBI(object):
    """
    Database interface class.

    Example usage::

      db = DBI(dbi='sqlite', server=dbfile, numpy=False, verbose=True)

    :param dbi:  Database interface name (sqlite)
    :param server: Server name (or file name for sqlite)
    :param user: User name (optional)
    :param autocommit: Automatically commit after each transaction.  Slower but easier to code.
    :param numpy:  Return multirow results as numpy.recarray; input vals can be numpy types
    :param verbose: Print transaction info
    :param authdir: Directory containing authorization files

    :rtype: DBI object
    """

    def __init__(
        self,
        dbi=None,
        server=None,
        numpy=True,
        autocommit=True,
        verbose=False,
        **kwargs,
    ):
        if dbi != "sqlite":
            raise ValueError(
                f"ska_dbi.DBI only supports sqlite at this time.  Got {dbi}."
            )

        self.dbi = dbi
        self.server = server or DEFAULT_CONFIG[dbi].get("server")
        self.numpy = numpy
        self.autocommit = autocommit
        self.verbose = verbose

        if self.verbose:
            print("Connecting to", self.dbi, "server", self.server)

        self.conn = dbapi2.connect(self.server)
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

        for subexpr in expr.split(";\n"):
            if vals is not None:
                args = (subexpr, vals)
            else:
                args = (subexpr,)

            if self.verbose:
                print("Running:", args)
            self.cursor.execute(*args)

        if (commit is None and self.autocommit) or commit:
            self.commit()

    def fetch(
        self,
        expr,
        vals=None,
    ):
        """
        Return a generator that will fetch one row at a time after executing with args.

        Example usage::

          for row in db.fetch(expr, vals):
              print row['column']

        :param expr: SQL expression to execute
        :param vals: Values associated with the expression (optional)

        :rtype: Generator that will get one row of database as dict() via next()
        """
        self.execute(expr, vals, commit=False)
        cols = [x[0] for x in self.cursor.description]
        while True:
            vals = self.cursor.fetchone()
            if vals:
                yield dict(zip(cols, vals, strict=False))
            else:
                if self.autocommit:
                    self.commit()
                self.cursor.close()
                break

    def fetchone(
        self,
        expr,
        vals=None,
    ):
        """Fetch one row after executing args.  This always gets the first row of the
        SQL query.  Use ska_dbi.fetch() to get multiple rows one at a time.

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
        self.execute(expr, vals, commit=False)
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
            return [dict(zip(cols, x, strict=False)) for x in vals]

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
        colrepls = ("?",) * len(cols)

        insert_str = "INSERT %s INTO %s (%s) VALUES (%s)"
        replace_str = replace and "OR REPLACE" or ""
        cmd = insert_str % (replace_str, tablename, ",".join(cols), ",".join(colrepls))

        # Finally run the insert command
        self.execute(cmd, vals, commit=commit)
        self.cursor.close()
