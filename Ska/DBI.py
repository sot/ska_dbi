import os
SKA = os.environ['SKA'] or '/proj/sot/ska'
authdir = os.path.join('/proj/sot/ska', 'data/aspect_authorization')
supported_dbis = ('sqlite', 'sybase')

def _denumpy(x):
    """Try using the numpy.tolist() to convert to native python type.
    DBI's can't typically handle numpy vals."""
    try:
        return x.tolist()
    except:
        return x

class DbiSimple(object):
    def __init__(self, dbi=None, server=None, user=None, passwd=None, database=None,
                 numpy=False, autocommit=True, verbose=False, **kwargs):
        """Initialize DbiSimple object. 

         dbi:  Database interface name (sqlite, sybase)
         server: Server name (or file name for sqlite)
         user: User name (optional)
         passwd: Password (optional).  Read from aspect authorization if required and not supplied
         database: Database name for sybase (optional).
         autocommit: Automatically commit after each transaction.  Slower but easier to code.
         numpy:  Return multirow results as numpy.recarray; input vals can be numpy types
         verbose: Print transaction info.
         """

        self.dbi = dbi
        self.server = server
        self.user = user
        self.passwd = passwd
        self.database = database
        self.numpy = numpy
        self.autocommit = autocommit
        self.verbose = verbose
        
        if dbi is None:
            raise TypeError, 'Need a string value for dbi param'

        if self.verbose:
            print 'Connecting to', self.dbi, 'server', self.server

        if dbi == 'sqlite':
            from pysqlite2 import dbapi2
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
                        print 'Using password from', passwd_file
                except:
                    # Several things might go wrong, but in any case forget about auto-setting passwd
                    pass

            self.conn = dbapi2.connect(self.server, self.user, self.passwd, self.database, **kwargs)

        else:
            raise ValueError, 'dbi = %s not supported - allowed = %s' % (dbi, supported_dbis)
        
        self.Error = dbapi2.Error
        self.cursor = self.conn.cursor()

    def commit(self):
        """Commit transactions"""
        self.conn.commit()

    def execute(self, expr, vals=None, commit=None):
        """Execute the expression with optional vals.  If commit is true then commit afterward."""
        expr = ' '.join(expr.splitlines())
        args = vals is not None and (expr, vals) or (expr,)
        if self.verbose:
            print 'Running:', args
        self.cursor.execute(*args)
        if (commit is None and self.autocommit) or commit:
            self.commit()
        
    def fetch(self, *args):
        """Return a generator that will fetch one row at a time after executing with args.
        Row is returned as a dict.  Example:
          for row in db.fetch(expr, vals):
              print row['column']"""
        self.execute(*args)
        cols = [x[0] for x in self.cursor.description]
        while True:
            vals = self.cursor.fetchone()
            if vals:
                yield dict(zip(cols, vals))
            else:
                if self.autocommit:
                    self.commit()
                break
            
    def fetchone(self, *args):
        """Fetch one row after executing args.  Row is returned as a dict."""
        try:
            return self.fetch(*args).next()
        except StopIteration:
            return None
            
    def fetchall(self, *args):
        """Fetch all rows after executing args.  Rows are returned either as
        a list of dicts or a numpy.rec.recarray, depending on self.numpy."""

        self.execute(*args)
        cols = [x[0] for x in self.cursor.description]
        vals = self.cursor.fetchall()

        if self.autocommit:
            self.commit()

        if self.numpy and vals:
            import numpy
            return numpy.rec.fromrecords(vals, names=cols)
        else:
            return [dict(zip(cols, x)) for x in vals]

    def insert(self, row, tablename, replace=False, commit=None):
        """Insert data row into table tablename.  Args:

         row: Data row for insertion (dict or numpy.record)
         tablename: Table name
         replace: If true then replace database record if it already exists
         commit: Commit insertion (default = self.autocommit)
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
            colrepls = tuple('@'+x for x in cols)
            vals = dict(zip(colrepls, vals))
        else:
            raise ValueError, 'Unsupported dbi'
        
        insert_str = "INSERT %s INTO %s (%s) VALUES (%s)"
        replace_str = replace and 'OR REPLACE' or ''
        cmd = insert_str % (replace_str, tablename,
                            ','.join(cols),
                            ','.join(colrepls))

        # Finally run the insert command
        self.execute(cmd, vals, commit=commit)

def test_db(db):
    # Delete table if it exists already
    try:
        db.execute('drop table aiprops')
    except db.Error, msg:
        print 'Could not drop table aiprops:', msg

    # Create table
    print 'CREATE TABLE'
    db.execute(open('aiprops_def.sql').read().strip())

    # Put in some data
    print 'INSERT DATA'
    for id_ in range(3):
        data = dict(id=id_, tstart=2.+id_, tstop=3.+id_, obsid=4+id_, pcad_mode='npnt',
                    aspect_mode='kalm', sim_mode='stop')
        db.insert(data, 'aiprops')

    print 'FETCHALL'
    rows = db.fetchall('select * from aiprops')
    print rows

    print 'INSERT from row read from db'
    row = rows[0]
    row['id'] += 10
    print row
    db.insert(row, 'aiprops')

    print 'FETCHONE'
    print db.fetchone('select * from aiprops')

    print 'FETCH'
    for row in db.fetch('select * from aiprops'):
        print row

    print 'FETCH (result set = null)'
    for row in db.fetch('select * from aiprops where id=100000'):
        print row

    print 'FETCHONE (result set = null)'
    print db.fetchone('select * from aiprops where id=100000')

    print 'FETCHALL (result set = null)'
    rows = db.fetchall('select * from aiprops where id=100000')
    print rows

    print 'DROP TABLE'
    db.execute('drop table aiprops')

def test_sqlite():
    dbfile = 'test.sql3'
    if os.path.exists(dbfile):
        os.unlink(dbfile)

    print "\n******* DbiSimple(dbi='sqlite', server=dbfile, numpy=True) ********\n"
    db = DbiSimple(dbi='sqlite', server=dbfile, numpy=True)
    test_db(db)
    print "\n******* DbiSimple(dbi='sqlite', server=dbfile, numpy=False) ********\n"
    db = DbiSimple(dbi='sqlite', server=dbfile, numpy=False, verbose=True)
    test_db(db)

def test_sybase():
    print "\n******* DbiSimple(dbi='sybase', server='sybase', user='aca_ops', database='aca', numpy=True, verbose=True) ********\n"
    db = DbiSimple(dbi='sybase', server='sybase', user='aca_ops', database='aca', numpy=True, verbose=True)
    test_db(db)
    print "\n******* DbiSimple(dbi='sybase', server='sybase', user='aca_ops', database='aca', numpy=False, verbose=True) ********\n"
    db = DbiSimple(dbi='sybase', server='sybase', user='aca_ops', database='aca', numpy=False, verbose=True)
    test_db(db)

if __name__ == '__main__':
    test_sqlite()
    test_sybase()
    
