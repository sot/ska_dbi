import subprocess
from pathlib import Path
from astropy.table import Table
from ska_dbi.common import DEFAULT_CONFIG, NoPasswordError

SYBASE = "/soft/SYBASE16.0"
LD_LIBRARY_PATH = "/soft/SYBASE16.0/OCS-16_0/lib:/soft/SYBASE16.0/OCS-16_0/lib3p64:/soft/SYBASE16.0/OCS-16_0/lib3p"
SQSH_BIN = "/usr/local/bin/sqsh.bin"


class Sqsh(object):
    """
    Class to interface with sqsh on HEAD to query the axafapstat or axafocat databases.

    Example usage::

      db = Sqsh(dbi='sybase', server='sybase', user='aca_ops', database='aca')
      db = Sqsh(dbi='sybase')   # Use defaults (same as above)

    :param server: Server name (default = sqlsao)
    :param user: User name (default = aca_ops)
    :param database: Database name (default = axafapstat)
    :param sqshrc: sqshrc file (optional).  Read from aspect authorization dir if required and not supplied.
    :param authdir: Directory containing authorization files

    :rtype: Sqsh object
    """
    def __init__(
        self,
        server=None,
        user=None,
        database=None,
        sqshrc=None,
        authdir="/proj/sot/ska/data/aspect_authorization",
        **kwargs,
    ):

        self.server = server or DEFAULT_CONFIG['sybase'].get("server")
        self.user = user or DEFAULT_CONFIG['sybase'].get("user")
        self.database = database or DEFAULT_CONFIG['sybase'].get("database")
        self.sqshrc = sqshrc

        if not Path(SYBASE).exists():
            raise RuntimeError(f"SYBASE does not exist: {SYBASE}")
        if not Path(SQSH_BIN).exists():
            raise RuntimeError(f"sqsh.bin does not exist: {SQSH_BIN}")

        if self.sqshrc is None:
            sqshrc_file = (
                Path(authdir) / f"sqsh-{self.server}-{self.database}-{self.user}"
            )
            if sqshrc_file.exists():
                self.sqshrc = sqshrc_file
            else:
                raise NoPasswordError(
                    f"Unable to read inferred sqshrc file {sqshrc_file}"
                )

    def __enter__(self):
        """Context manager enter runtime context.  No action required, just return self."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit run time context.
        """

    def fetch(self, query):
        """
        Execute a query and return all returned sql rows a list of lines.

        Note that this doesn't do anything fancy with the types of the return columns.
        Python Sybase, for example, handled Sybase datetime columns as Python datetimes types
        and here they are just strings.

        Parameters
        ----------
        query : str
            SQL query to execute (select statements are expected for our use cases)

        Returns
        -------
        list of str
        """
        cmd_env = {"SYBASE": SYBASE,
                   "LD_LIBRARY_PATH": LD_LIBRARY_PATH}
        if self.sqshrc is not None:
            cmd_env['SQSHRC'] = self.sqshrc

        cmd = [
            SQSH_BIN,
            "-X",
            "-S",
            self.server,
            "-U",
            self.user,
            "-D",
            self.database,
            "-m",
            "csv",
            "-C",
            query,
        ]

        stdout = subprocess.check_output(
            cmd, env=cmd_env,
        )
        outlines = stdout.decode().splitlines()
        return outlines


    def fetchall(self, query):
        """
        Fetches all the rows returned by the query.

        Parameters
        ----------
        query : str
            The SQL query to execute.

        Returns
        -------
        astropy.table.Table
            The table containing the fetched rows. If there are no rows that match the query,
            a zero-length table is returned.
        """
        outlines = self.fetch(query)
        tab = Table.read(outlines, format="ascii.csv")
        return tab


    def fetchone(self, query):
        """
        Fetches the first row returned by the query.

        Parameters
        ----------
        query : str
            The SQL query to execute.

        Returns
        -------
        astropy.table.Row or None
        """
        outlines = self.fetch(query)
        # Sqsh should always be returning a header line -- return None if that's it.
        if len(outlines) <= 1:
            return None
        tab = Table.read(outlines, format="ascii.csv")
        return tab[0]
