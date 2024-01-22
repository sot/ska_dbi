import subprocess
from pathlib import Path
from astropy.table import Table

SYBASE = "/soft/SYBASE16.0"
ENV_VARS = [
    f"SYBASE={SYBASE}",
    "LD_LIBRARY_PATH=/soft/SYBASE16.0/OCS-16_0/lib:/soft/SYBASE16.0/OCS-16_0/lib3p64:/soft/SYBASE16.0/OCS-16_0/lib3p",
]
SQSH_BIN = "/usr/local/bin/sqsh.bin"


class Sqsh(object):
    def __init__(
        self,
        server=None,
        user=None,
        passwd=None,
        database=None,
        sqshrc=None,
        authdir="/proj/sot/ska/data/aspect_authorization",
        **kwargs,
    ):
        DEFAULTS = {"server": "sqlsao", "user": "aca_ops", "database": "axafapstat"}

        self.server = server or DEFAULTS.get("server")
        self.user = user or DEFAULTS.get("user")
        self.database = database or DEFAULTS.get("database")
        self.passwd = passwd
        self.sqshrc = sqshrc

        if not Path(SYBASE).exists():
            raise RuntimeError("SYBASE does not exist: %s" % SYBASE)
        if not Path(SQSH_BIN).exists():
            raise RuntimeError("sqsh.bin does not exist: %s" % SQSH_BIN)

        if self.passwd is None and self.sqshrc is None:
            sqshrc_file = (
                Path(authdir) / f"sqsh-{self.server}-{self.database}-{self.user}"
            )
            if sqshrc_file.exists():
                self.sqshrc = sqshrc_file
            else:
                raise NoPasswordError(
                    "None supplied and unable to read inferred sqshrc file %s" % e
                )

    def __enter__(self):
        """Context manager enter runtime context.  No action required, just return self."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit run time context.

        Close connection.  By the implicit "return None" this will raise any exceptions
        after closing.
        """
        pass

    def fetchall(self, query):
        envs = ENV_VARS
        if self.sqshrc is not None:
            envs = envs + [f"SQSHRC={self.sqshrc}"]
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
        if self.passwd is not None:
            cmd += ["-P", self.passwd]
        cmd = ["env"] + envs + cmd

        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                "sqsh failed with return code %d: %s" % (proc.returncode, stderr)
            )

        tab = Table.read(stdout.decode().splitlines(), format="ascii.csv")
        return tab
