DEFAULT_CONFIG = {
    "sqlite": {"server": "db.sql3"},
    "sybase": {"server": "sqlsao", "user": "aca_ops", "database": "axafapstat"},
}


class NoPasswordError(Exception):
    """
    Error when password is neither supplied nor available from a file.
    """
