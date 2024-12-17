# Licensed under a 3-clause BSD style license - see LICENSE.rst
import ska_helpers

from .DBI import *

__version__ = ska_helpers.get_version("ska_dbi")


def test(*args, **kwargs):
    """
    Run py.test unit tests.
    """
    import testr

    return testr.test(*args, **kwargs)
