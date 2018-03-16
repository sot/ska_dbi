# Licensed under a 3-clause BSD style license - see LICENSE.rst
from .DBI import *

__version__ = '3.9.0'


def test(*args, **kwargs):
    '''
    Run py.test unit tests.
    '''
    import testr
    return testr.test(*args, **kwargs)
