# Licensed under a 3-clause BSD style license - see LICENSE.rst
from setuptools import setup

from Ska.DBI import __version__

try:
    from testr.setup_helper import cmdclass
except ImportError:
    cmdclass = {}

setup(name='Ska.DBI',
      author='Tom Aldcroft',
      description='Database interface utilities',
      author_email='taldcroft@cfa.harvard.edu',
      version=__version__,
      zip_safe=False,
      packages=['Ska', 'Ska.DBI', 'Ska.DBI.tests'],
      package_data={'Ska.DBI.tests': ['ska_dbi_test_table.sql']},
      tests_require=['pytest'],
      cmdclass=cmdclass,
      )
