from setuptools import setup

from Ska.DBI import __version__

setup(name='Ska.DBI',
      author = 'Tom Aldcroft',
      description='Database interface utilities',
      author_email = 'taldcroft@cfa.harvard.edu',
      test_suite="test.test_all",
      py_modules = ['Ska.DBI'],
      version=__version__,
      zip_safe=False,
      packages=['Ska'],
      package_dir={'Ska' : 'Ska'},
      package_data={}
      )
