# from distutils.core import setup, Extension
import os

from setuptools import setup
setup(name='Ska.DBI',
      author = 'Tom Aldcroft',
      description='Database interface utilities',
      author_email = 'taldcroft@cfa.harvard.edu',
      py_modules = ['Ska.DBI'],
      version='1.0',
      zip_safe=False,
      namespace_packages=['Ska'],
      packages=['Ska'],
      package_dir={'Ska' : 'Ska'},
      package_data={}
      )
