# Licensed under a 3-clause BSD style license - see LICENSE.rst
from setuptools import setup
from ska_helpers.setup_helper import duplicate_package_info
from testr.setup_helper import cmdclass

name = "ska_dbi"
namespace = "Ska.DBI"

packages = ["ska_dbi", "ska_dbi.tests"]
package_dir = {name: name}
package_data = {"ska_dbi.tests": ["ska_dbi_test_table.sql"]}

duplicate_package_info(packages, name, namespace)
duplicate_package_info(package_dir, name, namespace)
duplicate_package_info(package_data, name, namespace)

setup(
    name=name,
    author="Tom Aldcroft",
    description="Database interface utilities",
    author_email="taldcroft@cfa.harvard.edu",
    use_scm_version=True,
    setup_requires=["setuptools_scm", "setuptools_scm_git_archive"],
    zip_safe=False,
    package_dir=package_dir,
    packages=packages,
    package_data=package_data,
    tests_require=["pytest"],
    cmdclass=cmdclass,
)
