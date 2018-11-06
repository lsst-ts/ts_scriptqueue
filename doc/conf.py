"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import ts.scriptqueue.version


_g = globals()
_g.update(build_package_configs(
    project_name='ts_scriptqueue',
    version=ts.scriptqueue.version.__version__))
