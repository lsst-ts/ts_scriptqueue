.. py:currentmodule:: scriptloader

.. scriptloader:

############
scriptloader
############

Suppport for loading SAL scripts: scripts written as Script SAL components.
Loading consists of starting the script in a subprocess and configuring it, so it is ready to run.
SAL scripts implement one-off tasks: after they are configured they are run once and then they quit.
Unlike Commandable SAL Components (CSCs), Scripts are not intended to be long-lived SAL components.

The primary classes are:

* `scriptloader.BaseScript` base class for Python scripts.
* `scriptloader.ScriptLoader`: a `Commandable SAL Component`_ (CSC) to load SAL scripts.
  Note: a script queue is likely to replace this CSC.
* `scriptloader.LoaderModel`: a class that loads and configures SAL scripts and keeps track of loaded scripts.

SAL Scripts
===========

A SAL script is a SAL component that supports the Script API. In addition:

* A script is a command-line executable that takes a single command line argument: the index of the SAL component.
* A script must start up in the ``UNCONFIGURED`` state and output the ``description`` event.
* If the ``configure`` command succeeds, the script must report the ``metadata`` event and go into the ``CONFIGURED`` state.
* The ``configure`` command is permitted in both the ``UNCONFIGURED`` and the ``CONFIGURED`` states.
* The ``run`` command runs the script. The script must be in the ``CONFIGURED`` state to be run.
* When the ``run`` command finishes the script must exit, after reporting one of three states:

  * ``DONE`` on success
  * ``STOPPED`` if stopped by request
  * ``FAILED`` if an error occurred

`scriptloader.BaseScript` provides a base class for Python scripts which implements the above rules.

Python API reference
====================

.. automodapi:: scriptloader
    :no-main-docstr:
    :no-inheritance-diagram:

.. _Commandable SAL Component: https://docushare.lsst.org/docushare/dsweb/View/LSE-209
