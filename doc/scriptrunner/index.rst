.. py:currentmodule:: scriptrunner

.. scriptrunner:

############
scriptrunner
############

Suppport for loading and running scripts.

The primary classes are:

* `scriptrunner.ScriptLoader`: a `Commandable SAL Component`_ (CSC) to load scripts. Each loaded script is a SAL component that can be run.
* `scriptrunner.BaseScript` base class for Python scripts.

Scripts
=======

A script is a command-line executable file that takes a single command line argument: the index of the SAL component. When a script is run from the command line it must start a Script SAL Component with the specified index. In addition:

* A script must start up in a mode where it is waiting for the ``configure`` command.
* A ``configure`` command must be received before the script can be run with the ``run`` command.
* When a ``configure`` command is received the script must output an ``estimated_duration`` event.

Python API reference
====================

.. automodapi:: scriptrunner
    :no-main-docstr:
    :no-inheritance-diagram:

.. _Commandable SAL Component: https://docushare.lsst.org/docushare/dsweb/View/LSE-209
