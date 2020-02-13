.. py:currentmodule:: lsst.ts.scriptqueue

.. _lsst.ts.scriptqueue.revision_history:

################
Revision History
################

v2.5.0
======

Major changes:

* Output the ``nextVisit`` and ``nextVisitCanceled`` events.
* Code formatted by ``black``, with a pre-commit hook to enforce this. See the README file for configuration instructions.

Requirements:

* ts_salobj 5.4
* ts_idl 1
* ts_xml 4.7
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
* asynctest

v2.4.0
======

Update for ts_salobj v5.
Allow specifying log level and checkpoints when adding a script.
Modernize asyncio usage for python 3.7.

Requirements:

* ts_salobj v5
* ts_idl v0.4
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
* asynctest

v2.3.0
======
Update to run unit tests with asynctest

Requirements:

* ts_salobj v4.3
* ts_idl
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
* asynctest

v2.2.2
======

Fix the showSchema command.

Requirements:

* ts_salobj v4.3
* ts_idl
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``


v2.2.1
======

Improve timeouts in tests for robustness. This was necessitated by DM-20259 changes to ts_salobj.

Requirements:

* ts_salobj v4.3
* ts_idl
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``

v2.2.0
======

Move BaseScript and TestScript to ts_salobj to break a circular dependency.

Requirements:

* ts_salobj v4.3
* ts_idl
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``

v2.1.0
======

Add run_one_script.py bin script to easily run a single script,
e.g. for development.

Also modify the script queue to get the default locations
for standard and external scripts using ``get_scripts_dir``
functions in ``ts_standardscripts`` and ``ts_externalscripts``.

Requirements:

* ts_salobj v4.3
* ts_idl
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``

v2.0.0
======

Use OpenSplice dds instead of SALPY libraries and use a schema to validate configuration and specify default values.

See https://community.lsst.org/t/changes-to-sal-script-schemas-and-dds/3709 for more information about what has changed.

Requirements:

* ts_salobj v4.3
* ts_idl
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
