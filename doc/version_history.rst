.. py:currentmodule:: lsst.ts.scriptqueue

.. _lsst.ts.scriptqueue.version_history:

###############
Version History
###############

v2.6.4
======

Changes:

* Make the `move`, `requeue` and `showScript` commands fail without logging an exception if a specified script does not exist.

Requirements:

* ts_salobj 5.17
* ts_idl 1
* ts_xml 4.7
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
* asynctest

v2.6.3
======

Changes:

* Enhance the ScriptQueue commander to add options for the "add" command
  and to accept a default log level for scripts as a command-line argument.

Requirements:

* ts_salobj 5.17
* ts_idl 1
* ts_xml 4.7
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
* asynctest

v2.6.2
======

Changes:

* Fix the stopScripts command in `ScriptQueueCommander`.
* Update the pre-commit hook to block the commit if any code is not formatted with black.
* Update SConstruct so it does not need configuration and remove cfg file from ups.

Requirements:

* ts_salobj 5.17
* ts_idl 1
* ts_xml 4.7
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
* asynctest

v2.6.1
======

Salobj 6 changed the name of the ``SalInfo.makeAckCmd`` method to ``SalInfo.make_ackcmd``.
Add a check to make sure ``SalInfo`` has a ``make_ackcmd`` attribute and use ``makeAckCmd`` if not.

Changes:

* Add backward compatibility between salobj 5 and 6.
* Add Jenkinsfile for CI job.
* In test_utils.py separate testing ``get_scripts_dir`` from standard and external scripts.
  Since packages are optional, skip tests if packages cannot be imported.

v2.6.0
======

Changes:

* Replaced ``bin/request_script.py`` with ``bin/command_script_queue.py``, which is based on `lsst.ts.salobj.CscCommander`.
  This change requires ts_sal v5.17.0 or later.

Requirements:

* ts_salobj 5.17
* ts_idl 1
* ts_xml 4.7
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
* asynctest

v2.5.2
======

Changes:

* Fixed warnings in ``tests/test_queue_model.py`` caused by not allowing all queued scripts to finish.

Requirements:

* ts_salobj 5.11
* ts_idl 1
* ts_xml 4.7
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
* asynctest

v2.5.1
======

Changes:

* Add ``tests/test_black.py`` to verify that files are formatted with black.
  This requires ts_salobj 5.11 or later.
* Make `ui.RequestModel` compatible with ts_salobj 5.12.
* Make time limits in unit tests simpler and more generous.
  This makes the tests simpler and should help tests pass on machines with limited resources.
* Fix flake8 warnings about f strings with no {}.
* Update ``.travis.yml`` to remove ``sudo: false`` to github travis checks pass once again.

Requirements:

* ts_salobj 5.11
* ts_idl 1
* ts_xml 4.7
* IDL files for Script and ScriptQueue, e.g. built with ``make_idl_files.py``
* asynctest

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
