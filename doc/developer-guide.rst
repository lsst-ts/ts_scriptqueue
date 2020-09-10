.. py:currentmodule:: lsst.ts.ScriptQueue

.. _lsst.ts.ScriptQueue.developer_guide:

###############
Developer Guide
###############

The ScriptQueue CSC is implemented using `ts_salobj <https://github.com/lsst-ts/ts_salobj>`_.

.. _lsst.ts.ScriptQueue.api:

API
===

The primary classes in ts_scriptqueue are:

* `ScriptQueue`: a Commandable SAL Component (CSC) to queue and run SAL scripts.
* `QueueModel`: a class that does most of the work for `ScriptQueue`.

.. automodapi:: lsst.ts.scriptqueue
    :no-main-docstr:

.. _lsst.ts.ScriptQueue.build:

Build and Test
==============

This is a pure python package. There is nothing to build except the documentation.

.. code-block:: bash

    make_idl_files.py Script ScriptQueue
    setup -r .
    pytest -v  # to run tests
    package-docs clean; package-docs build  # to build the documentation

.. _lsst.ts.ScriptQueue.contributing:

Contributing
============

``lsst.ts.ScriptQueue`` is developed at https://github.com/lsst-ts/ts_scriptqueue.
Bug reports and feature requests use `Jira with labels=ts_scriptqueue <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20labels%20%20%3D%20ts_scriptqueue>`_.
