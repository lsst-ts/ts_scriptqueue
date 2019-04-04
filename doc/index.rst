.. py:currentmodule:: lsst.ts.scriptqueue

.. _lsst.ts.scriptqueue:

###################
lsst.ts.scriptqueue
###################

Suppport for running SAL scripts on a script queue.

.. _lsst.ts.scriptqueue-using:

Using lsst.ts.scriptqueue
=========================

:ref:`lsst.ts.scriptqueue_sal_scripts` are executables that perform coordinated telescope and instrument control operations, such as "slew to a target and take an image", or "take a series of flats". SAL scripts are intended to be run once and then quit. A typical observing sequence is to run a series of "slew to a target and take an image" scripts, each configured for a different target.

The `ScriptQueue` CSC manages SAL scripts, running one script at a time until the queue is exhausted or paused.
A script may be added to the queue in any position (e.g. at the end, at the begininning or just after an existing script) and once queued, a script may be moved within the queue or removed from the queue.
Once a script has run successfully it can be rerun by adding it back to the queue.
If a SAL script fails, the script queue will pause, giving the operator a chance to fix any problems before continuing.

LSST will run two separate script queues: one for the main telescope and one for the auxiliary telescope.
The scheduler and the telescope operator will both queue scripts.

Scripts on the queue are identified by "SAL index" that is assigned to the script when it is added to the queue and remains attached to that script. The main telescope and auxiliary telescope use separate ranges of SAL indices, and the ranges are large enough that a no two scripts will have the same index for a particular night's observing.

For information about writing SAL scripts, see :ref:`lsst.ts.scriptqueue_sal_scripts` and :ref:`lsst.ts.scriptqueue_python_sal_scripts`.
For more information about the script queue, see `ScriptQueue`, especially :ref:`Basic Usage<script_queue_basic_usage>`.

.. toctree::
    sal_scripts
    :maxdepth: 1

Classes
=======

The primary classes in ts_scriptqueue are:

* `BaseScript` base class for Python SAL scripts.
* `ScriptQueue`: a `Commandable SAL Component`_ (CSC) to queue and run SAL scripts.
* `QueueModel`: a class that does most of the work for `ScriptQueue`.

Python API reference
====================

.. automodapi:: lsst.ts.scriptqueue
    :no-main-docstr:
    :no-inheritance-diagram:
.. automodapi:: lsst.ts.scriptqueue.test_utils
    :no-main-docstr:
    :no-inheritance-diagram:

.. _Commandable SAL Component: https://docushare.lsst.org/docushare/dsweb/View/LSE-209
