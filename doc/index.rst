.. py:currentmodule:: lsst.ts.scriptqueue

.. _lsst.ts.scriptqueue:

###################
lsst.ts.scriptqueue
###################

.. image:: https://img.shields.io/badge/SAL\ Interface-gray.svg
    :target: https://ts-xml.lsst.io/sal_interfaces/ScriptQueue.html
.. image:: https://img.shields.io/badge/GitHub-gray.svg
    :target: https://github.com/lsst-ts/ts_scriptqueue
.. image:: https://img.shields.io/badge/Jira-gray.svg
    :target: https://jira.lsstcorp.org/issues/?jql=labels+%3D+ts_scriptqueue

The `ScriptQueue` CSC manages :ref:`SAL scripts<lsst.ts.salobj_sal_scripts>`, running one script at a time until the queue is exhausted or paused.

.. _lsst.ts.scriptqueue-using:

User Guide
==========

Basics
------

LSST will run two separate `ScriptQueue` CSCs: one for the main telescope and one for the auxiliary telescope.
SAL scripts can be queued by the automatic scheduler and by telescope operators.

Each script queue contains three areas for scripts, all reported in the ``queue`` event:

* The queue: for scripts to be run. Reported as ``queue.salIndices``.
* The current slot: for the currently running script, or 0 if no script is running. Reported as ``queue.currentSalIndex``.
* The history: a record of scripts that have been run. Reported as ``queue.pastSalIndices``.

A script may be added to the queue in any position (at the end, the begininning, or just before or after an existing script) and once queued, a script may be moved within the queue or removed from the queue.

SAL scripts on a queue are identified by SAL index, the value that differentiates multiple instances of the same SAL component (in this case the ``Script`` SAL component).
The SAL index for a given script is assigned by the script queue and passed as a command line argument when launching the script; the script then uses that SAL index until it is finished.
The main telescope and auxiliary telescope use separate ranges of SAL indices, and the ranges are large enough that no two scripts will have the same index for a particular night's observing; in fact it is likely to take several days or weeks before a given SAL index is used again.

If a SAL script fails, the script queue will pause, giving the operator a chance to fix any problems before continuing.

Starting and Stopping the Script Queue
--------------------------------------

Start the script queue CSC as follows:

.. prompt:: bash

    run_script_queue.py index

where index is 1 for the main telescope and 2 for the auxiliary telescope.

Stop the CSC by sending it to the OFFLINE state.

Commanding the Script Queue
---------------------------

* Use the ``add`` command to add a SAL script to the script queue.
  The script queue assigns a unique SAL index to the script and launches the script as a subprocess.
  The script queue reports the script's SAL index and also returns it as the ``result`` field of the final acknowledgement of the ``add`` command.
* Once the script reports its state as ``unconfigured``, the script queue configures it, using the configuration specified in the ``add`` command.
* When a script reaches the top of the queue and has been configured, the script queue sets its group ID and reports the information using the ``nextVisit`` event.
  If the top script is then moved further back in the queue or removed from the queue, the script queue clears its group ID and reports this in the ``nextVisitCanceled`` event.
* If the current script finishes running successfully, the script queue moves the script to the history and:

  * If the script queue is running, it waits for the first script in the queue to be configured and have its group ID set,
    then it moves the script from the queue to the current slot and runs it.
  * If the script queue is paused, then the current slot is left empty and no new script is run.
* If, instead, the current script fails, the script queue halts, leaving the failed script in the current slot, so you can see what went wrong.
  Once you have dealt with the problem and are ready to continue, send the ``resume`` command;
  the script queue will then move the failed script to the history and continue.
* Once a script has finished, for any reason, its information is moved to the history.
  The history allows you to see the final state of each script and to ``requeue`` scripts that have already run.
* Other commands include:

  * ``stop``: stop the currently running script and/or remove one or more scripts that have not yet run.
  * ``move``: move an existing script to a different position in the queue.
  * ``pause``: pause the script queue. The current script will keep running but no new scripts will be run.
  * ``resume``: resume the script queue.
  * ``requeue``: add a copy of an existing script (whether it has run or not) to the queue, using the same configuration as the original script.

Developer Guide
===============

.. toctree::
    developer-guide
    :maxdepth: 1

Version History
===============

.. toctree::
    version_history
    :maxdepth: 1
