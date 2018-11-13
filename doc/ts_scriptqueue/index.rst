.. py:currentmodule:: lsst.ts.scriptqueue

.. scriptqueue:

###########
scriptqueue
###########

Suppport for running `SAL Scripts`_.

SAL Scripts
===========

SAL scripts implement one-off tasks: after they are configured they can be run once and then they quit.
Thus they are very different from Commandable SAL Components (CSCs), which typically run indefinitely.

A SAL script is a SAL component that supports the Script API and follows these rules:

* A script is a command-line executable that takes a single command line argument: the index of the SAL component.
* A script must start up in the ``UNCONFIGURED`` state and output the ``description`` event.
* If the ``configure`` command succeeds, the script must report the ``metadata`` event and go into the ``CONFIGURED`` state.
* The ``configure`` command is permitted only in ``UNCONFIGURED`` state.
* The ``run`` command runs the script. The script must be in the ``CONFIGURED`` state to be run.
* When the ``run`` command finishes the script must exit, after reporting one of three states:

  * ``DONE`` on success
  * ``STOPPED`` if stopped by request
  * ``FAILED`` if an error occurred

`BaseScript` provides a base class for Python scripts which implements the above rules.

Python SAL Scripts
==================

Create one SAL script per Python file. In this file create or import your script as a class that is a subclass of `BaseScript`, as explained below.
For an example see script file ``tests/data/standard/script1``, which imports and runs the test script `test_utils.TestScript`.

At the top of your script file put this::

    #!/usr/bin/env python

At the end of your script file put the following::

    if __name__ == "__main__":
        <YourScriptSubclass>.main(descr="<brief description>")

Then make your script executable using ``chmod +x <path>`` and put it in an appropriate location in the ``standard`` or ``external`` directory tree.

Your script code should be a subclass of `BaseScript` with the following methods:

run method (required)
---------------------

Define asynchronous function to implement the main functionality of your script::

    async def run(self):
        # ...

If ``run`` needs to wait for a slow computation, fork that using `run_in_executor`_ e.g.::

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, blocking_function)

or if you wish to do other things while you wait:

    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(loop.run_in_executor(None, blocking_function))

    # do other work here...
    # then eventually you must wait for htat task
    result = await task

.. _run_in_executor: https://docs.python.org/3/library/asyncio-eventloop.html#id14

checkpoints
^^^^^^^^^^^

In your run method you may call ``await self.checkpoint([name])`` to specify a point at which users can pause or stop the script.
By providing a name for each checkpoint you allow users to specify exactly where they would like the script to pause or stop.
In addition, each checkpoint is reported as the ``last_checkpoint`` attribute of the ``state`` event, so providing checkpoints with informative names can be helpful in tracking the progress of a script.
We suggest you make checkpoint names fairly short, obvious and unique, but none of these rules is enforced.
If you have a checkpoint in a loop you may wish to modify the name for each iteration, e.g.::

    for iter in range(self.num_iter):  # num_iter is probably a configuration parameter
        await self.checkpoint(f"start_loop{iter}")

This allows the user to stop at any particular iteration and makes the reported state more informative.

configure method (optional)
---------------------------

If your script can be configured, define the configure method::

    def configure(self, param1, param2=default, ...):
        # ...

This method should verify that all parameters have usable type and reasonable values.
This method can then set attributes or perform actions accordingly.
However, if at all possible please make it synchronous and reasonably quick.

Note that ``configure`` cannot be called once the script is running.
However, `BaseScript` enforces this automatically; you don't need to do anything to prevent it.

cleanup method (optional)
-------------------------

When your script is ending, after ``run`` finishes, is stopped early, or raises an exception, ``BaseScript`` calls ``cleanup`` for final cleanup.
In some sense ``configure`` is like the ``finally`` clause of a ``try/finally`` block.
The default implementation does nothing, but you are free to override it.::

    async def cleanup(self):
        # ....

If your cleanup code cares about why the script is ending it can look at the `BaseScript.state` property, which will be one of:

* `ScriptState.ENDING`: the script is ending because ``run`` exited normally.
* `ScriptState.STOPPING`: the script is ending because it was commanded to stop.
* `ScriptState.FAILING`: the ``run`` method raised an exception.

If your cleanup code needs additional knowledge about the script's state, you can add one or more instance variables to your script class and set them in the ``run`` method.

other methods
-------------

You may define other methods as well, but please be careful not to shadow `BaseScript` methods.

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
