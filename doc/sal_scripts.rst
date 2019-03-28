.. py:currentmodule:: lsst.ts.scriptqueue

.. _lsst.ts.scriptqueue_sal_scripts:

SAL Scripts
###########

SAL scripts are programs that perform coordinated telescope and instrument control operations, such as "slew to a target and take an image", or "take a series of flats". SAL scripts are similar to CSCs in that they communicate via SAL messages, but a SAL script is run once and then it quits, whereas CSCs run for months at a time. SAL scripts are typically written in :ref:`Python<lsst.ts.scriptqueue_python_sal_scripts>`.

Technically a SAL script is any SAL component that supports the ``Script`` API and follows these rules:

* The script is a command-line executable that takes a single command line argument: the index of the SAL component.
* The script must start in the `ScriptState.UNCONFIGURED` state and output the ``description`` event.
* When the script is in the `ScriptState.UNCONFIGURED` state it can be configured with the ``configure`` command:

    * If the ``configure`` command succeeds, the script must report the ``metadata`` event and state `ScriptState.CONFIGURED`.
    * If the ``configure`` command fails the script must report state `ScriptState.FAILED` and exit.
    * The script must be configured before it can be run.

* Once the script is in the `ScriptState.CONFIGURED` state it can be run with the ``run`` command.
* When the ``run`` command finishes the script must exit, after reporting one of three states:

  * `ScriptState.DONE` on success
  * `ScriptState.STOPPED` if stopped by request
  * `ScriptState.FAILED` if an error occurred

`BaseScript` provides a base class for Python scripts which implements the above rules.

Script Packages
###############

All SAL scripts should go into the ``scripts`` directory of one of the following packages, so the `ScriptQueue` can find them:

* ``ts_standardscripts``: scripts that are approved for regular use.
  These must have :ref:`unit tests<lsst.ts.scriptqueue_python_unit_test>` and will be subject to strict version control.
* ``ts_externalscripts``: scripts for experimentation and one-off tasks.

.. _lsst.ts.scriptqueue_python_sal_scripts:

Python SAL Scripts
##################

Each SAL Script written in Python should consist of three parts:

* The script file itself, as an executable file in the ``scripts/...`` subdirectory of the ``ts_standardscripts`` or ``ts_externalscripts`` package.
* An implementation in the ``python/lsst/ts/...`` subdirectory of the same package.
  (The implementation can be in the script file, but that makes it much more difficult to test the code or run it from a Jupyter notebook.)
* A unit test in the ``tests/...`` directory, whose name is ``test_`` followed by the name of the script file.

A good example is the ``slew_telescope_icrs.py`` script, which includes the following files in the ``ts_standardscripts`` package:

* The script file: ``scripts/auxtel/slew_telescope_icrs.py``
* The implementation: ``python/lsst/ts/standardscripts/auxtel/slew_telescope_icrs.py``
* The unit test: ``tests/auxtel/test_slew_telescope_icrs.py``

Python Script File
==================

The script file should just import the script and call ``main``::

    #!/usr/bin/env python
    #...standard LSST boilerplate
    from lsst.ts.standardscripts.auxtel import SlewTelescopeIcrs
    SlewTelescopeIcrs.main()

Make your script executable using ``chmod +x <path>``.

Python Script Implementation
============================

See ``python/lsst/ts/standardscripts/auxtel/test_slew_telescope_icrs.py`` in the ``ts_standardscripts`` package for an example.

The script implementation does the actual work.
Your script should be a subclass of `BaseScript` with the following methods:

configure method (optional)
---------------------------

If your script can be configured (and most can), define the asynchronous configure method::

    async def configure(self, param1, param2=default, ...):
        # ...

This method should verify that all parameters have usable type and reasonable values.
This method can then set attributes or perform actions accordingly, but it should not talk to other SAL components.

Note that ``configure`` will always be called once before ``run`` and never again.
Thus if ``configure`` sets attributes needed by ``run``, there is no point to initializing those attributes in the constructor.

run method (required)
---------------------

Define asynchronous function ``run`` to do the main work of the script::

    async def run(self):
        ...

If ``run`` needs to run a slow computation, either call ``await asyncio.sleep(0)`` occasionally to give other coroutines a chance to run (0 is sufficient to free the event loop), or run the computation in a thread using `run_in_executor`_ e.g.::

    def slow_computation(self):
        ...

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, slow_computation)

or if you wish to do other things while you wait::

    loop = asyncio.get_running_loop()
    thread_task = asyncio.ensure_future(loop.run_in_executor(None, slow_computation))

    # do other work here...
    # then eventually you must wait for the background task
    result = await thread_task

.. _run_in_executor: https://docs.python.org/3/library/asyncio-eventloop.html#id14

checkpoints
^^^^^^^^^^^

In your run method you may call ``await self.checkpoint(name_of_checkpoint)`` to specify a point at which users can pause or stop the script.
By providing a diferent name for each checkpoint you allow users to specify exactly where they would like the script to pause or stop.
In addition, each checkpoint is reported as the ``lastCheckpoint`` attribute of the ``state`` event, so providing informative names can be helpful in tracking the progress of a script.
We suggest you make checkpoint names fairly short, obvious and unique, but none of these rules is enforced.
If you have a checkpoint in a loop you may wish to modify the name for each iteration, e.g.::

    for iter in range(num_exposures):
        await self.checkpoint(f"start exposure {iter}")
        ...

This allows the user to pause or stop at any particular iteration, and makes the ``state`` event more informative.

cleanup method (optional)
-------------------------

When your script is ending, after ``run`` finishes, is stopped early, or raises an exception, ``BaseScript`` calls asynchronous method ``cleanup`` for final cleanup.
In some sense ``cleanup`` is like the ``finally`` clause of a ``try/finally`` block.
The default implementation does nothing, but you are free to override it.::

    async def cleanup(self):
        ....

If your cleanup code cares about why the script is ending, examine ``self.state.state``; it will be one of:

* `ScriptState.ENDING`: the ``run`` method ran normally.
* `ScriptState.STOPPING`: the script was commanded to stop.
* `ScriptState.FAILING`: the ``run`` method raised an exception.

If your cleanup code needs additional knowledge about the script's state, you can add one or more instance variables to your script class and set them in the ``run`` method.

other methods
-------------

You may define other methods as well, but be careful not to shadow `BaseScript` methods.

.. _lsst.ts.scriptqueue_python_unit_test:

Python Unit Test
================

See ``tests/auxtel/test_slew_telescope_icrs.py`` in the ``ts_standardscripts`` package for an example.

There are two basic parts to testing a script: testing configuration and testing the run method.

Testing configuration is straightforward:

* Write a test method that calls ``configure`` with different sorts of invalid data and make sure that ``configure`` raises a suitable exception.
* Write one or more test methods that calls ``configure`` with valid data and test that your script is now properly configured.

Testing the run method is more work. My suggestion:

* Make a trivial class for each controller that your script commands.
  The class should execute a callback for each commands your script sends.
  Each callback should record any command data you want to check later, and output any events and telemetry that your script relies on.
* Configure the script by sending it the ``do_configure`` command.
  This is important because it puts the script into the `ScriptState.CONFIGURED` state.
* Run the script by sending it the ``do_run`` command.
* Check that the final state is `ScriptState.DONE`.
* Check recorded data to see that it matches your expectations.
