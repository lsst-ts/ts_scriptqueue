__all__ = ["TestScript"]

import asyncio
import logging

import salobj
import ts_scriptqueue


class TestScript(ts_scriptqueue.BaseScript):
    """Test script to allow testing BaseScript.

    Parameters
    ----------
    index : `int`
        Index of Script SAL component.

    Wait for the specified time, then exit. See `configure` for details.
    """
    __test__ = False  # stop pytest from warning that this is not a test

    def __init__(self, index, descr=""):
        super().__init__(index=index, descr=descr)
        self.wait_time = 0
        self.fail_run = False
        self.fail_cleanup = False
        self.log.setLevel(logging.INFO)

    def configure(self, wait_time=0, fail_run=False, fail_cleanup=False):
        """Configure the script.

        Parameters
        ----------
        wait_time : `float`
            Time to wait, in seconds
        fail_run : `bool`
            If True then raise an exception in `run` after the "start"
            checkpoint but before waiting.
        fail_cleanup : `bool`
            If True then raise an exception in `cleanup`.

        Raises
        ------
        salobj.ExpectedError
            If ``wait_time < 0``. This can be used to make config fail.
        """
        self.log.info("Configure started")
        self.wait_time = float(wait_time)
        if self.wait_time < 0:
            raise salobj.ExpectedError(f"wait_time={self.wait_time} must be >= 0")
        self.fail_run = bool(fail_run)
        self.fail_cleanup = bool(fail_cleanup)

        self.log.info(f"wait_time={self.wait_time}, "
                      f"fail_run={self.fail_run}, "
                      f"fail_cleanup={self.fail_cleanup}, "
                      )
        self.log.info("Configure succeeded")

    def set_metadata(self, metadata):
        metadata.duration = self.wait_time

    async def run(self):
        self.log.info("Run started")
        await self.checkpoint("start")
        if self.fail_run:
            raise salobj.ExpectedError(f"Failed in run after wait: fail_run={self.fail_run}")
        await asyncio.sleep(self.wait_time)
        await self.checkpoint("end")
        self.log.info("Run succeeded")

    async def cleanup(self):
        self.log.info("Cleanup started")
        if self.fail_cleanup:
            raise salobj.ExpectedError(f"Failed in cleanup: fail_cleanup={self.fail_cleanup}")
        self.log.info("Cleanup succeeded")
