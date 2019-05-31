__all__ = ["TestScript"]

import asyncio

import yaml

from lsst.ts import salobj
from .base_script import BaseScript


class TestScript(BaseScript):
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

    @property
    def schema(self):
        schema_yaml = """
            $schema: http://json-schema.org/draft-07/schema#
            $id: https://github.com/lsst-ts/ts_scriptqueue/TestScript.yaml
            title: TestScript v1
            description: Configuration for TestScript
            type: object
            properties:
              wait_time:
                description: Time to wait, in seconds
                type: number
                minimum: 0
                default: 0
              fail_run:
                description: If true then raise an exception in
                    the "run" method afer the "start" checkpoint
                    but before waiting.
                type: boolean
                default: false
              fail_cleanup:
                description: If true then raise an exception in
                    the "cleanup" method.
                type: boolean
                default: false
            required: [wait_time, fail_run, fail_cleanup]
            additionalProperties: false
        """
        return yaml.safe_load(schema_yaml)

    async def configure(self, config):
        """Configure the script.

        Parameters
        ----------
        config : `types.SimpleNamespace`
            Configuration, as described by `schema`.

        Raises
        ------
        `salobj.ExpectedError`
            If ``wait_time < 0``. This can be used to make config fail.
        """
        self.log.info("Configure started")
        # wait a short time so unit tests can reliably start a queue
        # before the first script has been configured
        await asyncio.sleep(0.1)
        self.config = config

        self.log.info(f"wait_time={self.config.wait_time}, "
                      f"fail_run={self.config.fail_run}, "
                      f"fail_cleanup={self.config.fail_cleanup}, "
                      )
        self.log.info("Configure succeeded")

    def set_metadata(self, metadata):
        metadata.duration = self.config.wait_time

    async def run(self):
        self.log.info("Run started")
        await self.checkpoint("start")
        if self.config.fail_run:
            raise salobj.ExpectedError(f"Failed in run after wait: fail_run={self.config.fail_run}")
        await asyncio.sleep(self.config.wait_time)
        await self.checkpoint("end")
        self.log.info("Run succeeded")

    async def cleanup(self):
        self.log.info("Cleanup started")
        if self.config.fail_cleanup:
            raise salobj.ExpectedError(f"Failed in cleanup: fail_cleanup={self.config.fail_cleanup}")
        self.log.info("Cleanup succeeded")
