# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""Provides a test runner class."""
import logging
import platform
import sys
from datetime import datetime
from typing import Any, Dict, List

import psutil

from okpt.io.config.parsers import test
from okpt.test.test import Test, get_avg


def _aggregate_runs(runs: List[Dict[str, Any]]):
    """Aggregates and averages a list of test results.

    Args:
        results: A list of test results.
        num_runs: Number of times the tests were ran.

    Returns:
        A dictionary containing the averages of the test results.
    """
    aggregate: Dict[str, Any] = {}
    for run in runs:
        for key, value in run.items():
            if key in aggregate:
                aggregate[key].append(value)
            else:
                aggregate[key] = [value]

    aggregate = {key: get_avg(value) for key, value in aggregate.items()}
    return aggregate


class TestRunner:
    """Test runner class for running tests and aggregating the results.

    Methods:
        execute: Run the tests and aggregate the results.
    """

    def __init__(self, test_config: test.TestConfig):
        """"Initializes test state."""
        self.test_config = test_config
        self.test = Test(test_config)

    def _get_metadata(self):
        """"Retrieves the test metadata."""
        svmem = psutil.virtual_memory()
        return {
            'test_name':
                self.test_config.test_name,
            'test_id':
                self.test_config.test_id,
            'date':
                datetime.now().strftime('%m/%d/%Y %H:%M:%S'),
            'python_version':
                sys.version,
            'os_version':
                platform.platform(),
            'processor':
                platform.processor() + ', ' +
                str(psutil.cpu_count(logical=True)) + ' cores',
            'memory':
                str(svmem.used) + ' (used) / ' + str(svmem.available) +
                ' (available) / ' + str(svmem.total) + ' (total)',
        }

    def execute(self) -> Dict[str, Any]:
        """Runs the tests and aggregates the results.

        Returns:
            A dictionary containing the aggregate of test results.
        """
        logging.info('Setting up tests.')
        self.test.setup()
        logging.info('Beginning to run tests.')
        runs = []
        for i in range(self.test_config.num_runs):
            logging.info(
                f'Running test {i + 1} of {self.test_config.num_runs}'
            )
            runs.append(self.test.execute())

        logging.info('Finished running tests.')
        aggregate = _aggregate_runs(runs)

        # add metadata to test results
        test_result = {
            'metadata':
                self._get_metadata(),
            'results':
                aggregate
        }

        # include info about all test runs if specified in config
        if self.test_config.show_runs:
            test_result['runs'] = runs

        return test_result
