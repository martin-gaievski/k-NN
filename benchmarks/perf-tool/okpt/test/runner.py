# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""Provides a test runner class."""
import logging
import platform
import sys
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List
from okpt.io import args
from okpt.io.utils import writer
from io import TextIOWrapper

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

    regex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')

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

    def parse_time(self, time_str):
        parts = self.regex.match(time_str)
        if not parts:
            return
        parts = parts.groupdict()
        time_params = {}
        for name, param in parts.items():
            if param:
                time_params[name] = int(param)
        return timedelta(**time_params)

    def execute(self) -> Dict[str, Any]:
        """Runs the tests and aggregates the results.

        Returns:
            A dictionary containing the aggregate of test results.
        """
        logging.info('Setting up tests.')
        self.test.setup()
        logging.info('Beginning to run tests.')
        runs = []
        logging.info(f'Duration setting {self.test_config.duration}')
        if not self.test_config.duration:
            for i in range(self.test_config.num_runs):
                logging.info(
                    f'Running test {i + 1} of {self.test_config.num_runs}'
                )
                runs.append(self.test.execute())
        else :
            cur_time = datetime.now()
            end_time = cur_time + self.parse_time(self.test_config.duration)
            i = 0

            cli_args = args.get_args()
            output_name = cli_args.output.name + '.snapshot'
            output_snapshot_file = TextIOWrapper(open(output_name, 'wb'), encoding='utf-8')

            while cur_time < end_time:
                i += 1
                logging.info(f'Running test {i}')
                test_run = self.test.execute()
                runs.append(test_run)
                runs[i - 1]['run'] = i

                cur_time = datetime.now()
                logging.info(f'Time left {end_time - cur_time}')

                writer.write_json(test_run, output_snapshot_file, pretty=True)
                output_snapshot_file.flush()

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
