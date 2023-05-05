'''
Test the get_ec2_instance_info.py script.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

from copy import deepcopy
from CSVLogParser import CSVLogParser
import filecmp
from JobAnalyzer import JobAnalyzer, JobCost, logger as JobAnalyzer_logger
import json
import logging
from MemoryUtils import MEM_GB, MEM_KB, MEM_MB
import os
from os import environ, getenv, listdir, makedirs, path, system
from os.path import abspath, dirname
import pytest
from SchedulerJobInfo import SchedulerJobInfo
import subprocess
from subprocess import CalledProcessError, check_output
from test_security import order as last_order
import unittest

order = last_order // 100 * 100 + 100
assert order == 1000

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

class TestEC2InstanceInfo(unittest.TestCase):
    global order

    def __init__(self, name):
        super().__init__(name)
        self._restore_instance_type_info()

    REPO_DIR = abspath(f"{dirname(__file__)}/..")

    OUTPUT_DIR = path.join(REPO_DIR, 'output')

    region = 'eu-west-1'

    def _remove_instance_type_info(self):
        system(f"rm -f {dirname(__file__)+'/../instance_type_info.json'}")

    def _use_static_instance_type_info(self):
        system(f"cp {self.REPO_DIR}/test_files/instance_type_info.json {self.REPO_DIR}/instance_type_info.json")

    def _restore_instance_type_info(self):
        system(f"git restore {dirname(__file__)+'/../instance_type_info.json'}")

    def cleanup_output_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    @pytest.mark.order(-2)
    def test_get_instance_type_info_region(self):
        try:
            self.cleanup_output_files()
            self._remove_instance_type_info()
            try:
                output = check_output(['./get_ec2_instance_info.py', '--disable-version-check', '--region', self.region, '--input', 'instance_type_info.json'], stderr=subprocess.STDOUT, encoding='utf8')
            except CalledProcessError as e:
                print(e.output)
                raise
            print(f"output:\n{output}")
            assert(path.exists(path.join(self.REPO_DIR, 'instance_type_info.json')))
        finally:
            self._remove_instance_type_info()
            self._restore_instance_type_info()

    @pytest.mark.order(-1)
    def test_get_instance_type_info(self):
        '''
        Generate instance_type_info.json to make sure it is up to date

        Generate for all AWS regions.
        '''
        try:
            self._remove_instance_type_info()
            try:
                check_output(['./get_ec2_instance_info.py', '--disable-version-check', '--input', 'instance_type_info.json'], stderr=subprocess.STDOUT, encoding='utf8')
            except CalledProcessError as e:
                print(f"returncode: {e.returncode}")
                print(f"output:\n{e.stdout}")
                raise
        finally:
            self._remove_instance_type_info()
            self._restore_instance_type_info()
