'''
Test CSVLogParser class.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import filecmp
from os import path, system
from os.path import dirname
from CSVLogParser import CSVLogParser
from MemoryUtils import MEM_GB, MEM_KB
import psutil
import pytest
from SchedulerJobInfo import SchedulerJobInfo
import subprocess
from subprocess import CalledProcessError, check_output

@pytest.mark.order(3, after=['tests/test_AcceleratorLogParser.py::TestAcceleratorLogParser::test_main'])
class TestCSVLogParser:
    def cleanup_output_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    def parse_job(self):
        self.cleanup_output_files()
        test_files_dir = 'test_files/LSFLogParser'
        input_csv = path.join(test_files_dir, 'exp_jobs.csv')
        output_dir = 'output/CSVLogParser'
        output_csv = path.join(output_dir, 'jobs.csv')
        parser = CSVLogParser(input_csv, output_csv)
        job = True
        while job:
            job = parser.parse_job()
        assert(filecmp.cmp(input_csv, output_csv, shallow=False))

    def test_main_no_args(self):
        self.cleanup_output_files()
        test_files_dir = 'test_files/LSFLogParser'
        input_csv = path.join(test_files_dir, 'exp_jobs.csv')
        output_dir = 'output/CSVLogParser'
        output_csv = path.join(output_dir, 'jobs.csv')
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./CSVLogParser.py'], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("CSVLogParser.py: error: the following arguments are required: --input-csv" in excinfo.value.output)

    def test_main_no_input(self):
        self.cleanup_output_files()
        test_files_dir = 'test_files/LSFLogParser'
        input_csv = path.join(test_files_dir, 'exp_jobs.csv')
        output_dir = 'output/CSVLogParser'
        output_csv = path.join(output_dir, 'jobs.csv')
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./CSVLogParser.py', '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        assert("CSVLogParser.py: error: the following arguments are required: --input-csv" in excinfo.value.output)

    def test_main_no_output(self):
        self.cleanup_output_files()
        test_files_dir = 'test_files/LSFLogParser'
        input_csv = path.join(test_files_dir, 'exp_jobs.csv')
        output_dir = 'output/CSVLogParser'
        try:
            output = check_output(['./CSVLogParser.py', '--input-csv', input_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise

    def test_main_same_input_and_output(self):
        self.cleanup_output_files()
        input_csv = 'test_files/AcceleratorLogParser/exp_jobs.csv'
        output_dir = 'output/CSVLogParser'
        output_csv = path.join(output_dir, 'jobs.csv')
        try:
            output = check_output(['./CSVLogParser.py', '--input-csv', input_csv, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
            print(output)
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        assert(filecmp.cmp(input_csv, output_csv, shallow=False))

        input_csv = output_csv
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./CSVLogParser.py', '--input-csv', input_csv, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("Input and output CSV cannot be the same" in excinfo.value.output)

    def test_main_from_accelerator(self):
        self.cleanup_output_files()
        input_csv = 'test_files/AcceleratorLogParser/exp_jobs.csv'
        output_dir = 'output/CSVLogParser'
        output_csv = path.join(output_dir, 'jobs.csv')
        output = check_output(['./CSVLogParser.py', '--input-csv', input_csv, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(output)
        assert(filecmp.cmp(output_csv, input_csv, shallow=False))

    def test_main_from_lsf(self):
        self.cleanup_output_files()
        input_csv = 'test_files/LSFLogParser/exp_jobs.csv'
        output_dir = 'output/CSVLogParser'
        output_csv = path.join(output_dir, 'jobs.csv')
        check_output(['./CSVLogParser.py', '--input-csv', input_csv, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        assert(filecmp.cmp(input_csv, output_csv, shallow=False))

    def test_main_from_slurm(self):
        self.cleanup_output_files()
        input_csv = 'test_files/SlurmLogParser/exp_jobs.csv'
        output_dir = 'output/CSVLogParser'
        output_csv = path.join(output_dir, 'jobs.csv')
        try:
            check_output(['./CSVLogParser.py', '--input-csv', input_csv, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        assert(filecmp.cmp(input_csv, output_csv, shallow=False))
