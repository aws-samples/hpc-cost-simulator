'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import filecmp
from os import path, system
from os.path import dirname
import pytest
from MemoryUtils import mem_string_to_float, mem_string_to_int, MEM_KB, MEM_MB, MEM_GB, MEM_TB, MEM_PB
import subprocess
from subprocess import CalledProcessError, check_output

@pytest.mark.order(5, after=['tests/test_LSFLogParser.py::TestLSFLogParser::test_main'])
class TestSlurmLogParser:
    def cleanup_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    #@pytest.mark.order(after=['tests/test_LSFLogParser.py::TestLSFLogParser::test_main'])
    def test_convert_int(self):
        assert(mem_string_to_int('1000000') == 1000000)
        assert(mem_string_to_int('2K')/MEM_KB == 2)
        assert(mem_string_to_int('3M')/MEM_MB == 3)
        assert(mem_string_to_int('4G')/MEM_GB == 4)
        assert(mem_string_to_int('5T')/MEM_TB == 5)
        assert(mem_string_to_int('6P')/MEM_PB == 6)
        # Test lowercase
        assert(mem_string_to_int('2k')/MEM_KB == 2)
        assert(mem_string_to_int('3m')/MEM_MB == 3)
        assert(mem_string_to_int('4g')/MEM_GB == 4)
        assert(mem_string_to_int('5t')/MEM_TB == 5)
        assert(mem_string_to_int('6p')/MEM_PB == 6)
        # Test extra character
        assert(mem_string_to_int('2kc')/MEM_KB == 2)
        assert(mem_string_to_int('3mm')/MEM_MB == 3)
        assert(mem_string_to_int('4gm')/MEM_GB == 4)
        assert(mem_string_to_int('5tc')/MEM_TB == 5)
        assert(mem_string_to_int('6pc')/MEM_PB == 6)
        # Test float values
        assert(mem_string_to_int('2.13') == 2)
        assert(mem_string_to_int('2.50') == 2)
        assert(mem_string_to_int('2.51') == 3)
        assert(mem_string_to_int('2.475kc') == round(2.475 * MEM_KB))
        assert(mem_string_to_int('1e-3') == 0)
        assert(mem_string_to_int('1e3') == 1e3)
        # Test mem_string_to_float
        assert(mem_string_to_float('2.13') == 2.13)
        assert(mem_string_to_float('2.475k') == 2.475 * MEM_KB)
        assert(mem_string_to_float('1e-3M') == 1e-3 * MEM_MB)
        assert(mem_string_to_float('1e3') == 1000)
        # Test invalid values
        for value in ['abc', '1z', '1.2kcn']:
            with pytest.raises(ValueError) as excinfo:
                mem_string_to_float(value)
            print(excinfo.value)
            assert(str(excinfo.value) == f"could not convert string to float: '{value}'")

    #@pytest.mark.order(after=['test_convert_int]'])
    def test_main_no_args(self):
        self.cleanup_files()
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./SlurmLogParser.py'], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("SlurmLogParser.py: error: the following arguments are required: --output-csv" in excinfo.value.output)

    @pytest.mark.order(after=['test_missing_args]'])
    def test_main_no_slurm(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser'
        sacct_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser.jobs.csv'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./SlurmLogParser.py', '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("SlurmLogParser.py: error: one of the arguments --sacct-output-file --sacct-input-file is required" in excinfo.value.output)

    @pytest.mark.order(after=['test_main_no_slurm]'])
    def test_main_missing_sacct_input_file(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt-invalid')
        output_csv = 'output/SlurmLogParser.jobs.csv'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./SlurmLogParser.py', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert(f"Sacct file doesn't exist: {sacct_input_file}" in excinfo.value.output)

    @pytest.mark.order(after=['test_main_missing_sacct_input_file]'])
    def test_main_issue_20(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser/issues/20'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/jobs.csv'
        try:
            check_output(['./SlurmLogParser.py', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(expected_jobs_csv, output_csv, shallow=False))

    @pytest.mark.order(after=['test_main_issue_20]'])
    def test_main_issue_5(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser/issues/20'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/jobs.csv'
        try:
            check_output(['./SlurmLogParser.py', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(expected_jobs_csv, output_csv, shallow=False))

    @pytest.mark.order(after=['test_main_issue_5]'])
    def test_main_sacct_input_file(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/jobs.csv'
        try:
            check_output(['./SlurmLogParser.py', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))

    #@pytest.mark.order(after=['test_main_missing_sacct_output]'])
    def test_main_from_slurm(self):
        self.cleanup_files()
        # Only run this test if sacct is in the path so can run tests on instances without slurm
        try:
            check_output(["squeue"], stderr=subprocess.STDOUT, encoding='utf8') # nosec
        except (CalledProcessError, FileNotFoundError) as e:
            print(f"Slurm is not installed or available in the path.")
            return

        test_files_dir = 'test_files/SlurmLogParser'
        output_csv = 'output/SlurmLogParser/jobs.csv'
        check_output(['./SlurmLogParser.py', '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        # Can't check output because we won't know what it is.
