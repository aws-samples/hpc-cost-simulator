'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import filecmp
import logging
from os import path, system
from os.path import dirname
import pytest
from MemoryUtils import logger as MemoryUtils_logger, mem_string_to_float, mem_string_to_int, MEM_KB, MEM_MB, MEM_GB, MEM_TB, MEM_PB
from SlurmLogParser import logger as SlurmLogParser_logger, SlurmLogParser
import subprocess
from subprocess import CalledProcessError, check_output
from test_LSFLogParser import order as last_order

order = last_order // 100 * 100 + 100
assert order == 400

class TestSlurmLogParser:
    global order
    def cleanup_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    order += 1
    @pytest.mark.order(order)
    def test_mem_string_to_float(self):
        MemoryUtils_logger.setLevel(logging.DEBUG)
        assert(mem_string_to_float('2.13') == 2.13)
        assert(mem_string_to_float('2.475k') == 2.475 * MEM_KB)
        assert(mem_string_to_float('1e-3M') == 1e-3 * MEM_MB)
        assert(mem_string_to_float('1e3') == 1000)
        # Test invalid values
        # Test a single extra character that isn't a legal suffix
        for value in ['2.475kc', '0n', '1z', 'abc', '1.2kcn']:
            with pytest.raises(ValueError) as excinfo:
                mem_string_to_float(value)
            print(excinfo.value)
            assert(str(excinfo.value) == f"could not convert string to float: '{value}'")

    order += 1
    @pytest.mark.order(order)
    def test_mem_string_to_int(self):
        MemoryUtils_logger.setLevel(logging.DEBUG)
        # Test float values
        assert(mem_string_to_int('2.13') == 2)
        assert(mem_string_to_int('2.50') == 2)
        assert(mem_string_to_int('2.51') == 3)
        assert(mem_string_to_int('1e-3') == 0)
        assert(mem_string_to_int('1e3') == 1e3)
        # Test invalid values
        # Test a single extra character that isn't a legal suffix
        for value in ['0n', '1z', 'abc']:
            with pytest.raises(ValueError) as excinfo:
                mem_string_to_int(value)
            print(excinfo.value)
            assert(str(excinfo.value) == f"could not convert string to float: '{value}'")

    order += 1
    @pytest.mark.order(order)
    def test_slurm_mem_string_to_int(self):
        # Test with no suffix
        assert(SlurmLogParser._slurm_mem_string_to_int('2k') == (2*MEM_KB, ''))
        assert(SlurmLogParser._slurm_mem_string_to_int('2.475k') == (round(2.475 * MEM_KB), ''))
        # Test with 'c' suffix
        assert(SlurmLogParser._slurm_mem_string_to_int('0c') == (0, 'c'))
        assert(SlurmLogParser._slurm_mem_string_to_int('2kc') == (2*MEM_KB, 'c'))
        assert(SlurmLogParser._slurm_mem_string_to_int('5tc') == (5*MEM_TB, 'c'))
        assert(SlurmLogParser._slurm_mem_string_to_int('6pc') == (6*MEM_PB, 'c'))
        assert(SlurmLogParser._slurm_mem_string_to_int('2.475kc') == (round(2.475 * MEM_KB), 'c'))
        # Test with 'n' suffix
        assert(SlurmLogParser._slurm_mem_string_to_int('0n') == (0, 'n'))
        assert(SlurmLogParser._slurm_mem_string_to_int('2kn') == (2*MEM_KB, 'n'))
        assert(SlurmLogParser._slurm_mem_string_to_int('5tn') == (5*MEM_TB, 'n'))
        assert(SlurmLogParser._slurm_mem_string_to_int('6pn') == (6*MEM_PB, 'n'))
        assert(SlurmLogParser._slurm_mem_string_to_int('2.475kn') == (round(2.475 * MEM_KB), 'n'))
        # Test float values
        assert(SlurmLogParser._slurm_mem_string_to_int('2.13') == (2, ''))
        assert(SlurmLogParser._slurm_mem_string_to_int('2.50') == (2, ''))
        assert(SlurmLogParser._slurm_mem_string_to_int('2.51') == (3, ''))
        assert(SlurmLogParser._slurm_mem_string_to_int('1e-3') == (0, ''))
        assert(SlurmLogParser._slurm_mem_string_to_int('1e3') == (1000, ''))
        # Test a single extra character that isn't a legal suffix
        invalid_values = [
            '1z',
            '3mm',
            '4gm',
            'abc',
            '1.2kcn',
        ]
        for value in invalid_values:
            with pytest.raises(ValueError) as excinfo:
                SlurmLogParser._slurm_mem_string_to_int(value)
            print(excinfo.value)
            assert(f"could not convert string to float:" in str(excinfo.value))

    order += 1
    @pytest.mark.order(order)
    def test_main_no_args(self):
        self.cleanup_files()
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./SlurmLogParser.py', '--disable-version-check'], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("SlurmLogParser.py: error: one of the arguments --show-data-collection-cmd --sacct-output-file --sacct-input-file is required" in excinfo.value.output)

    order += 1
    @pytest.mark.order(order)
    def test_main_show_data_collection_command(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser'
        sacct_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser.jobs.csv'
        try:
            output = check_output(['./SlurmLogParser.py', '--disable-version-check', '--show-data-collection-cmd'], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")
        assert("Run the following command to save the job information to a file:" in output)

    order += 1
    @pytest.mark.order(order)
    def test_main_no_sacct_file(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser'
        sacct_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser.jobs.csv'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./SlurmLogParser.py', '--disable-version-check', '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("SlurmLogParser.py: error: one of the arguments --show-data-collection-cmd --sacct-output-file --sacct-input-file is required" in excinfo.value.output)

    order += 1
    @pytest.mark.order(order)
    def test_main_missing_sacct_input_file(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt-invalid')
        output_csv = 'output/SlurmLogParser.jobs.csv'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert(f"Sacct file doesn't exist: {sacct_input_file}" in excinfo.value.output)

    order += 1
    @pytest.mark.order(order)
    def test_issue_gl_20(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser/issues/gl-20'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/jobs.csv'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("4 errors while parsing jobs" in excinfo.value.output)

        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_issue_5(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser/issues/5'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/jobs.csv'
        try:
            check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_issue_17(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser/issues/17'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/issues/17/jobs.csv'
        try:
            output = check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(output)
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_issue_59(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser/issues/59'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/issues/59/jobs.csv'
        try:
            output = check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv, '-d'], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(output)
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_issue_61(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser/issues/61'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/issues/61/jobs.csv'
        with pytest.raises(CalledProcessError) as excinfo:
            output = check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv, '-d'], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("1 errors while parsing jobs" in excinfo.value.output)

        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_sacct_input_file_v1(self):
        '''
        Test with the original format that doesn't have the Partition field
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser'
        sacct_input_file = path.join(test_files_dir, 'sacct-output-v1.txt')
        output_csv = 'output/SlurmLogParser/jobs.csv'
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs-v1.csv')
        try:
            check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_sacct_input_file_v2(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser'
        sacct_input_file = path.join(test_files_dir, 'sacct-output-v2.txt')
        output_csv = 'output/SlurmLogParser/jobs.csv'
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs-v2.csv')
        try:
            check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
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
        check_output(['./SlurmLogParser.py', '--disable-version-check', '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        # Can't check output because we won't know what it is.

    order += 1
    @pytest.mark.order(order)
    def test_main_multi_node(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser/multi-node'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/jobs.csv'
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        try:
            output = check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_array(self):
        self.cleanup_files()
        test_files_dir = 'test_files/SlurmLogParser/array'
        sacct_input_file = path.join(test_files_dir, 'sacct-output.txt')
        output_csv = 'output/SlurmLogParser/jobs.csv'
        try:
            check_output(['./SlurmLogParser.py', '--disable-version-check', '--sacct-input-file', sacct_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        expected_jobs_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(output_csv, expected_jobs_csv, shallow=False))
