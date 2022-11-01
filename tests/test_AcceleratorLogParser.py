'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import filecmp
from os import path, system
from os.path import dirname
import pytest
import subprocess
from subprocess import CalledProcessError, check_output
from test_SchedulerJobInfo import order as last_order

order = last_order // 100 * 100 + 100
assert order == 100

class TestAcceleratorLogParser:
    def cleanup_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    global order
    order += 1
    @pytest.mark.order(order)
    def test_main_no_args(self):
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./AcceleratorLogParser.py', '--disable-version-check'], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("AcceleratorLogParser.py: error: one of the arguments --show-data-collection-cmd --sql-output-file --sql-input-file is required" in excinfo.value.output)

    order += 1
    @pytest.mark.order(order)
    def test_main_no_sql_file_arg(self):
        output_csv = 'output/AcceleratorLogParser/jobs.csv'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./AcceleratorLogParser.py', '--disable-version-check', '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("AcceleratorLogParser.py: error: one of the arguments --show-data-collection-cmd --sql-output-file --sql-input-file is required" in excinfo.value.output)

    order += 1
    @pytest.mark.order(order)
    def test_main_both_sql_file_args(self):
        sql_output_file = 'output/sql_output.txt'
        sql_input_file = 'test_files/sql_output.txt'
        output_csv = 'output/AcceleratorLogParser/jobs.csv'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./AcceleratorLogParser.py', '--disable-version-check', '--sql-output-file', sql_output_file, '--sql-input-file', sql_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("AcceleratorLogParser.py: error: argument --sql-input-file: not allowed with argument --sql-output-file" in excinfo.value.output)

    order += 1
    @pytest.mark.order(order)
    def test_main_no_output_csv_file_args(self):
        sql_output_file = 'output/sql_output.txt'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./AcceleratorLogParser.py', '--disable-version-check', '--sql-output-file', sql_output_file], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert("the following arguments are required: --output-csv" in excinfo.value.output)

    order += 1
    @pytest.mark.order(order)
    def test_main_sql_input_file(self):
        self.cleanup_files()
        sql_input_file = 'test_files/AcceleratorLogParser/sql_output.txt'
        output_csv = 'output/AcceleratorLogParser/jobs.csv'
        exp_output_csv = 'test_files/AcceleratorLogParser/exp_jobs.csv'
        try:
            output = check_output(['./AcceleratorLogParser.py', '--disable-version-check', '--sql-input-file', sql_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")
        assert(filecmp.cmp(output_csv, exp_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_sql_input_file_default_mem_1GB(self):
        self.cleanup_files()
        sql_input_file = 'test_files/AcceleratorLogParser/sql_output.txt'
        output_csv = 'output/AcceleratorLogParser/jobs.csv'
        exp_output_csv = 'test_files/AcceleratorLogParser/exp_jobs_default_mem_1gb.csv'
        try:
            output = check_output(['./AcceleratorLogParser.py', '--disable-version-check', '--default-mem-gb', '1', '--sql-input-file', sql_input_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")
        assert(filecmp.cmp(output_csv, exp_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_sql_output_file(self):
        self.cleanup_files()
        try:
            result = subprocess.run(["nc", "-h"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='UTF-8') # nosec
        except FileNotFoundError as e:
            logger.info(f"Cannot find nc command. You must set up the Accelerator environment (source vovrc.sh) or specify --sql-input-file.")
            return
        except CalledProcessError as e:
            logger.info(f"'nc -h' failed. You must set up the Accelerator environment (source vovrc.sh) or specify --sql-input-file.")
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.stdout}")
            return
        output = result.stdout
        if result.returncode != 2 or 'Altair Engineering.' not in output:
            print(f"Unexpected result from 'nc -h'\nreturncode: expected 2, actual {result.returncode}\noutput:\n{output}")
            if 'Usage: nc' in output:
                print(f"'nc -h' called ncat (netcat), not Altair nc.")
            print(f"'nc -h' failed. You must set up the Accelerator environment (source vovrc.sh) or specify --sql-input-file.")
            return

        sql_output_file = 'output/sql_output.txt'
        output_csv = 'output/AcceleratorLogParser/jobs.csv'
        try:
            check_output(['./AcceleratorLogParser.py', '--disable-version-check', '--sql-output-file', sql_output_file, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        assert(path.exists(sql_output_file))
        assert(path.exists(output_csv))
