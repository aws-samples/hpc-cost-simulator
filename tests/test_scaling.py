'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import filecmp
from LSB_ACCT_FIELDS import LSB_ACCT_RECORD_FORMATS
from psutil import virtual_memory, swap_memory
from os import makedirs, path, system
from os.path import dirname
from LSFLogParser import LSFLogParser
from MemoryUtils import MEM_GB, MEM_KB
import psutil
import pytest
from SchedulerJobInfo import SchedulerJobInfo
import subprocess
from subprocess import CalledProcessError, check_output
from test_ModelComputeCluster import order as last_order

order = last_order // 100 * 100 + 100
assert order == 800

class TestScaling:
    global order

    def cleanup_output_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    def gen_input_csv(self, filename, number_of_tests):
        csv_header = 'job_id,num_cores,max_mem_gb,num_hosts,submit_time,start_time,finish_time,resource_request,ineligible_pend_time,eligible_time,requeue_time,wait_time,run_time,exit_status,ru_inblock,ru_majflt,ru_maxrss,ru_minflt,ru_msgrcv,ru_msgsnd,ru_nswap,ru_oublock,ru_stime,ru_utime'
        csv_line = '281,20,1.4697265625,20,2022-02-03T21:33:15,2022-02-03T21:33:15,2022-02-03T21:33:28,,0:00:00,2022-02-03T21:33:15,0:00:00,0:00:00,0:00:13,0,928,4,1541580,3721,0,0,0,8,0:00:00.314794,0:00:01.777730'
        with open(filename, 'w') as input_csv_fh:
            input_csv_fh.write(f"{csv_header}\n")
            for idx in range(number_of_tests):
                input_csv_fh.write(f"{csv_line}\n")

    order += 1
    @pytest.mark.order(order)
    def test_10k(self):
        ''''
        Analyze 10,000 jobs to see what memory utilization and run time does.

        real    0m7.325s
        user    0m4.279s
        sys     0m2.528s

        1365 jobs/s
        '''
        self.cleanup_output_files()
        output_dir = 'output/scaling'
        input_csv = path.join(output_dir, 'jobs-10k.csv')
        output_csv = path.join(output_dir, 'jobs-10k-out.csv')
        number_of_tests = 10000

        makedirs(output_dir)

        self.gen_input_csv(input_csv, number_of_tests)

        try:
            output = check_output(['./JobAnalyzer.py', '--disable-version-check', '--acknowledge-config', '--output-csv', output_csv, '--output-dir', output_dir, 'csv', '--input-csv', input_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")

    order += 1
    @pytest.mark.order(order)
    @pytest.mark.skip(reason='Runs too long')
    def test_100k(self):
        ''''
        Analyze 100,000 jobs to see what memory utilization and run time does.

        real    0m38.047s
        user    0m36.703s
        sys     0m1.324s

        2628.32 jobs/s
        '''
        self.cleanup_output_files()
        output_dir = 'output/scaling'
        input_csv = path.join(output_dir, 'jobs-100k.csv')
        output_csv = path.join(output_dir, 'jobs-100k-out.csv')
        number_of_tests = 100000

        makedirs(output_dir)

        self.gen_input_csv(input_csv, number_of_tests)

        try:
            output = check_output(['./JobAnalyzer.py', '--disable-version-check', '--acknowledge-config', '--input-csv', input_csv, '--output-csv', output_csv, '--output-dir', output_dir], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")

    order += 1
    @pytest.mark.order(order)
    @pytest.mark.skip(reason='Runs too long')
    def test_1M(self):
        ''''
        Analyze 1,000,000 jobs to see what memory utilization and run time does.

        real    6m35.355s
        user    6m15.130s
        sys     0m19.864s

        2529 jobs/s
        '''
        self.cleanup_output_files()
        output_dir = 'output/scaling'
        input_csv = path.join(output_dir, 'jobs-1M.csv')
        output_csv = path.join(output_dir, 'jobs-1M-out.csv')
        number_of_tests = 1000000

        makedirs(output_dir)

        self.gen_input_csv(input_csv, number_of_tests)

        try:
            output = check_output(['./JobAnalyzer.py', '--disable-version-check', '--acknowledge-config', '--input-csv', input_csv, '--output-csv', output_csv, '--output-dir', output_dir], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")

    order += 1
    @pytest.mark.order(order)
    @pytest.mark.skip(reason='Runs too long')
    def test_15M(self):
        ''''
        Analyze 15,000,000 jobs to see what memory utilization and run time does.
        '''
        self.cleanup_output_files()
        output_dir = 'output/scaling'
        input_csv = path.join(output_dir, 'jobs-15M.csv')
        output_csv = path.join(output_dir, 'jobs-15M-out.csv')
        number_of_tests = 15000000

        makedirs(output_dir)

        self.gen_input_csv(input_csv, number_of_tests)

        try:
            output = check_output(['./JobAnalyzer.py', '--disable-version-check', '--acknowledge-config', '--input-csv', input_csv, '--output-csv', output_csv, '--output-dir', output_dir], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")

    order += 1
    @pytest.mark.order(order)
    @pytest.mark.skip(reason='Runs too long')
    def test_150M(self):
        ''''
        Analyze 150,000,000 jobs to see what memory utilization and run time does.

        real    932m7.336s   15:32:07.336
        user    898m16.430s  14:58:16.430
        sys     31m31.261s      31:31.261

        2682 jobs/s
        '''
        self.cleanup_output_files()
        output_dir = 'output/scaling'
        input_csv = path.join(output_dir, 'jobs-150M.csv')
        output_csv = path.join(output_dir, 'jobs-150M-out.csv')
        number_of_tests = 150000000

        makedirs(output_dir)

        self.gen_input_csv(input_csv, number_of_tests)

        try:
            output = check_output(['./JobAnalyzer.py', '--disable-version-check', '--acknowledge-config', '--input-csv', input_csv, '--output-csv', output_csv, '--output-dir', output_dir], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")
