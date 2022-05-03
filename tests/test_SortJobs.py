'''
Test the SortJobs.py module and script.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import csv
from datetime import datetime, timedelta
import filecmp
import logging
from os import path, system
from os.path import abspath, dirname
import pytest
import random
from SchedulerJobInfo import SchedulerJobInfo, logger as SchedulerJobInfo_logger
from SortJobs import JobSorter, logger as JobSorter_logger
import subprocess
from subprocess import CalledProcessError, check_output
import unittest

@pytest.mark.order(6, after=['tests/test_SlurmLogParser.py::TestSlurmLogParser::test_main_from_slurm'])
class TestSortJobs(unittest.TestCase):

    REPO_DIR = abspath(f"{dirname(__file__)}/..")

    OUTPUT_DIR = path.join(REPO_DIR, 'output/SortJobs')

    region = 'eu-west-1'

    def _cleanup_output_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    def test_no_args(self):
        self._cleanup_output_files()
        with pytest.raises(CalledProcessError) as excinfo:
            check_output(['./SortJobs.py', '--output-dir', self.OUTPUT_DIR], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert('the following arguments are required: --input-csv, --output-csv' in excinfo.value.output)
        assert(excinfo.value.returncode == 2)

    @pytest.mark.order(after='test_no_args')
    def test_from_accelerator(self):
        '''
        Test SortJobs.py when parsing jobs from Accelerator logs.
        '''
        self._cleanup_output_files()
        unsorted_jobs_csv = 'test_files/AcceleratorLogParser/exp_jobs.csv'
        sorted_jobs_csv = path.join(self.OUTPUT_DIR, 'sorted_jobs.csv')
        expected_sorted_jobs_csv = path.join(self.OUTPUT_DIR, 'sorted_jobs.csv')
        # Put this in a try block so that can print the output if an unexpected exception occurs.
        try:
            check_output(['./SortJobs.py', '--input-csv', unsorted_jobs_csv, '--output-csv', sorted_jobs_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.stdout}")
            raise
        assert(filecmp.cmp(sorted_jobs_csv, expected_sorted_jobs_csv, shallow=False))

    @pytest.mark.order(after='test_no_args')
    def test_random(self):
        '''
        Test SortJobs.py when parsing jobs with random start times.
        '''
        self._cleanup_output_files()

        unsorted_jobs_csv = 'test_files/AcceleratorLogParser/exp_jobs.csv'
        sorted_jobs_csv = path.join(self.OUTPUT_DIR, 'sorted_jobs.csv')

        job_sorter = JobSorter(unsorted_jobs_csv, sorted_jobs_csv)

        unsorted_jobs_csv = path.join(self.OUTPUT_DIR, 'unsorted_jobs.csv')
        output_fh = open(unsorted_jobs_csv, 'w')
        csv_writer = csv.writer(output_fh, dialect='excel', lineterminator='\n')
        job_sorter._write_csv_header(csv_writer)

        number_of_jobs = 10000
        min_year = 1980
        max_year = 2022
        start = datetime(min_year, 1, 1, 0, 0, 0)
        end = start + timedelta(days=(365 * (max_year - min_year + 1)))
        for i in range(number_of_jobs):
            eligible_time = SchedulerJobInfo.datetime_to_str(start + (end - start) * random.random())
            dummy_job = SchedulerJobInfo(
                job_id = '1',
                resource_request = 'linux64',
                num_cores = 1,
                max_mem_gb = 1.1,
                num_hosts = 1,

                submit_time = eligible_time,
                start_time = eligible_time,
                finish_time = eligible_time,
            )
            job_sorter.write_job_to_csv(dummy_job, csv_writer)
        output_fh.close()
        job_sorter._output_csv_fh.close()
        system(f"rm {job_sorter._output_csv_fh.name}")
        print(f"Created {unsorted_jobs_csv}")

        print(f"Sorting {unsorted_jobs_csv} into {sorted_jobs_csv}")
        #JobSorter_logger.setLevel(logging.DEBUG)
        #SchedulerJobInfo_logger.setLevel(logging.DEBUG)
        job_sorter = JobSorter(unsorted_jobs_csv, sorted_jobs_csv)
        job_sorter.sort_jobs()
        assert(job_sorter._num_errors == 0)

        sorted_jobs_csv_fh = open(sorted_jobs_csv, 'r')
        csv_reader = csv.reader(sorted_jobs_csv_fh, dialect='excel')
        next(csv_reader)
        previous_job = job_sorter._read_job_from_csv(csv_reader)
        while previous_job:
            job = job_sorter._read_job_from_csv(csv_reader)
            if not job:
                break
            assert(job.eligible_time_dt >= previous_job.eligible_time_dt)
