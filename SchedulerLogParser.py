'''
Base class for parsing scheduler log files to collect high level job information.
Possible uses for the information is to analyze the EC2 instance types required to run the
jobs and the cost of running those jobs.
Potential other uses are to analyze resource utilization of the jobs to look for
underutilized cores or memory to optimize resource requests for the jobs.

The scheduler logs can have many millions of jobs. For example, one customer had 15 million
jobs in a month. This means that the parser cannot hold all of the jobs in memory so
it must be able to parse a job at a time and store it in a file or be used by another
program to process jobs one at a time.

The parsed output will be written to a CSV file if a filename is provided.

The parser can parse the scheduler job logs or the pre-parsed job logs from a CSV file.
This allows the logs to be processed once and then the results used multiple times.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

from abc import ABC, abstractmethod
import csv
import json
import logging
from os import makedirs, path
from os.path import dirname, realpath
from SchedulerJobInfo import SchedulerJobInfo, str_to_datetime
from typing import List

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

class SchedulerLogParser(ABC):
    '''
    Abstract base class for parser that parses job completion information from
    resource schedulers like LSF, Slurm, and Altair Accelerator.

    The parser can output a CSV that is not specific to any scheduler and it
    can parse that CSV as input for logs that have already been parsed.
    '''
    def __init__(self, input_csv: str, output_csv: str, starttime: str=None, endtime: str=None):
        '''
        Constructor

        Args:
            input_csv (str): Filename of input CSV file. Can be None or ''.
            output_csv (str): Filename of output CSV file. Can be None or ''.
                If the directory of output_csv does not exist then it will be created.
            starttime (str): Select jobs after the specified time
            endtime (str): Select jobs after the specified time
        Raises:
            FileNotFoundError: If input_csv does not exist.
        Returns:
            None
        '''
        self._input_csv = input_csv
        self._output_csv = output_csv
        self._starttime = starttime
        self._endtime = endtime

        if input_csv:
            if not path.exists(input_csv):
                raise FileNotFoundError(f"Input CSV file doesn't exist: {input_csv}")
            self._input_csv_fh = open(input_csv, 'r', newline='')
            self._csv_dict_reader = csv.DictReader(self._input_csv_fh, dialect='excel')
            self._input_line_number = 1
        else:
            self._input_csv_fh = None
        self._num_input_jobs = 0

        if output_csv:
            output_dir = dirname(realpath(output_csv))
            if not path.exists(output_dir):
                makedirs(output_dir)
            self._output_csv_fh = open(output_csv, 'w', newline='')
            self._output_field_names = self._get_job_field_names()
            # Profiling showed that the dict writer is slightly slower
            self._use_csv_dict_writer = False
            if not self._use_csv_dict_writer:
                self._csv_writer = csv.writer(self._output_csv_fh, dialect='excel', lineterminator='\n')
            else:
                self._csv_dict_writer = csv.DictWriter(self._output_csv_fh, self._output_field_names, dialect='excel', lineterminator='\n', extrasaction='ignore')
            self._write_csv_header()
        else:
            self._output_csv_fh = None
        self._num_output_jobs = 0

        if self._starttime:
            self._starttime_dt = str_to_datetime(self._starttime)
        else:
            self._starttime_dt = None
        if self._endtime:
            self._endtime_dt = str_to_datetime(self._endtime)
        else:
            self._endtime_dt = None

        self.total_jobs_outside_time_window = 0

        self._num_errors = 0

    @abstractmethod
    def parse_job(self) -> SchedulerJobInfo:
        '''
        Parse a job.

        This is an abstract method that must be defined in the derived class.

        Raises:
            RuntimeError: If the derived class doesn't implement this function. This should never happen because Python should not allow you to create an instance of an abstract base class.
        Returns:
            SchedulerJobInfo: Parsed job or None if there are no more jobs to be parsed.
        '''
        raise RuntimeError('Not implemented')

    def num_input_jobs(self):
        '''
        Number of jobs returned by parse_job.

        Returns:
            int: Number of jobs parsed
        '''
        return self._num_input_jobs

    def num_output_jobs(self):
        '''
        Number of jobs written to output csv file.

        Returns:
            int: Number of jobs written to output csv file
        '''
        return self._num_output_jobs

    @staticmethod
    def _get_job_field_names():
        dummy_job = SchedulerJobInfo(
            job_id = '1',
            resource_request = 'rusage[mem=6291456,xcelium_sc=1:duration=1m]',
            num_cores = 1,
            max_mem_gb = 1.1,
            num_hosts = 1,

            submit_time = '1970-01-01T00:00:00',
            start_time = '1970-01-01T00:00:01',
            finish_time = '1970-01-01T00:00:05',
        )
        field_names = []
        job_dict = dummy_job.__dict__
        for field_name in job_dict.keys():
            if field_name[-3:] in ['_dt', '_td']:
                continue
            field_names.append(field_name)
        logger.debug(f"field_names={field_names}")
        return field_names

    def _write_csv_header(self) -> None:
        '''
        Writes the CSV header line to the output csv file.

        Called by the constructor if an output csv filename provided.

        Raises:
            RuntimeError: If no output csv file handle exists.
        '''
        if not self._output_csv_fh:
            raise RuntimeError(f"_write_csv_header called without self._output_csv_fh being set.")
        if not self._use_csv_dict_writer:
            if not self._csv_writer:
                raise RuntimeError(f"_write_csv_header called without self._csv_writer being set.")
            self._csv_writer.writerow(self._output_field_names)
        else:
            if not self._csv_dict_writer:
                raise RuntimeError(f"_write_csv_header called without self._csv_dict_writer being set.")
            self._csv_dict_writer.writeheader()

    def write_job_to_csv(self, job) -> None:
        '''
        Write a job to the output csv file.

        Raises:
            RuntimeError: If no output csv file handle exists.
        '''
        if not self._output_csv_fh:
            raise RuntimeError(f"write_job_to_csv called without self._output_csv_fh being set.")
        if not self._use_csv_dict_writer:
            if not self._csv_writer:
                raise RuntimeError(f"write_job_to_csv called without self._csv_writer being set.")
        else:
            if not self._csv_dict_writer:
                raise RuntimeError(f"_write_csv_header called without self._csv_dict_writer being set.")

        if not self._use_csv_dict_writer:
            field_values = []
            for field_name in self._output_field_names:
                field_value = job.__dict__[field_name]
                if field_value == None:
                    field_value = ''
                else:
                    field_value = str(field_value)
                field_values.append(field_value)
            self._csv_writer.writerow(field_values)
        else:
            self._csv_dict_writer.writerow(job.__dict__)
        self._num_output_jobs += 1

    def _read_job_from_csv(self):
        '''
        Read a job from the input csv file.

        Read the CSV file until a job is found or an empty line is returned which signifies the EOF

        Raises:
            RuntimeError: If no input csv file handle exists.
            ValueError: If the line has invalid field values.
        Returns:
            SchedulerJobInfo: Parsed job or None if there are no more jobs to be parsed.
        '''
        if not self._csv_dict_reader:
            raise RuntimeError(f"_read_job_from_csv called without self._csv_dict_reader being set.")
        while True:
            try:
                job_dict = next(self._csv_dict_reader)
            except StopIteration:
                return None
            self._input_line_number += 1
            try:
                job = SchedulerJobInfo.from_dict(job_dict)
            except Exception as e:
                logger.error(f"Exception reading {self._input_csv}, line {self._input_line_number}: {e}\njob_dict: {json.dumps(job_dict, indent=4)}")
                self._num_errors += 1
                raise
            self._num_input_jobs += 1
            return job

    def _job_in_time_window(self, job: SchedulerJobInfo) -> bool:
        '''
        Check if the job is inside the time window.

        Args:
            job_fields (SchedulerJobInfo): Job fields
        Returns:
            bool: True if the job was active in the time window.
        '''
        in_time_window = True
        if self._starttime:
            if job.submit_time_dt < self._starttime_dt and job.finish_time_dt < self._starttime_dt:
                in_time_window = False
                logger.debug(f"Skipping {job.job_id} submit {job.submit_time} finish {job.finish_time} before starttime {self._starttime}")
        if self._endtime and in_time_window:
            if job.submit_time_dt > self._endtime_dt and job.finish_time_dt > self._endtime_dt:
                in_time_window = False
                logger.debug(f"Skipping {job.job_id} submit {job.submit_time} finish {job.finish_time} after endtime {self._endtime}")
        return in_time_window
