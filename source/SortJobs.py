#!/usr/bin/env python3
'''
Script to sort a CSV job file by eligible time.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import argparse
from collections import deque
import csv
import logging
from os import makedirs, path, rename, system
from os.path import basename, dirname, realpath
from SchedulerJobInfo import SchedulerJobInfo, logger as SchedulerJobInfo_logger
from tempfile import NamedTemporaryFile
from typing import List

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

class JobSorter:
    '''
    Read jobs from an input CSV file and write out a new CSV file with the jobs sorted by eligible time.
    '''
    def __init__(self, input_csv: str, output_csv: str):
        '''
        Constructor

        Args:
            input_csv (str): Filename of input CSV file. Can be None or ''.
            output_csv (str): Filename of output CSV file. Can be None or ''.
                If the directory of output_csv does not exist then it will be created.
        Raises:
            FileNotFoundError: If input_csv does not exist.
        Returns:
            None
        '''
        self._input_csv = input_csv
        self._output_csv = realpath(output_csv)
        self._max_buffer_size = 10000

        if not path.exists(input_csv):
            raise FileNotFoundError(f"Input CSV file doesn't exist: {input_csv}")
        self._input_csv_fh = open(input_csv, 'r', newline='')
        self._csv_reader = csv.reader(self._input_csv_fh, dialect='excel')
        self._input_line_number = 0
        self._read_csv_header()
        self._num_input_jobs = 0

        self._output_dir = dirname(self._output_csv)
        if not path.exists(self._output_dir):
            makedirs(self._output_dir)

        self._jobs = deque()

        self._output_files = []
        self._output_csv_fh = None
        self._csv_writer = None
        self._num_output_jobs = 0
        self._open_output_file()

        self._num_errors = 0

    def _open_output_file(self):
        self._output_csv_fh = NamedTemporaryFile(mode='w', prefix=f"{basename(self._output_csv)}-", dir=self._output_dir, delete=False)
        logger.debug(f"Created {self._output_csv_fh.name}")
        self._output_files.append(self._output_csv_fh.name)
        self._csv_writer = csv.writer(self._output_csv_fh, dialect='excel', lineterminator='\n')
        self._write_csv_header(self._csv_writer)

    def sort_jobs(self):
        while True:
            try:
                job = self._read_job_from_csv(self._csv_reader)
            except Exception: # nosec
                continue
            if not job:
                logger.debug(f"Read last job")
                break
            insertion_point = self._find_insertion_point(job)
            logger.debug(f"insertion_point: {insertion_point}")
            if len(self._jobs) and insertion_point == 0:
                # This job is older than the oldest in the buffer so need to start a new file
                self._flush_buffer()
                self._open_output_file()
            self._jobs.insert(insertion_point, job)
            if len(self._jobs) > self._max_buffer_size:
                self.write_job_to_csv(job)
        self._flush_buffer()
        self._merge_temp_files()

    def _find_insertion_point(self, job):
        '''
        Search backwards to find the insertion point.

        Starting from the end because it seems likely the insertion point will be there.

        1, 3, 5
        len = 5
        insertion_point(6) = 3
        insertion_point(4) = 2
        insertion_point(2) = 1
        insertion_point(0) = 0
        Returns:
            int: Index where job should be inserted.
        '''
        if not self._jobs:
            return 0
        if job.eligible_time_dt < self._jobs[0].eligible_time_dt:
            return 0
        insertion_point = len(self._jobs)
        while insertion_point:
            if job.eligible_time_dt >= self._jobs[insertion_point - 1].eligible_time_dt:
                break
            insertion_point -= 1
        return insertion_point

    def _flush_buffer(self):
        logger.debug(f"Flushing {len(self._jobs)} jobs")
        while len(self._jobs):
            job = self._jobs.popleft()
            self.write_job_to_csv(job, self._csv_writer)
        self._output_csv_fh.close()

    def _merge_temp_files(self):
        logger.debug(f"Merging {len(self._output_files)} files")
        files_to_merge = deque(self._output_files)
        self._output_files = []
        while len(files_to_merge) > 1:
            merged_file = self._merge_files(files_to_merge.popleft(), files_to_merge.popleft())
            files_to_merge.append(merged_file)
        rename(files_to_merge[0], self._output_csv)

    def _merge_files(self, file1, file2):
        merged_fh = NamedTemporaryFile(mode='w', prefix=f"{basename(self._output_csv)}-", dir=self._output_dir, delete=False)
        merged_filename = merged_fh.name
        csv_writer = csv.writer(merged_fh, dialect='excel', lineterminator='\n')
        self._write_csv_header(csv_writer)

        file1_fh = open(file1, 'r')
        csv_reader1 = csv.reader(file1_fh, dialect='excel')
        next(csv_reader1)

        file2_fh = open(file2, 'r')
        csv_reader2 = csv.reader(file2_fh, dialect='excel')
        next(csv_reader2)

        logger.debug(f"Merging {file1} and {file2} into {merged_filename}")
        job1 = self._read_job_from_csv(csv_reader1)
        job2 = self._read_job_from_csv(csv_reader2)
        while True:
            if job1.eligible_time_dt <= job2.eligible_time_dt:
                self.write_job_to_csv(job1, csv_writer)
                job1 = self._read_job_from_csv(csv_reader1)
                if not job1:
                    while job2:
                        self.write_job_to_csv(job2, csv_writer)
                        job2 = self._read_job_from_csv(csv_reader2)
                    break
            else:
                self.write_job_to_csv(job2, csv_writer)
                job2 = self._read_job_from_csv(csv_reader2)
                if not job2:
                    while job1:
                        self.write_job_to_csv(job1, csv_writer)
                        job1 = self._read_job_from_csv(csv_reader1)
                    break
        merged_fh.close()
        system(f"rm {file1}") # nosec
        system(f"rm {file2}") # nosec
        return merged_filename

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

    def _read_csv_header(self):
        '''
        Reads the CSV header line from the input csv file.

        Called by the constructor.

        Raises:
            RuntimeError: If no input csv file handle exists.
        '''
        if not self._csv_reader:
            raise RuntimeError(f"_read_csv_header called without self._csv_reader being set.")
        self._input_field_names = next(self._csv_reader)
        self._input_line_number += 1
        logger.debug(f"_input_job_field_names={self._input_field_names}")
        logger.debug(f"{len(self._input_field_names)} input fields")

    def _read_job_from_csv(self, csv_reader):
        '''
        Read a job from the input csv file.

        Read the CSV file until a job is found or an empty line is returned which signifies the EOF

        Args:
            csv_reader (csv.reader): csv reader
        Raises:
            ValueError: If the line has invalid field values.
        Returns:
            SchedulerJobInfo: Parsed job or None if there are no more jobs to be parsed.
        '''
        while True:
            try:
                field_values = next(csv_reader)
            except StopIteration:
                return None
            self._input_line_number += 1
            logger.debug(f"    {len(field_values)} field values")
            field_dict = {}
            for index, field_name in enumerate(self._input_field_names):
                if field_name[-3:] in ['_dt', '_td']:
                    continue
                field_dict[field_name] = field_values[index]
            try:
                job = SchedulerJobInfo.from_dict(field_dict)
            except ValueError as e:
                logger.exception(f"Exception reading {self._input_csv}, line {self._input_line_number}: {e}\nfields:\n{','.join(field_values)}")
                self._num_errors += 1
                raise
            self._num_input_jobs += 1
            return job
        raise RuntimeError('Did not find a job or EOF.')

    def _write_csv_header(self, csv_writer: csv.writer) -> None:
        '''
        Writes the CSV header line to the output csv file.

        Called by the constructor if an output csv filename provided.

        Args:
            csv_writer (csv.writer): CSV writer
        '''
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
        job_fields = dummy_job.__dict__
        self._output_field_names = []
        for field_name in job_fields.keys():
            if field_name[-3:] in ['_dt', '_td']:
                continue
            self._output_field_names.append(field_name)
        logger.debug(f"self._output_field_names={self._output_field_names}")
        csv_writer.writerow(self._output_field_names)

    def write_job_to_csv(self, job, csv_writer: csv.writer) -> None:
        '''
        Write a job to the output csv file.

        Args:
            csv_writer (csv.writer): CSV writer
        '''
        field_values = []
        for field_name in self._output_field_names:
            field_value = job.__dict__[field_name]
            if field_value == None:
                field_value = ''
            else:
                field_value = str(field_value)
            field_values.append(field_value)
        csv_writer.writerow(field_values)
        self._num_output_jobs += 1

def main() -> None:
    '''
    Main function when the script is called.

    Uses argparse to get command line arguments.
    '''
    parser = argparse.ArgumentParser(description="Parse LSF logs.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input-csv", required=True, help="CSV file with parsed job info.")
    parser.add_argument("--output-csv", required=True, help="CSV file with parsed job completion records")
    parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        SchedulerJobInfo_logger.setLevel(logging.DEBUG)

    logger.info('Started Job sorter')
    logger.info(f"Reading CSV input from {args.input_csv}")
    logger.info(f"Writing sorted jobs to {args.output_csv}")
    job_sorter = JobSorter(args.input_csv, args.output_csv)
    job_sorter.sort_jobs()

if __name__ == '__main__':
    main()
