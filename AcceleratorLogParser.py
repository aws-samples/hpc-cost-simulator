#!/usr/bin/env python3
'''
Parse Altair Accelerator logfiles and write job information to a yaml file.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import argparse
import logging
from math import ceil
from MemoryUtils import MEM_MB, MEM_GB, mem_string_to_float, mem_string_to_int
from os import makedirs, path, remove
from os.path import dirname
import re
from SchedulerJobInfo import SchedulerJobInfo
from SchedulerLogParser import SchedulerLogParser, logger as SchedulerLogParser_logger
import subprocess # nosec
from subprocess import CalledProcessError, check_output # nosec

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.setLevel(logging.INFO)

class AcceleratorLogParser(SchedulerLogParser):
    '''
    Parse Accelerator vovsql_query output to get job completion information.
    '''

    def __init__(self, default_mem_gb: float=0.0, sql_input_file: str=None, sql_output_file: str=None, output_csv: str=None, starttime: str=None, endtime: str=None):
        '''
        Constructor

        Args:
            sql_input_file (str): File with the output of vovsql_query so can process output offline.
            sql_output_file (str): File where vovsql_query output will be written. Can be used to process job data on another machine without vovsql_query access.
            output_csv (str): CSV file where parsed jobs will be written.
            starttime (str): Select jobs after the specified time
            endtime (str): Select jobs after the specified time
        Raises:
            FileNotFoundError: If sql_input_file doesn't exist.
        '''
        super().__init__(None, output_csv, starttime, endtime)

        if default_mem_gb:
            self._default_mem_gb = default_mem_gb
            logger.debug(f"self._default_mem_gb set to {self._default_mem_gb}")
        else:
            self._default_mem_gb = AcceleratorLogParser.DEFAULT_MEMORY_GB

        self._sql_input_file = sql_input_file
        self._sql_output_file = sql_output_file

        if sql_input_file and sql_output_file:
            raise ValueError(f"Cannot specify sql_input_file and sql_output_file.")
        if not(sql_input_file or sql_output_file):
            raise ValueError(f"Must specify either sql_input_file or sql_output_file")
        if sql_input_file and not path.exists(sql_input_file):
            raise FileNotFoundError(f"sql_input_file doesn't exist: {sql_input_file}")
        if sql_output_file:
            sql_output_dir = dirname(sql_output_file)
            if not path.exists(sql_output_dir):
                makedirs(sql_output_dir)

        self._sql_fh = None
        self._line_number = 0
        self._next_job_fields = {}
        self._eof = False

    def parse_jobs(self) -> None:
        '''
        Parse all the jobs from the Accelerator vovsql_query command.

        Returns:
            None
        '''
        job = True
        while job:
            job = self.parse_job()
        logger.debug(f"Parsed {self.num_output_jobs()} jobs.")

    DEFAULT_MEMORY_GB = round(100 * MEM_MB / MEM_GB, 3)

    _VOVSQL_JOBS_COLUMN_TUPLES = [
        ('jobs.id', 'd'),
        ('jobs.submittime', 'd'),
        ('jobs.starttime', 'd'),
        ('jobs.endtime', 'd'),
        ('resources.name', 's'),
        ('jobs.exitstatus', 'd'),
        ('jobs.maxram', 'd'),
        ('jobs.maxvm', 'd'),
        ('jobs.cputime', 'f'),
        ('jobs.susptime', 'd'),
        ]

    _VOVSQL_COLUMNS = [field_tuple[0] for field_tuple in _VOVSQL_JOBS_COLUMN_TUPLES]

    _VOVSQL_QUERY_COMMAND_LIST = [
        "nc", "cmd", "vovsql_query", "-e",
        f"select {', '.join(_VOVSQL_COLUMNS)} from jobs inner join resources on jobs.resourcesid=resources.id"
    ]

    _VOVSQL_QUERY_COMMAND = f"nc cmd vovsql_query -e \"select {', '.join(_VOVSQL_COLUMNS)} from jobs inner join resources on jobs.resourcesid=resources.id\""

    def parse_job(self):
        if self._eof:
            return None
        if not self._sql_fh:
            if not self._sql_input_file:
                self._call_vovsql_query()
            self._sql_fh = open(self._sql_input_file, 'r')
        while True:
            line = self._sql_fh.readline()
            self._line_number += 1
            if line == '':
                logger.debug(f"Hit EOF at line {self._line_number}")
                self._eof = True
                self._clean_up()
                return None
            line = line.lstrip().rstrip()
            logger.debug(f"line {self._line_number}: '{line}'")
            if re.match(f'^\s*$', line):
                logger.debug("    Skipping blank line")
                continue
            if re.match(f'^\s*#', line):
                logger.debug("    Skipping comment line")
                continue
            raw_fields = line.split(' ')
            fields = []
            while len(raw_fields):
                field = raw_fields.pop(0)
                if field.startswith('{'):
                    while not field.endswith('}'):
                        field += ' ' + raw_fields.pop(0)
                fields.append(field)
            logger.debug(f"    {len(fields)} fields: {fields}")
            job_fields = {}
            for field_tuple in self._VOVSQL_JOBS_COLUMN_TUPLES:
                field_name = field_tuple[0]
                field_format = field_tuple[1]
                field_value = fields.pop(0)
                logger.debug(f"    {field_name}: '{field_value}'")
                if field_value:
                    try:
                        if field_format == 'd':
                            field_value = mem_string_to_int(field_value)
                        elif field_format == 'f':
                            field_value = mem_string_to_float(field_value)
                        elif field_format == 's':
                            pass
                        else:
                            raise ValueError(f"Invalid format of {field_format} for field {field_name}")
                    except ValueError as e:
                        logger.exception(f"Unable to convert field {field_name} to format {field_format}: {field_value}")
                        raise
                job_fields[field_name] = field_value
            job_id = job_fields['jobs.id']
            logger.debug(f"    job_id: {job_id}")
            job = self._create_job_from_job_fields(job_fields)
            if self._job_in_time_window(job):
                if self._output_csv_fh:
                    self.write_job_to_csv(job)
                self._num_input_jobs += 1
                return job
            job = None

    def _call_vovsql_query(self):
        '''
        Call vovsql_query to get job information
        '''
        if self._sql_input_file:
            raise RuntimeError("Cannot call _call_vovsql_query when sql_input_file given for input")

        # Create a file handle to write the vovsql_query output to.
        sql_write_fh = open(self._sql_output_file, 'w')

        logger.debug(f"Calling vovsql_query:\n{' '.join(self._VOVSQL_QUERY_COMMAND_LIST)}")
        rc = subprocess.run(self._VOVSQL_QUERY_COMMAND_LIST, stdout=sql_write_fh, stderr=subprocess.STDOUT, encoding='UTF-8').returncode # nosec
        sql_write_fh.close()
        if rc:
            logger.error(f"vovsql_query failed with rc={rc}. See {self._sql_output_file}")
            exit(1)
        self._sql_input_file = self._sql_output_file

    def _create_job_from_job_fields(self, job_fields):
        resources = job_fields['resources.name']

        # Get number of requested cores from the resources field
        # If not found then set the default to 1 core.
        matches = re.search(r'CORES/(\d+)', resources)
        if matches:
            num_cores = int(matches.group(1))
            logger.debug(f"num_cores={num_cores}")
        else:
            num_cores = 1
            logger.debug(f"num_cores not found. Default to {num_cores}")

        maxram = job_fields['jobs.maxram'] * MEM_MB
        logger.debug(f"maxram={maxram}")

        # Get the amount of memory requested for the job.
        # If not found then use the maximum of the default memory request or 110% of the actual memory used.
        matches = re.search(r'RAM/(\d+)', resources)
        if matches:
            max_mem_gb = float(matches.group(1)) * MEM_MB / MEM_GB
            logger.debug(f"max_mem_gb={max_mem_gb}")
        else:
            # Request max of default memory or 10% more than used
            max_mem_gb = max(self._default_mem_gb, round(maxram * 1.10 / MEM_GB, 3))
            logger.debug(f"max_mem_gb not found default to {max_mem_gb}")
        num_hosts = 1
        job = SchedulerJobInfo(
            job_id = job_fields['jobs.id'],
            resource_request = resources,
            num_cores = num_cores,
            max_mem_gb = max_mem_gb,
            num_hosts = num_hosts,

            submit_time = job_fields['jobs.submittime'],
            start_time = job_fields['jobs.starttime'],
            finish_time = job_fields['jobs.endtime'],

            exit_status = job_fields['jobs.exitstatus'],

            ru_maxrss = maxram,
            ru_utime = job_fields['jobs.cputime'],
        )
        return job

    def _clean_up(self):
        '''
        Clean up after the last vovsql_query line has been read.
        '''
        if not self._sql_input_file:
            # Delete the tmp file
            remove(self._sql_output_file)

def main():
    parser = argparse.ArgumentParser(description="Parse Altair Accelerator logs.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--default-mem-gb", required=False, default=AcceleratorLogParser.DEFAULT_MEMORY_GB, help="Default amount of memory (in GB) requested for jobs.")
    accelerator_mutex_group = parser.add_mutually_exclusive_group(required=True)
    accelerator_mutex_group.add_argument("--sql-output-file", help=f"File where the output of sql query will be written. Cannot be used with --sql-input-file. Required if --sql-input-file not set. \nCommand to create file:\n{AcceleratorLogParser._VOVSQL_QUERY_COMMAND} > SQL_OUTPUT_FILE")
    accelerator_mutex_group.add_argument("--sql-input-file", help="File with the output of sql query so can process it offline. Cannot be used with --sql-output-file. Required if --sql-output-file not set.")
    parser.add_argument("--output-csv", required=True, help="CSV file with parsed job completion records")
    parser.add_argument("--starttime", help="Select jobs after the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--endtime", help="Select jobs before the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        SchedulerLogParser_logger.setLevel(logging.DEBUG)

    logger.info('Started Altair Accelerator log parser')

    if args.sql_input_file:
        if not path.exists(args.sql_input_file):
            logger.error(f"sql input file doesn't exist: {args.sql_input_file}")
            exit(1)
        logger.info(f"Accelerator job data will be read from {args.sql_input_file} instead of calling nc vovsql_query.")
    else:
        try:
            result = subprocess.run(["nc", "-h"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='UTF-8') # nosec
        except FileNotFoundError as e:
            logger.error(f"Cannot find nc command. You must set up the Accelerator environment (source vovrc.sh) or specify --sql-input-file.")
            exit(1)
        except CalledProcessError as e:
            logger.error(f"'nc -h' failed. You must set up the Accelerator environment (source vovrc.sh) or specify --sql-input-file.")
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.stdout}")
            exit(1)
        output = result.stdout
        if result.returncode != 2 or 'Altair Engineering.' not in output:
            logger.error(f"Unexpected result from 'nc -h'\nreturncode: expected 2, actual {result.returncode}\noutput:\n{output}")
            if 'Usage: nc' in output:
                logger.error(f"'nc -h' called ncat (netcat), not Altair nc.")
            logger.info(f"'nc -h' failed. You must set up the Accelerator environment (source vovrc.sh) or specify --sql-input-file.")
            exit(1)
        logger.info(f"Sql job data will be written to {args.sql_output_file}")
    logger.info(f"Parsed job information will be written to {args.output_csv}")
    acceleratorLogParser = AcceleratorLogParser(default_mem_gb=float(args.default_mem_gb), sql_input_file=args.sql_input_file, sql_output_file=args.sql_output_file, output_csv=args.output_csv, starttime=args.starttime, endtime=args.endtime)
    acceleratorLogParser.parse_jobs()

if __name__ == '__main__':
    main()
