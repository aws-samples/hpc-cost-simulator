#!/usr/bin/env python3
'''
Scheduler agnostic parser that reads a jobs CSV file that was output by parsing the
scheduler's log files.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import argparse
import logging
from os import path
from os.path import realpath
from SchedulerJobInfo import SchedulerJobInfo
from SchedulerLogParser import SchedulerLogParser, logger as SchedulerLogParser_logger

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

class CSVLogParser(SchedulerLogParser):
    '''
    Scheduler agnostic parser that reads a jobs CSV file that was output by parsing the
    scheduler's log files.

    The parser can output a CSV that is not specific to any scheduler and it
    can parse that CSV as input for logs that have already been parsed.
    '''
    def __init__(self, input_csv: str, output_csv: str, starttime: str=None, endtime: str=None):
        '''
        Constructor

        Args:
            input_csv (str): Filename of input CSV file. Required and must exist.
            output_csv (str): Filename of output CSV file. Can be None or ''.
                If the directory of output_csv does not exist then it will be created.
            starttime (str): Select jobs after the specified time
            endtime (str): Select jobs after the specified time
        Raises:
            FileNotFoundError: If input_csv does not exist.
        Returns:
            None
        '''
        if not input_csv:
            raise ValueError(f"Constructor called without input_csv.")
        super().__init__(input_csv, output_csv, starttime, endtime)

    def parse_job(self) -> SchedulerJobInfo:
        '''
        Parse a job from the CSV file.

        Returns:
            SchedulerJobInfo: Parsed job or None if there are no more jobs to be parsed.
        '''
        while True:
            try:
                job = self._read_job_from_csv()
            except ValueError:
                continue
            if not job:
                return job
            if self._job_in_time_window(job):
                return job

    def parse_jobs(self) -> None:
        '''
        Parse all the jobs from the CSV file.

        Returns:
            None
        '''
        job = True
        while job:
            job = self.parse_job()
            if job:
                if self._output_csv_fh:
                    self.write_job_to_csv(job)

def main() -> None:
    '''
    Main function when the script is called.

    Uses argparse to get command line arguments.
    '''
    parser = argparse.ArgumentParser(description="Parse CSV file with results of parsed logs.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input-csv", required=True, help="CSV file with parsed job info.")
    parser.add_argument("--output-csv", required=False, help="CSV file where parsed jobs will be written.")
    parser.add_argument("--starttime", help="Select jobs after the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--endtime", help="Select jobs before the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        SchedulerLogParser_logger.setLevel(logging.DEBUG)

    logger.info('Started CSV log parser')
    if not path.exists(args.input_csv):
        logger.error(f"Input CSV file doesn't exist: {args.input_csv}")
        exit(1)
    logger.info(f"Reading CSV input from {args.input_csv}")
    if args.output_csv:
        logger.info(f"Writing parsed job output to {args.output_csv}")
        if realpath(args.input_csv) == realpath(args.output_csv):
            logger.error(f"Input and output CSV cannot be the same.")
            exit(1)

    csvLogParser = CSVLogParser(args.input_csv, args.output_csv, starttime=args.starttime, endtime=args.endtime)
    try:
        csvLogParser.parse_jobs()
    except Exception as e:
        logger.exception(f"Unhandled exception in {__file__}")
        logger.info(f"{csvLogParser._num_input_jobs} jobs parsed")
        if args.output_csv:
            logger.info(f"{csvLogParser._num_output_jobs} jobs written to {args.output_csv}")
        logger.error(f"Failed")
        exit(1)
    logger.info(f"{csvLogParser._num_input_jobs} jobs parsed")
    if args.output_csv:
        logger.info(f"{csvLogParser._num_output_jobs} jobs written to {args.output_csv}")
    logger.info('Passed')
    exit(0)

if __name__ == '__main__':
    main()
