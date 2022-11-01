#!/usr/bin/env python3
'''
Script to sort a CSV job file by eligible time.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import argparse
import csv
import logging
from os import makedirs, path, remove, system
from os.path import basename, dirname, realpath
from sys import exit
from tempfile import NamedTemporaryFile
from VersionCheck import logger as VersionCheck_logger, VersionCheck

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
        if not path.exists(input_csv):
            raise FileNotFoundError(f"Input CSV file doesn't exist: {input_csv}")
        self._input_csv = realpath(input_csv)

        self._output_csv = realpath(output_csv)
        self._output_dir = dirname(self._output_csv)
        if not path.exists(self._output_dir):
            makedirs(self._output_dir)

    def sort_jobs(self) -> None:
        # Make sure that the job fields are in the correct order so can use sort.
        # Specifically, the resource_request needs to be after eligible_time because it can contain ','
        reorder_fh = NamedTemporaryFile(mode='w', prefix=f"{basename(self._output_csv)}-", dir=self._output_dir, delete=False)
        reorder_filename = reorder_fh.name
        reorder_fh.close()
        system(f"{dirname(__file__)}/ReorderJobsFields.py --disable-version-check --input-csv {self._input_csv} --output-csv {reorder_filename}") #nosec

        # Read the CSV header to find out which field is the eligible_time
        reorder_fh = open(reorder_filename, 'r', newline='')
        csv_reader = csv.reader(reorder_fh, dialect='excel')
        field_names = next(csv_reader)
        key_index = field_names.index('eligible_time') + 1

        system(f"head -n 1 {reorder_filename} > {self._output_csv}") #nosec
        system(f"tail -n +2 {reorder_filename} | sort -k {key_index} -t , >> {self._output_csv}") # nosec
        remove(reorder_filename)
        logger.info(f"Wrote jobs sorted by eligible_time to {self._output_csv}")

def main() -> None:
    '''
    Main function when the script is called.

    Uses argparse to get command line arguments.
    '''
    parser = argparse.ArgumentParser(description="Sort jobs by eligible_time.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input-csv", required=True, help="CSV file with parsed job info.")
    parser.add_argument("--disable-version-check", action='store_const', const=True, default=False, help="Disable git version check")
    parser.add_argument("--output-csv", required=True, help="CSV file with parsed job completion records")
    parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        VersionCheck_logger.setLevel(logging.DEBUG)

    if not args.disable_version_check and not VersionCheck().check_git_version():
        exit(1)

    logger.info('Started Job sorter')
    logger.info(f"Reading CSV input from {args.input_csv}")
    logger.info(f"Writing sorted jobs to {args.output_csv}")
    job_sorter = JobSorter(args.input_csv, args.output_csv)
    job_sorter.sort_jobs()

if __name__ == '__main__':
    main()
