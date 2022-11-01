#!/usr/bin/env python3
'''
Script to reorder the fields in a CSV job file.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import argparse
from collections import deque
import csv
import json
import logging
from os import makedirs, path, rename, system
from os.path import basename, dirname, realpath
from sys import exit
from tempfile import NamedTemporaryFile
from typing import List
from VersionCheck import logger as VersionCheck_logger, VersionCheck

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

def main() -> None:
    '''
    Main function when the script is called.

    Uses argparse to get command line arguments.
    '''
    parser = argparse.ArgumentParser(description="Reorder jobs fields.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input-csv", required=True, help="CSV file with parsed job info.")
    parser.add_argument("--output-csv", required=True, help="CSV file with parsed job completion records")
    parser.add_argument("--disable-version-check", action='store_const', const=True, default=False, help="Disable git version check")
    parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        VersionCheck_logger.setLevel(logging.DEBUG)

    if not args.disable_version_check and not VersionCheck().check_git_version():
        exit(1)

    if not path.exists(args.input_csv):
        raise FileNotFoundError(f"Input CSV file doesn't exist: {args.input_csv}")

    output_dir = dirname(args.output_csv)
    if not path.exists(output_dir):
        makedirs(output_dir)

    # Read the CSV header to get the current fields
    input_csv_fh = open(args.input_csv, 'r', newline='')
    csv_reader = csv.reader(input_csv_fh, dialect='excel')
    original_field_names = next(csv_reader)
    logger.debug(f"original field names: {original_field_names}")
    csv_reader = None
    input_csv_fh.close()

    # Move resource_request to the end
    new_field_names = original_field_names.copy()
    new_field_names.remove('resource_request')
    new_field_names.append('resource_request')
    if original_field_names == new_field_names:
        logger.debug(f"Fields are already ordered correctly.")
        logger.debug(f"original field names: {original_field_names}")
        logger.debug(f"new      field names: {new_field_names}")
        system(f"cp {args.input_csv} {args.output_csv}") #nosec
        return
    logger.info(f"original field names: {original_field_names}")
    logger.info(f"new      field names: {new_field_names}")

    input_csv_fh = open(args.input_csv, 'r', newline='')
    csv_dict_reader = csv.DictReader(input_csv_fh, dialect='excel')
    output_csv_fh = open(args.output_csv, 'w', newline='')
    csv_dict_writer = csv.DictWriter(output_csv_fh, fieldnames=new_field_names, dialect='excel')
    csv_dict_writer.writeheader()
    num_input_jobs = 0
    for job_dict in csv_dict_reader:
        num_input_jobs += 1
        logger.debug(f"job_dict: {json.dumps(job_dict, indent=4)}")
        csv_dict_writer.writerow(job_dict)
    logger.info(f"Wrote {num_input_jobs} jobs to {args.output_csv}")

if __name__ == '__main__':
    main()
