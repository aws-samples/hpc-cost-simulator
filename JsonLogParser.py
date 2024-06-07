#!/usr/bin/env python3
'''
Scheduler agnostic parser that reads a jobs Json file and a schema map that maps the json items
to SchedulerJobInfo fields.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import argparse
import ijson
import json
import logging
from math import ceil
from os import path
from os.path import realpath
from SchedulerJobInfo import datetime_to_str, SchedulerJobInfo
from SchedulerLogParser import SchedulerLogParser, logger as SchedulerLogParser_logger
from VersionCheck import logger as VersionCheck_logger, VersionCheck

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

class JsonLogParser(SchedulerLogParser):
    '''
    Scheduler agnostic parser that reads a jobs Json file and a schema map that maps the json items
    to SchedulerJobInfo fields.

    The parser can output a CSV that is not specific to any scheduler.
    '''
    def __init__(self, input_json: str, json_schema_map: str, output_csv: str, starttime: str=None, endtime: str=None):
        '''
        Constructor

        Args:
            input_json (str): Filename of input json file. Required and must exist.
            json_schema_map (str): Filename of json file that maps json field names to SchedulerJobInfo fields. Required and must exist.
            output_csv (str): Filename of output CSV file. Can be None or ''.
                If the directory of output_csv does not exist then it will be created.
            starttime (str): Select jobs after the specified time
            endtime (str): Select jobs after the specified time
        Raises:
            FileNotFoundError: If input_json or json_schema_map do not exist.
        Returns:
            None
        '''
        if not input_json:
            raise ValueError(f"Constructor called without input_json.")
        logger.info(f"input_json: {input_json}, json_schema_map: {json_schema_map}, output_csv: {output_csv}")
        self._input_json = input_json
        self._json_schema_map_filename = json_schema_map

        num_errors = 0
        if not path.exists(input_json):
            logger.error(f"Input JSON file doesn't exist: {input_json}")
            num_errors += 1

        if not path.exists(json_schema_map):
            logger.error(f"JSON schema map file doesn't exist: {json_schema_map}")
            num_errors += 1

        if output_csv:
            if realpath(input_json) == realpath(output_csv):
                logger.error(f"Input JSON and output CSV cannot be the same.")
                num_errors += 1
            if realpath(json_schema_map) == realpath(output_csv):
                logger.error(f"Input JSON schema map and output CSV cannot be the same.")
                num_errors += 1

        if num_errors:
            exit(1)

        super().__init__('', output_csv, starttime, endtime)

        with open(self._json_schema_map_filename, 'rb') as fh:
            self._json_schema_map = json.loads(fh.read())

        self._json_fh = open(self._input_json, 'rb')

    def parse_job(self) -> SchedulerJobInfo:
        '''
        Parse a job from the JSON file.

        Returns:
            SchedulerJobInfo: Parsed job or None if there are no more jobs to be parsed.
        '''
        while True:
            try:
                job = self._read_job_from_json()
            except ValueError:
                continue
            if not job:
                return job
            if self._job_in_time_window(job):
                return job
            else:
                self.total_jobs_outside_time_window += 1

    def parse_jobs(self) -> None:
        '''
        Parse all the jobs from the JSON file.

        Returns:
            None
        '''
        logger.debug("Parsing jobs")
        job_dict_iterator = ijson.items(self._json_fh, 'item')
        logger.debug(f"job_dict_iterator: {job_dict_iterator}")
        for job_dict in job_dict_iterator:
            self._num_input_jobs += 1
            logger.debug(f"job_dict:\n{json.dumps(job_dict, indent=4)}")
            try:
                job = self._create_job_from_dict(job_dict)
            except ValueError as e:
                logger.error(f"Couldn't parse job_dict:\n{json.dumps(job_dict, indent=4)}\n{e}")
                continue
            if not job:
                return job
            logger.debug(f"job: {job}")
            if not self._job_in_time_window(job):
                self.total_jobs_outside_time_window += 1
                return None
            if self._output_csv_fh:
                self.write_job_to_csv(job)

    KB = 1024
    MB = KB * KB
    GB = MB * KB

    SRC_DST_CONVERSION_FACTORS = {
        'b': {
            'b':  1,
            'kb': 1 / KB,
            'mb': 1 / MB,
            'gb': 1 / GB
        },
        'kb': {
            'b':  KB,
            'kb': 1,
            'mb': 1 / KB,
            'gb': 1 / MB
        },
        'mb': {
            'b':  MB,
            'kb': KB,
            'mb': 1,
            'gb': 1 / KB
        },
        'gb': {
            'b':  GB,
            'kb': MB,
            'mb': KB,
            'gb': 1
        }
    }

    def _create_job_from_dict(self, job_dict) -> SchedulerJobInfo:
        '''
        Create a job from the JSON dict.

        Returns:
            SchedulerJobInfo: Parsed job or None if there are no more jobs to be parsed.
        '''
        kwargs = {}
        for dst_field, src_field in self._json_schema_map.items():
            logger.debug(f"src_field: {type(src_field)}")
            if type(src_field) == dict:
                src_field_dict = src_field
                src_field = list(src_field_dict.keys())[0]
                if dst_field in ['max_mem_gb']:
                    dst_units = 'gb'
                else:
                    dst_units = 'b'
                src_units = src_field_dict[src_field]['units']
                conversion_factor = float(JsonLogParser.SRC_DST_CONVERSION_FACTORS[src_units][dst_units])
                if src_field not in job_dict:
                    continue
                src_value = float(job_dict[src_field])
                dst_value = ceil(src_value * conversion_factor)
            else:
                if src_field not in job_dict:
                    continue
                dst_value = job_dict[src_field]
            kwargs[dst_field] = dst_value
        if 'finish_time' not in kwargs:
            if 'run_time' not in kwargs:
                logger.error(f"Must have either finish_time or run_time mapped in the json-schema-map.")
                exit(1)
            (start_time, start_time_dt) = SchedulerJobInfo.fix_datetime(kwargs['start_time'])
            (run_time, run_time_td) = SchedulerJobInfo.fix_duration(kwargs['run_time'])
            finish_time_dt = start_time_dt + run_time_td
            kwargs['finish_time'] = datetime_to_str(finish_time_dt)
        job = SchedulerJobInfo(**kwargs)
        return job

def main() -> None:
    '''
    Main function when the script is called.

    Uses argparse to get command line arguments.
    '''
    parser = argparse.ArgumentParser(description="Parse JSON file with job results.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--input-json",      required=True, help="Json file with parsed job info.")
    parser.add_argument("--json-schema-map", required=True, help="Json file that maps input json field names to SchedulerJobInfo field names.")
    parser.add_argument("--output-csv",      required=False, help="CSV file where parsed jobs will be written.")
    parser.add_argument("--starttime", help="Select jobs after the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--endtime", help="Select jobs before the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--disable-version-check", action='store_const', const=True, default=False, help="Disable git version check")
    parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        SchedulerLogParser_logger.setLevel(logging.DEBUG)
        VersionCheck_logger.setLevel(logging.DEBUG)

    if not args.disable_version_check and not VersionCheck().check_git_version():
        exit(1)

    logger.info('Started JSON log parser')
    jsonLogParser = JsonLogParser(args.input_json, args.json_schema_map, args.output_csv, starttime=args.starttime, endtime=args.endtime)
    logger.info(f"Reading JSON input from {args.input_json}")
    logger.info(f"Reading JSON schema map from {args.json_schema_map}")
    if args.output_csv:
        logger.info(f"Writing parsed job output to {args.output_csv}")

    try:
        jsonLogParser.parse_jobs()
    except Exception as e:
        logger.exception(f"Unhandled exception in {__file__}")
        logger.info(f"{jsonLogParser._num_input_jobs} jobs parsed")
        if args.output_csv:
            logger.info(f"{jsonLogParser._num_output_jobs} jobs written to {args.output_csv}")
        logger.error(f"Failed")
        exit(1)
    logger.info(f"{jsonLogParser._num_input_jobs} jobs parsed")
    if args.output_csv:
        logger.info(f"{jsonLogParser._num_output_jobs} jobs written to {args.output_csv}")
    logger.info('Passed')
    exit(0)

if __name__ == '__main__':
    main()
