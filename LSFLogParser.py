#!/usr/bin/env python3
'''
Parse LSF logfiles out write job information to a yaml file.

Format described at: https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=files-lsbacct

bacct command documentation:
https://www.ibm.com/docs/en/spectrum-lsf/10.1.0?topic=reference-bacct

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import argparse
from copy import deepcopy
import csv
import json
import logging
from LSB_ACCT_FIELDS import LSB_ACCT_RECORD_FORMATS, MINIMAL_LSB_ACCT_FIELDS
from MemoryUtils import MEM_GB, MEM_KB, MEM_MB
from os import listdir, path
from os.path import basename, dirname, realpath
import re
from SchedulerJobInfo import SchedulerJobInfo, logger as SchedulerJobInfo_logger
from SchedulerLogParser import SchedulerLogParser, logger as SchedulerLogParser_logger
from sys import exit
from VersionCheck import logger as VersionCheck_logger, VersionCheck

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

class LSFLogParser(SchedulerLogParser):
    '''
    Parse LSF bacct.lsb* files to get job completion information.
    '''

    def __init__(self, logfile_dir: str, output_csv: str, default_max_mem_gb: float, starttime: str=None, endtime: str=None):
        '''
        Constructor

        Args:
            logfile_dir (str): Directory where LSF log files are located.
            output_dir (str):
                Directory where output will be written.
                Will be created if it doesn't already exist.
            output_csv (str): CSV file where parsed jobs will be written.
            starttime (str): Select jobs after the specified time
            endtime (str): Select jobs after the specified time
        '''
        super().__init__(None, output_csv, starttime, endtime)
        self._logfile_dir = logfile_dir
        self._default_max_mem_gb = default_max_mem_gb

        self._lsb_acct_files = self._get_lsb_acct_files(logfile_dir)
        self._lsb_acct_filename = None
        self._lsb_acct_fh = None
        self._lsb_acct_csv_reader = None

        self._invalid_record_dict = {}
        self._number_of_invalid_records = 0

    def parse_jobs(self) -> None:
        '''
        Parse all the jobs from the LSF log files.

        Returns:
            None
        '''
        job = True
        while job:
            job = self.parse_job()
            if job:
                if self._output_csv_fh:
                    self.write_job_to_csv(job)
        if self._invalid_record_dict:
            print("\n\n")
            logger.error(f"{self._number_of_invalid_records} invalid records were found in {len(self._invalid_record_dict)} files")
            for file in self._invalid_record_dict:
                logger.error(f"{file}:")
                number_of_invalid_records = self._invalid_record_dict[file]['number_of_invalid_records']
                logger.error(f"    {number_of_invalid_records} invalid records")
                logger.error(f"    Invalid records can be found in: {self._invalid_record_dict[file]['invalid_records_filename']}")

    def parse_job(self) -> SchedulerJobInfo:
        return self.parse_job_from_csv_reader()

    def parse_job_from_csv_reader(self) -> SchedulerJobInfo:
        '''
        Parse a job from the LSF log files.

        Returns:
            SchedulerJobInfo: Parsed job or None if there are no more jobs to be parsed.
        '''
        while True:
            if not self._lsb_acct_csv_reader:
                if not self._lsb_acct_files:
                    return None
                self._lsb_acct_filename = self._lsb_acct_files.pop(0)
                logger.info(f"Parsing lsb.acct file: {self._lsb_acct_filename}")
                self._lsb_acct_line_number = 0
                self._lsb_acct_fh = open(self._lsb_acct_filename, 'r', errors='replace')
                self._lsb_acct_csv_reader = csv.reader(self._lsb_acct_fh, delimiter=' ')
            try:
                record_fields = next(self._lsb_acct_csv_reader)
                logger.debug(f"record_fields: {record_fields}")
            except UnicodeDecodeError as e:
                self._lsb_acct_line_number += 1
                self._save_invalid_record(self._lsb_acct_filename, self._lsb_acct_line_number, str(e), '')
                continue
            except StopIteration:
                logger.debug(f"Reached EOF of {self._lsb_acct_filename}")
                self._lsb_acct_csv_reader = None
                self._lsb_acct_fh.close()
                continue
            self._lsb_acct_line_number += 1
            logger.debug(f"line {self._lsb_acct_line_number}: {record_fields}")
            if len(record_fields) == 0:
                logger.debug(f"Blank line")
                continue
            if re.match(r'^\s*#', record_fields[0]):
                logger.debug(f"Comment line")
                continue
            try:
                record = self._parse_record_fields(record_fields, LSB_ACCT_RECORD_FORMATS)
            except Exception as e:
                logger.error(f'{self._lsb_acct_filename}, line {self._lsb_acct_line_number}: Bad record: {e}\n{record_fields}')
                self._save_invalid_record(self._lsb_acct_filename, self._lsb_acct_line_number, str(e), '"' + '" "'.join(record_fields) + '"')
                # Keep going to try to parse all valid records
                continue
            if record['record_type'] != 'JOB_FINISH':
                logger.debug(f"{self._lsb_acct_filename}, line {self._lsb_acct_line_number}: Skipping {record['record_type']} record type")
                continue
            if record['startTime'] == 0 and record['runTime'] == 0:
                logger.debug(f"{self._lsb_acct_filename}, line {self._lsb_acct_line_number}: Ignoring job {record['jobId']} because startTime and runTime are zero.")
                continue

            # Get num_hosts
            # This must be set before calculating max_mem_gb because need to know number of hosts first.
            # Get span from effectiveResReq. Example: span[hosts=1]
            span_hosts = None
            logger.debug(f"Effective resource request: {record['effectiveResReq']}")
            match = re.search(r'span\[([^]]*)\]', record['effectiveResReq'])
            if match:
                span = match.groups(0)[0]
                logger.debug(f"span: {span}")
                match = re.search(r'hosts=(\d+)', span)
                if match:
                    num_hosts = match.groups(0)[0]
                    logger.debug(f"span_hosts: {span_hosts}")
            # If span not set then slots can be spread across different hosts, but for the number of cores doesn't change.
            # So for cost calculation just leave num_hosts == 1.
            num_hosts = 1
            logger.debug(f"num_hosts: {num_hosts}")

            # Calculate max_mem_gb.
            # If found in resource request use that. Otherwise, use the max of the actual usage or default max mem gb
            max_mem_gb = None
            match = re.search(r'rusage\[([^\]]*)\]', record['effectiveResReq'])
            if match:
                rusage = match.groups(0)[0]
                logger.debug(f"rusage: {rusage}")
                match = re.search(r'mem=([0-9\.]+)', rusage)
                if match:
                    max_mem = float(match.groups(0)[0])
                    max_mem_gb = (max_mem * MEM_KB) / MEM_GB
                    logger.debug(f"max_mem_gb: {max_mem_gb}")
                else:
                    logger.debug(f"No memory request found in rusage")
            else:
                logger.debug(f"No rusage found in resource request")
            if not max_mem_gb:
                logger.debug(f"max_mem_gb defaults to max of default_max_mem_gb({self._default_max_mem_gb}) and maxRMem({record['maxRMem']})")
                max_mem_gb = max((record['maxRMem'] * MEM_KB) / MEM_GB, self._default_max_mem_gb * num_hosts)
            logger.debug(f"max_mem_gb: {max_mem_gb}")

            # todo Get licenses from effectiveResReq
            licenses = []
            licenses = ','.join(licenses)

            job = SchedulerJobInfo(
                job_id = record['jobId'],
                num_cores = record['maxNumProcessors'],
                max_mem_gb = max_mem_gb,
                num_hosts = num_hosts,

                submit_time = record['submitTime'],
                ineligible_pend_time = record.get('ineligiblePendTime', 0),
                requeue_time = record.get('requeueTime', 0),
                start_time = record['startTime'],
                run_time = record['runTime'],
                finish_time = record['Event Time'],

                queue = record['queue'],
                project = record['projectName'],

                exit_status = record['exitStatus'],

                ru_inblock = record['ru_inblock'],
                ru_majflt = record['ru_majflt'],
                ru_maxrss = record['ru_maxrss'],
                ru_minflt = record['ru_minflt'],
                ru_msgrcv = record['ru_msgrcv'],
                ru_msgsnd = record['ru_msgsnd'],
                ru_nswap = record['ru_nswap'],
                ru_oublock = record['ru_oublock'],
                ru_stime = record['ru_stime'],
                ru_utime = record['ru_utime'],

                resource_request = record['effectiveResReq'],
            )
            if self._job_in_time_window(job):
                self._num_input_jobs += 1
                return job

    def _get_lsb_acct_files(self, logfile_dir):
        '''
        Get the list of lsb.acct* files that will be parsed

        Args:
            logfile_dir (str): Directory containining LSF log files.
        Returns:
            (str): List of filenames
        '''
        lsb_acct_files = []
        try:
            all_files = sorted(listdir(self._logfile_dir))
        except FileNotFoundError as e:
            logger.error(f"Input directory doesn't exist: {self._logfile_dir}: {e}")
            exit(1)
        for file in all_files:
            filename = path.join(logfile_dir, file)
            if path.isdir(filename):
                logger.debug(f"Skipping {filename} because it's a directory")
                continue
            if file.startswith('lsb.acct'):
                lsb_acct_files.append(filename)
            else:
                logger.debug(f"Skipping: {filename} because doesn't start with lsb.acct")
        return lsb_acct_files

    def _parse_record_fields(self, original_fields, record_format: dict) -> dict:
        '''
        Parse a line from the bacct.lsb* file and return the field values.

        Args:
            record_line (str): The line from the logfile.
            record_format (str):

        Raises:
            ValueError: If there are any errors parsing the fields.

        Returns:
            {str: str}: Dictionary with field name/value pairs.
        '''
        fields = original_fields.copy()
        try:
            record_type = fields.pop(0)
            logger.debug(f"Record type: {record_type} {len(fields)} fields")
            if record_type not in record_format:
                raise ValueError(f"Invalid record type: {record_type}")
            record = {
                'numExHosts': 0,
                'execHosts': {}
            }
            record['record_type'] = record_type

            if not(record_format[record_type] or record_format[record_type]['fields']):
                # For record types that haven't been implemented yet just capture the raw fields
                record['raw_fields'] = fields
                return record

            for field_tuple in record_format[record_type]['fields']:
                field_name = field_tuple[0]
                field_format = field_tuple[1]
                field_str = fields.pop(0).lstrip().rstrip()
                logger.debug(f"    {field_name}({field_format})={field_str}")
                if field_format == '%s':
                    field = field_str
                elif field_format == '%d':
                    if field_str == '':
                        field_str = '-1'
                    try:
                        field = int(field_str)
                    except ValueError:
                        raise ValueError(f"{field_name}({field_format})={field_str} is not an int")
                elif field_format == '%f':
                    if field_str == '':
                        field_str = '-1'
                    try:
                        field = float(field_str)
                    except ValueError:
                        raise ValueError(f"{field_name}({field_format})={field_str} is not a float")
                else:
                    raise ValueError(f"Invalid field format {field_format}")
                if record_type == 'JOB_FINISH':
                    if field_name == 'numExHosts':
                        # Sometimes numExHosts is missing.
                        # In testing it was always missing if numAskHosts != 0
                        # However in testing on production log files I would see both numAskHosts and numExHosts.
                        # The next 2 fields are jStatus(%d) and hostFactor(%f).
                        # If numExHosts != 0 then the next field should be a str, not an int, assuming all hostnames start with a character.
                        # Otherwise it is jStatus.
                        if field >= 0:
                            logger.debug(f"        Checking to see if following fields are correct:")
                            found_error = False
                            try:
                                try:
                                    for idx in range(0, field):
                                        execHost = fields[idx]
                                        logger.debug(f"            execHost[{idx}]={execHost}")
                                        if execHost == '':
                                            logger.debug(f"                execHost must be non-empty string")
                                            raise ValueError("execHost must be non-empty string")
                                        # Should not be a number
                                        try:
                                            float(execHost)
                                            logger.debug(f"                execHost must not be a number")
                                            found_error = True
                                        except ValueError:
                                                pass
                                        if found_error:
                                            raise ValueError("Invalid execHost")
                                except IndexError:
                                    logger.debug(f"            Couldn't get execHost[{idx}]. Ran out of fields.")
                                    found_error = True
                                    raise ValueError("Invalid execHost")
                                jStatus = fields[field]
                                logger.debug(f"            jStatus={jStatus}")
                                hostFactor = fields[field + 1]
                                logger.debug(f"            hostFactor={hostFactor}")
                                jobName = fields[field + 2]
                                logger.debug(f"            jobName={jobName}")
                                command = fields[field + 3]
                                logger.debug(f"            command={command}")
                                int(jStatus)
                                float(hostFactor)
                                try:
                                    float(jobName)
                                    float(command)
                                    found_error = True
                                except ValueError:
                                    pass
                            except ValueError:
                                found_error = True
                            if found_error:
                                logger.debug("        numExHosts is missing so skip field")
                                fields.insert(0, field_str)
                                continue
                record[field_name] = field
                if record_type == 'JOB_FINISH':
                    if field_name == 'Version Number':
                        version_fields = field.split('.')
                        major_version = int(version_fields[0])
                        if major_version != 10:
                            raise ValueError(f"Unsupported logfile format version {field}. Only support version 10.*. Ignoring record.")
                    elif field_name == 'numAskedHosts':
                        record['askedHosts'] = []
                        for idx in range(0, field):
                            askedHost = fields.pop(0)
                            logger.debug(f"    askedHost[{idx}]={askedHost}")
                            record['askedHosts'].append(askedHost)
                    elif field_name == 'numExHosts':
                        record['execHosts'] = {}
                        for idx in range(0, field):
                            execHost = fields.pop(0)
                            logger.debug(f"    execHost[{idx}]={execHost}")
                            record['execHosts'][execHost] = record['execHosts'].get(execHost, 0) + 1
                    elif field_name == 'Num':
                        record['submitEXT'] = {}
                        for idx in range(0, field):
                            key = fields.pop(0)
                            value = fields.pop(0)
                            logger.debug(f"        submitEXT[{idx}][{key}]={value}")
                            record['submitEXT'][key] = value
                    elif field_name == 'numHostRusage':
                        for idx in range(0, field):
                            hostname = fields.pop(0)
                            mem = fields.pop(0)
                            swap = fields.pop(0)
                            utime = fields.pop(0)
                            stime = fields.pop(0)
                            logger.debug(f"        hostRusage[{idx}][{hostname}]: mem{mem} swap={swap} utime={utime} stime={stime}")
                    elif field_name == 'num_network':
                        for idx in range(0, field):
                            networkID = fields.pop(0)
                            num_window = fields.pop(0)
                            logger.debug(f"        networkAlloc[{idx}]: networkID{networkID} num_window={num_window}")
                    elif field_name == 'numAllocSlots':
                        for idx in range(0, field):
                            allocSlot = fields.pop(0)
                            logger.debug(f"        allocSlot[{idx}]: {allocSlot}")
                    elif field_name == 'indexRangeCnt':
                        record['indexRange'] = []
                        for idx in range(0, field):
                            indexRangeStart = fields.pop(0)
                            indexRangeEnd = fields.pop(0)
                            indexRangeStep = fields.pop(0)
                            record['indexRange'].append({
                                'indexRangeStart': indexRangeStart,
                                'indexRangeEnd': indexRangeEnd,
                                'indexRangeStep': indexRangeStep
                                })
                            logger.debug(f"        indexRange[{idx}]: indexRangeStart={indexRangeStart} indexRangeEnd={indexRangeEnd} indexRangeStep={indexRangeStep}")
                    elif field_name == 'numGPURusages':
                        for idx in range(0, field):
                            hostname = fields.pop(0)
                            numKVP = int(fields.pop(0))
                            logger.debug(f"        GPURusages[{idx}]: hostname{hostname} numKVP={numKVP}")
                            for idx in range(0, numKVP):
                                key = fields.pop(0)
                                value = fields.pop(0)
                                logger.debug(f"            KVP[{idx}][{key}]: {value}")
                    elif field_name == 'storageInfoC':
                        for idx in range(0, field):
                            storageInfoV = fields.pop(0)
                            logger.debug(f"        storageInfoV[{idx}]: {storageInfoV}")
                    elif field_name == 'numKVP':
                        for idx in range(0, field):
                            key = fields.pop(0)
                            value = fields.pop(0)
                            logger.debug(f"        KVP[{idx}][{key}]: {value}")
                elif record_type == 'JOB_NEW':
                    if field_name == 'numAskedHosts':
                        record['askedHosts'] = []
                        for idx in range(0, field):
                            askedHost = fields.pop(0)
                            logger.debug(f"        askedHost[{idx}]={askedHost}")
                            record['askedHosts'].append(askedHost)
                    elif field_name == 'nxf':
                        record['xf'] = []
                        for idx in range(0, field):
                            xf = fields.pop(0)
                            logger.debug(f"        xf[{idx}]={xf}")
                            record['xf'].append(xf)
                    elif field_name == 'Num':
                        record['submitEXT'] = {}
                        for idx in range(0, field):
                            key = fields.pop(0)
                            value = fields.pop(0)
                            logger.debug(f"        submitEXT[{idx}][{key}]={value}")
                            record['submitEXT'][key] = value
                    elif field_name == 'nStinFile':
                        record['stinFiles'] = []
                        for idx in range(0, field):
                            options = fields.pop(0)
                            host = fields.pop(0)
                            name = fields.pop(0)
                            hash_str = fields.pop(0)
                            size = fields.pop(0)
                            modifyTime = fields.pop(0)
                            logger.debug(f"        stinFiles[{idx}]: options={options} host={host} name={name} hash={hash_str} size={size} modifyTime={modifyTime}")
                            stinFile = {
                                'options': options,
                                'host': host,
                                'name': name,
                                'hash': hash_str,
                                'size': size,
                                'modifyTime': modifyTime
                            }
                            record['stinFiles'].append(stinFile)
                elif record_type == 'JOB_FORWARD':
                    if field_name == 'cluster':
                        numReserHosts = record['numReserHosts']
                        record['reserHosts'] = []
                        for idx in range(0, numReserHosts):
                            reserHost = fields.pop(0)
                            logger.debug(f"        reserHosts[{idx}]={reserHost}")
                            record['reserHosts'].append(reserHost)
                elif record_type == 'JOB_START':
                    if field_name == 'numExHosts':
                        record['execHosts'] = {}
                        for idx in range(0, field):
                            execHost = fields.pop(0)
                            logger.debug(f"        execHosts[{idx}]={execHost}")
                            record['execHosts'][execHost] = record['execHosts'].get(execHost, 0) + 1
        except IndexError:
            if field_name not in ['ineligiblePendTime', 'indexRangeCnt', 'requeueTime', 'numGPURusages', 'storageInfoC', 'numKVP']:
                raise ValueError(f"Not enough fields to get value for {field_name}.")
        if fields:
            extra_fields = "'" + ','.join(fields) + "'"
            raise ValueError(f"{len(fields)} extra fields left over: {extra_fields}")
        return record

    def _save_invalid_record(self, filename: str, line_number: int, error_message, record) -> None:
        if filename not in self._invalid_record_dict:
            self._invalid_record_dict[filename] = {
                'invalid_records_filename': path.join(dirname(realpath(self._output_csv)), f"{basename(filename)}.invalid_records.txt"),
                'number_of_invalid_records': 0
            }
            self._invalid_record_dict[filename]['invalid_records_fh'] = open(self._invalid_record_dict[filename]['invalid_records_filename'], 'w')
        self._invalid_record_dict[filename]['invalid_records_fh'].write(f"# line {line_number}\n# {error_message}\n{record}\n")
        self._invalid_record_dict[filename]['number_of_invalid_records'] += 1
        self._number_of_invalid_records += 1

def main() -> None:
    '''
    Main function when the script is called.

    Uses argparse to get command line arguments.
    '''
    parser = argparse.ArgumentParser(description="Parse LSF logs.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--logfile-dir", required=True, help="LSF logfile directory")
    parser.add_argument("--output-csv", required=True, help="CSV file with parsed job completion records")
    parser.add_argument("--default-max-mem-gb", type=float, default=0.0, required=False, help="Default maximum memory for a job in GB.")
    parser.add_argument("--starttime", help="Select jobs after the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--endtime", help="Select jobs before the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--disable-version-check", action='store_const', const=True, default=False, help="Disable git version check")
    parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        SchedulerJobInfo_logger.setLevel(logging.DEBUG)
        SchedulerLogParser_logger.setLevel(logging.DEBUG)
        VersionCheck_logger.setLevel(logging.DEBUG)

    if not args.disable_version_check and not VersionCheck().check_git_version():
        exit(1)

    logger.info('Started LSF log parser')
    logger.info(f"LSF logfile directory: {args.logfile_dir}")

    lsfLogParser = LSFLogParser(args.logfile_dir, args.output_csv, args.default_max_mem_gb, starttime=args.starttime, endtime=args.endtime)
    try:
        lsfLogParser.parse_jobs()
    except Exception as e:
        logger.exception('parse_jobs failed')
        logger.info(f"{lsfLogParser._num_input_jobs} jobs parsed")
        if args.output_csv:
            logger.info(f"{lsfLogParser._num_output_jobs} jobs written to {args.output_csv}")
        logger.error(f"Failed")
        exit(1)

    logger.info(f"{lsfLogParser._num_input_jobs} jobs parsed")
    if args.output_csv:
        logger.info(f"{lsfLogParser._num_output_jobs} jobs written to {args.output_csv}")
    if lsfLogParser._invalid_record_dict:
        logger.error(f"Failed")
        exit(1)
    logger.info('Passed')
    exit(0)

if __name__ == '__main__':
    main()
