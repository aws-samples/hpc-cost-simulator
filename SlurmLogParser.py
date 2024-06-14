#!/usr/bin/env python3
'''
Parse Slurm logfiles out write job information to a csv file.
'''

import argparse
import json
import logging
from MemoryUtils import mem_string_to_float, mem_string_to_int, MEM_GB
from os import environ, makedirs, path, remove
from os.path import dirname, realpath
import re
from SchedulerJobInfo import logger as SchedulerJobInfo_logger, SchedulerJobInfo, str_to_datetime, str_to_timedelta
from SchedulerLogParser import logger as SchedulerLogParser_logger, SchedulerLogParser
import subprocess # nosec
from subprocess import CalledProcessError, check_output # nosec
from sys import exit
from textwrap import dedent
from VersionCheck import logger as VersionCheck_logger, VersionCheck

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False

def camel2snake(x):
    # Convert Slurm camel-case field names to our snake-case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', x)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

class SlurmLogParser(SchedulerLogParser):
    '''
    Parse Slurm sacct output to get job completion information.
    '''

    def __init__(self, sacct_input_file: str=None, sacct_output_file: str=None, output_csv: str=None, starttime: str=None, endtime: str=None):
        '''
        Constructor

        Args:
            sacct_input_file (str): File with the output of sacct so can process sacct output offline.
            sacct_output_file (str): File where sacct output will be written. Can be used to process job data on another machine without sacct access.
            output_csv (str): CSV file where parsed jobs will be written.
            starttime (str): Select jobs after the specified time
            endtime (str): Select jobs after the specified time
        Raises:
            FileNotFoundError: If sacct_input_file doesn't exist.
        '''
        super().__init__(None, output_csv, starttime, endtime)

        self._sacct_input_file = sacct_input_file
        self._sacct_output_file = sacct_output_file

        if sacct_input_file and sacct_output_file:
            raise ValueError(f"Cannot specify sacct_input_file and sacct_output_file.")
        if not(sacct_input_file or sacct_output_file):
            raise ValueError(f"Must specify either sacct_input_file or sacct_output_file")
        if sacct_input_file and not path.exists(sacct_input_file):
            raise FileNotFoundError(f"sacct_input_file doesn't exist: {sacct_input_file}")
        if sacct_output_file:
            sacct_output_dir = dirname(realpath(sacct_output_file))
            if not path.exists(sacct_output_dir):
                makedirs(sacct_output_dir)

        self._sacct_input_file_has_header = False
        try:
            # Analyse first line of input file. If it looks like
            # a header line, then use to overwrite SLURM_ACCT_FIELDS
            with open(self._sacct_input_file, 'r') as F:
                l = F.readline()
                elems = l.split('|')
                known_fields = set([x[0] for x in SlurmLogParser.SLURM_ACCT_FIELDS])
                if len(set(elems) & known_fields) >= 3:
                    # Looks like a header line - has at least 3 delimited field names.
                    self._sacct_input_file_has_header = True
                    NEW_SLURM_ACCT_FIELDS = []
                    for e in elems:
                        for x in SlurmLogParser.SLURM_ACCT_FIELDS:
                            if x[0] == e:
                                NEW_SLURM_ACCT_FIELDS.append(x)
                                break
                    SlurmLogParser.SLURM_ACCT_FIELDS = NEW_SLURM_ACCT_FIELDS
        except Exception:
            pass

        self._sacct_fh = None
        self._line_number = 0
        self._eof = False
        self._parsed_lines = []

    SLURM_ACCT_FIELDS = [
        ('State', 's'),            # Displays the job status, or state. Put this first so can skip ignored states.
        ('JobID', 's'),            # The identification number of the job or job step
        # Job properties
        ('ReqCPUS', 'd'),          # Number of requested CPUs
        ('ReqMem', 'sd', True),    # Minimum required memory for the job in bytes
        ('ReqNodes', 'd'),         # Requested minimum Node count for the job/step
        ('Reason', 's'),           #
        ('Constraints', 's'),      #
        # Times
        ('Submit', 'dt'),          # The time the job was submitted. NOTE: If a job is requeued, the submit time is reset. This is handled by not overwriting fields with the batch step.
        ('Eligible', 'dt'),        # When the job became eligible to run
        ('Start', 'dt'),           # Initiation time of the job
        ('Elapsed', 'td'),         # The job's elapsed time: [DD-[HH:]]MM:SS
        ('Suspended', 'td'),       # The amount of time a job or job step was suspended
        ('End', 'dt'),             # Termination time of the job
        ('Timelimit', 'td', True), # Time limit of job

        ('ExitCode', 's'),         # The exit code returned by the job script or salloc
        ('DerivedExitCode', 's'),  # The highest exit code returned by the job's job steps

        ('AllocNodes', 'd'),       # Number of nodes allocated to the job/step
        ('NCPUS', 'd'),            # Total number of CPUs allocated to the job. Equivalent to AllocCPUS

        ('MaxDiskRead', 'sd', True),  # Maximum number of bytes read by all tasks in job')
        ('MaxDiskWrite', 'sd', True), # Maximum number of bytes written by all tasks in job
        ('MaxPages', 'd', True),      # Maximum number of page faults of all tasks in job
        ('MaxRSS', 'd', True),        # Maximum resident set size of all tasks in job
        ('MaxVMSize', 'd', True),     # Maximum Virtual Memory size of all tasks in job
        ('CPUTime', 'td', True),      # Time used (Elapsed time * CPU count) by a job or step in HH:MM:SS format
        ('UserCPU', 'td', True),      # The amount of user CPU time used by the job or job step. Format is the same as Elapsed
        ('SystemCPU', 'td', True),    # The amount of system CPU time used by the job or job step. Format is the same as Elapsed
        ('TotalCPU', 'td', True),     # The sum of the SystemCPU and UserCPU time used by the job or job step

        # This field was added, so it should be optional
        ('Partition', 's'),       # The exit code returned by the job script or salloc

        ('User', 's'),            # User that submitted job
    ]

    SLURM_STATE_CODES = [
        'BOOT_FAIL', # Job terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot the node or block and the job can not be requeued).
        'CANCELLED',     # Job was explicitly cancelled by the user or system administrator. The job may or may not have been initiated.
        'COMPLETED',     # Job has terminated all processes on all nodes with an exit code of zero.
        'DEADLINE',      # Job terminated on deadline.
        'FAILED',        # Job terminated with non-zero exit code or other failure condition.
        'NODE_FAIL',     # Job terminated due to failure of one or more allocated nodes.
        'OUT_OF_MEMORY', # Job experienced out of memory error.
        'PENDING',       # Job is awaiting resource allocation.
        'PREEMPTED',     # Job terminated due to preemption.
        'RUNNING',       # Job currently has an allocation.
        'REQUEUED',      # Job was requeued.
        'RESIZING',      # Job is about to change size.
        'REVOKED',       # Sibling was removed from cluster due to other cluster starting the job.
        'SUSPENDED',     # Job has an allocation, but execution has been suspended and CPUs have been released for other jobs.
        'TIMEOUT',       # Job terminated upon reaching its time limit
    ]
    SLURM_STATE_CODES_TO_IGNORE = [
        'BOOT_FAIL',
        'NODE_FAIL',
        'PENDING',
        'PREEMPTED',
        'RUNNING',
        'REQUEUED',
        'REVOKED',
    ]

    def parse_jobs(self) -> bool:
        '''
        Parse all the jobs from the Slurm sacct command.

        Returns:
            bool: Return True if no errors
        '''
        self._parsed_lines = []
        self.errors = []

        if self._sacct_input_file_has_header:
            # Discard first line of input file.
            if not self._sacct_fh:
                self._sacct_fh = open(self._sacct_input_file, 'r')
                self._eof = False
            line = self._sacct_fh.readline()

        job = True
        while job:
            job = self.parse_job()
            if job:
                if self._output_csv_fh:
                    self.write_job_to_csv(job)
        logger.info(f"Parsed {self.num_output_jobs()} jobs")
        if self.errors:
            logger.error(f"{len(self.errors)} errors while parsing jobs")
            return False
        return True

    def parse_jobs_to_dict(self) -> dict:
        '''
        Parse all the jobs from the Slurm sacct command.

        Returns:
            bool: Return True if no errors
        '''
        self._parsed_lines = []
        self.errors = []

        if self._sacct_input_file_has_header:
            # Discard first line of input file.
            if not self._sacct_fh:
                self._sacct_fh = open(self._sacct_input_file, 'r')
                self._eof = False
            line = self._sacct_fh.readline()

        # Use JobID as index
        jobs_dict = {}

        job = True
        while job:
            job = self.parse_job()
            if job:
                jd = job.to_dict()
                idx = jd['job_id'] ; del jd['job_id']
                jobs_dict[idx] = jd
        logger.info(f"Parsed {len(jobs_dict)} jobs")
        if self.errors:
            logger.error(f"{len(self.errors)} errors while parsing jobs")
            return None
        return jobs_dict

    def parse_job(self):
        '''
        Parse a job from the Slurm sacct output.

        sacct writes multiple lines for each job so must parse lines until have read all of the lines for a job.

        Returns:
            SchedulerJobInfo or None: Parsed job info or None if no more jobs found.
        '''
        if self._eof:
            job = self._process_parsed_lines()
            if job:
                return job
        if not self._sacct_fh:
            if not self._sacct_input_file:
                self._call_sacct()
            self._sacct_fh = open(self._sacct_input_file, 'r')
            self._eof = False
        while not self._eof:
            line = self._sacct_fh.readline()
            self._line_number += 1
            if line == '':
                logger.debug(f"Hit EOF at line {self._line_number}")
                self._eof = True
                self._clean_up()
            else:
                parsed_line = self._parse_line(line)
                if not parsed_line:
                    continue
                self._parsed_lines.append(parsed_line)
            job = self._process_parsed_lines()
            if job:
                return job
        return None

    @staticmethod
    def get_sacct_command_args(starttime: str, endtime: str):
        format_fields = []
        for field_tuple in SlurmLogParser.SLURM_ACCT_FIELDS:
            format_fields.append(field_tuple[0])
        args = ["sacct", '--allusers', '--parsable2', '--noheader', '--format', ','.join(format_fields)]
        if not starttime:
            starttime = '1970-01-01T0:00:00'
        args.extend(['--starttime', starttime])
        if endtime:
            args.extend(['--endtime', endtime])
        return args

    @staticmethod
    def get_sacct_command(starttime: str, endtime: str):
        command = ' '.join(SlurmLogParser.get_sacct_command_args(starttime, endtime))
        return command

    def _call_sacct(self):
        '''
        Call sacct to get job information.

        Saves the output to a file and sets to the file.
        '''
        if self._sacct_input_file:
            raise RuntimeError("Cannot call _call_sacct when sacct_input_file given for input")

        # Create a file handle to write the sacct output to.
        sacct_write_fh = open(self._sacct_output_file, 'w')

        logger.debug(f"Calling sacct")
        args = self.get_sacct_command_args(self._starttime, self._endtime)
        rc = subprocess.call(args, stdout=sacct_write_fh, stderr=subprocess.STDOUT, encoding='UTF-8') # nosec
        sacct_write_fh.close()
        if rc:
            logger.error(f"sacct failed with rc={rc}. See {self._sacct_output_file}")
            exit(1)
        self._sacct_input_file = self._sacct_output_file

    def _parse_line(self, line):
        '''
        Parse line from sacct output

        Args:
            line (str): Untouched line
        Returns:
            (int, dict, int, int) or None: Tuple with job_id, fields, line_number, and number of errors or None if comment or blank line.
        '''
        line = line.lstrip().rstrip()
        logger.debug(f"line {self._line_number}: '{line}'")
        if re.match(r'^\s*$', line):
            logger.debug("    Skipping blank line")
            return None
        if re.match(r'^\s*#', line):
            logger.debug("    Skipping comment line")
            return None
        fields = line.split('|')
        logger.debug(f"    {len(fields)} fields: {fields}")
        job_fields = {}
        try:
            field_errors = 0
            for field_tuple in self.SLURM_ACCT_FIELDS:
                field_name = field_tuple[0]
                field_format = field_tuple[1]
                if len(field_tuple) == 3:
                    empty_field_allowed = field_tuple[2]
                else:
                    empty_field_allowed = False
                try:
                    field_value = fields.pop(0)
                except IndexError:
                    if field_name in ['Partition']:
                        field_value = None
                    else:
                        raise
                req_mem_suffix = None
                if field_value != None:
                    try:
                        if field_format == 'd':
                            field_value = mem_string_to_int(field_value)
                        elif field_format == 'sd':
                            (field_value, req_mem_suffix) = self._slurm_mem_string_to_int(field_value)
                        elif field_format == 'f':
                            field_value = mem_string_to_float(field_value)
                        elif field_format == 'dt':
                            # Check value by trying to convert to datetime
                            str_to_datetime(field_value)
                        elif field_format == 'td':
                            # Check value by trying to convert to timedelta
                            str_to_timedelta(field_value)
                        elif field_format == 's':
                            pass
                        else:
                            raise ValueError(f"Invalid format of {field_format} for field {field_name}")
                    except ValueError as e:
                        if field_name == 'Start' and job_fields['State'] == 'CANCELLED':
                            # Ignore cancelled jobs that didn't start
                            logger.debug(f"Ignoring job because it was CANCELLED and did not start.")
                            return None
                        if empty_field_allowed and field_value == '':
                            field_value = None
                        else:
                            field_errors += 1
                            msg = f"Unable to convert field {field_name} to format {field_format}: {field_value}: {e}\n{line}"
                            logger.error(f"{self._sacct_input_file}, line {self._line_number}: {msg}")
                            self.errors.append((self._sacct_input_file, self._line_number, msg))
                logger.debug(f"    {field_name}: '{field_value}' {type(field_value)}")
                job_fields[field_name] = field_value
                if field_name == 'State':
                    if job_fields['State'] not in self.SLURM_STATE_CODES:
                        # Handle case where state is 'CANCELLED by uid'
                        if re.match(r'CANCELLED', job_fields['State']):
                            job_fields['State'] = 'CANCELLED'
                        else:
                            raise ValueError(f"Invalid state: {job_fields['State']}")
                    # Need to stop processing lines with invalid states since following fields may be invalid and cause errors.
                    if job_fields['State'] in self.SLURM_STATE_CODES_TO_IGNORE:
                        logger.debug(f"    Ignored state: {field_value}")
                        return None
        except Exception as e:
            field_errors += 1
            msg = f"Exception while processing fields, {field_name} ({field_format}): {e}\n{line}"
            logger.error(f"{self._sacct_input_file}, line {self._line_number}: {msg}")
            self.errors.append((self._sacct_input_file, self._line_number, msg))
        logger.debug(f"    job_fields: {job_fields}")

        if 'JobID' not in job_fields:
            return None
        job_fields['JobID'] = job_fields['JobID'].replace('.batch', '')
        job_fields['JobID'] = job_fields['JobID'].replace('.extern', '')
        job_fields['JobID'] = job_fields['JobID'].replace('.interactive', '')
        match = re.match(r'(\d+)\.(.+)$', job_fields['JobID'])
        if match:
            job_fields['JobID'] = match.group(1)
            suffix = match.group(2)
            if not re.match(r'\d+$', suffix):
                logger.warning(f"Unknown job step suffix for job {job_fields['JobID']}: {suffix}")
            job_fields['JobID'] = job_fields['JobID'].replace(f'.{suffix}', '')
        job_id = job_fields['JobID']
        logger.debug(f"    job_id: {job_id}")

        # NCPUS and ReqMem are for the entire job. For a multi-node job this means the per node value must be divided by the number of nodes (AllocNodes).
        if job_fields['AllocNodes'] == 0:
            job_fields['AllocNodes'] = 1
        if job_fields['ReqMem'] and req_mem_suffix:
            if req_mem_suffix == 'n':
                job_fields['ReqMem'] *= job_fields['AllocNodes']
            if req_mem_suffix == 'c' and (job_fields['NCPUS'] > 1):
                job_fields['ReqMem'] *= job_fields['NCPUS']

        # TODO: Add an option to add a ReqMem if one not specified. For now leave it blank or 0.
        # if job_fields['ReqMem'] == 0 and job_fields['MaxRSS']:
        #     logger.debug(f"No memory request for job {job_fields['JobID']} so using MaxRSS")
        #     job_fields['ReqMem'] = round(job_fields['MaxRSS'] * job_fields['AllocNodes'] * 1.10)

        return (job_id, job_fields, self._line_number, field_errors)

    def _process_parsed_lines(self):
        '''
        Process parsed lines to assemble a job.

        There must be at least 2 lines to assemble a job.
        Returns:
            SchedulerJobInfo or None: Parsed job info or None if there is an error in the job or no job can be assembled.
        '''
        if not self._parsed_lines:
            return None

        first_job_id = self._parsed_lines[0][0]
        last_job_id = self._parsed_lines[-1][0]
        if not self._eof and first_job_id == last_job_id:
            return None

        (job_id, job_fields, first_line_number, field_errors) = self._parsed_lines.pop(0)
        last_line_number = first_line_number
        logger.debug(f"Assembling job {job_id}")
        while self._parsed_lines and self._parsed_lines[0][0] == job_id:
            # Update fields. Don't overwrite with .update or else blank fields will overwrite non-blank fields.
            (job_id, next_job_fields, last_line_number, next_field_errors) = self._parsed_lines.pop(0)
            field_errors += next_field_errors
            logger.debug(f"    Updating job_fields")
            for field_tuple in self.SLURM_ACCT_FIELDS:
                field_name = field_tuple[0]
                if next_job_fields.get(field_name, None):   
                    if not job_fields.get(field_name, None) or \
                        field_name in ['State']:
                        job_fields[field_name] = next_job_fields[field_name]
        logger.debug(f"    Merged job fields:\n{json.dumps(job_fields, indent=4)}")

        if field_errors:
            logger.debug(f"Ignoring job {job_id} because it had {field_errors} field errors.")
            return None
        # Verify that the job has all fields
        missing_fields = []
        for field_tuple in self.SLURM_ACCT_FIELDS:
            field_name = field_tuple[0]
            if field_name not in job_fields:
                missing_fields.append(field_name)
        if missing_fields:
            msg = f"Missing fields: {missing_fields}"
            logger.error(f"{self._sacct_input_file}, lines {first_line_number}-{last_line_number}: {msg}")
            self.errors.append((self._sacct_input_file, first_line_number, msg))
            return None

        if job_fields['State'] in self.SLURM_STATE_CODES_TO_IGNORE:
            logger.debug(f"    Ignored state: {field_value}")
            return None

        try:
            job = self._create_job_from_job_fields(job_fields)
        except Exception as e:
            msg = f"Exception creating job from fields: {e}\njob_fields: {json.dumps(job_fields, indent=4)}"
            logger.error(f"{self._sacct_input_file}, lines {first_line_number}-{last_line_number}: {msg}")
            self.errors.append((self._sacct_input_file, first_line_number, msg))
            return None

        # Check if it is in the starttime - endtime window.
        # This is for the case where we are parsing the output from a previous call to sacct.
        if not self._job_in_time_window(job):
            logger.debug("    Skipping because not in time window")
            self.total_jobs_outside_time_window += 1
            return None

        return job

    def _create_job_from_job_fields(self, job_fields):
        '''
        Returns:
            SchedulerJobInfo: Parsed job info
        '''
        logger.debug(f"_create_job_from_job_fields({job_fields})")

        job_fields = dict(job_fields)  # copy

        if job_fields.get('AllocNodes') == 0:
            job_fields['AllocNodes'] = 1
        if job_fields.get('ReqMem') is not None:
            job_fields['ReqMemGB'] = job_fields['ReqMem'] / MEM_GB
        else:
            job_fields['ReqMemGB'] = job_fields.get('ReqMem')
        del job_fields['ReqMem']

        if job_fields.get('ExitCode') is not None:
            job_fields['ExitStatus'] = exit_status
            del job_fields['ExitCode']
        else:
            exit_status = None

        job_fields = {camel2snake(k):job_fields[k] for k in job_fields}
        sacct_renames={ 'ncpus': 'num_cores',
                        'alloc_nodes': 'num_hosts',
                        'submit': 'submit_time',
                        'eligible': 'eligible_time',
                        'start': 'start_time',
                        'elapsed': 'run_time',
                        'end': 'finish_time',
                        'partition': 'queue',
                        'req_mem_gb': 'max_mem_gb',
                        'max_disk_read': 'ru_inblock',
                        'max_disk_write': 'ru_oublock',
                        'max_pages': 'ru_majflt',
                        'max_rss': 'ru_minflt',
                        'system_cpu': 'ru_stime',
                        'user_cpu': 'ru_utime',
                        'total_cpu': 'ru_ttime',
                        'constraints': 'resource_requests'
        }
        for k,v in list(job_fields.items()):
            if k in sacct_renames:
                job_fields[sacct_renames[k]] = v
                del job_fields[k]
        job = SchedulerJobInfo(**job_fields)

        return job

    def _clean_up(self):
        '''
        Clean up after the last sacct line has been read.
        '''
        if not self._sacct_input_file:
            # Delete the tmp file
            remove(self._sacct_output_file)

    @staticmethod
    def _slurm_mem_string_to_int(string_value: str) -> float:
        '''
        Slurm can add a 'c' or 'n' at the end of the ReqMem field to indicate if the memory request is per node or per core.
        For per core requests then the request must be multiplied by the number of cores.

        Args:
            string_value (str): String value
        Returns:
            (float, str): Tuple with the memory value as bytes and a 'c' or 'n' to indicate if the value is per node or per core.
        '''
        if not string_value:
            raise ValueError("Empty string cannot be converted to int")
        if string_value[-1] in ['c', 'n']:
            suffix = string_value[-1]
            string_value = string_value[0:-1]
        else:
            suffix = ''
        value = mem_string_to_int(string_value)
        return (value, suffix)

def main():
    parser = argparse.ArgumentParser(description="Parse Slurm logs.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    slurm_mutex_group = parser.add_mutually_exclusive_group(required=True)
    slurm_mutex_group.add_argument("--show-data-collection-cmd", action='store_const', const=True, default=False, help="Show the command to create SACCT_OUTPUT_FILE.")
    slurm_mutex_group.add_argument("--sacct-output-file", help="File where the output of sacct will be written. Cannot be used with --sacct-input-file. Required if --sacct-input-file not set.")
    slurm_mutex_group.add_argument("--sacct-input-file", help="File with the output of sacct so can process sacct output offline. Cannot be used with --sacct-output-file. Required if --sacct-output-file not set.")
    parser.add_argument("--output-csv", required=False, help="CSV file with parsed job completion records")
    parser.add_argument("--slurm-root", required=False, help="Directory that contains the Slurm bin directory.")
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


    if args.show_data_collection_cmd:
        print(dedent(f"""\
            Run the following command to save the job information to a file:

                {SlurmLogParser.get_sacct_command(args.starttime, args.endtime)} > OUTPUT_FILE

            Then you can parse OUTPUT_FILE using the following command:

                {__file__} --sacct-input-file OUTPUT_FILE --output-csv OUTPUT_CSV
        """))
        exit(0)

    if not (args.sacct_output_file or args.sacct_input_file):
        logger.error("one of the arguments --sacct-output-file --sacct-input-file is required")
        exit(1)

    if not args.output_csv:
        logger.error("the following arguments are required: --output-csv")
        exit(1)

    logger.info('Started Slurm log parser')

    if args.slurm_root:
        if not path.exists(args.slurm_root):
            logger.error(f"Slurm root dir {args.slurm_root} doesn't exist.")
            exit(1)
        logger.info(f"Adding {args.slurm_root}/bin to PATH")
        environ['PATH'] = f"{args.slurm_root}/bin:{environ['PATH']}"

    if args.sacct_input_file:
        if not path.exists(args.sacct_input_file):
            logger.error(f"Sacct file doesn't exist: {args.sacct_input_file}")
            exit(1)
        logger.info(f"Slurm job data will be read from {args.sacct_input_file} instead of calling sacct.")
    else:
        try:
            check_output(["squeue"], stderr=subprocess.STDOUT, encoding='UTF-8') # nosec
        except (CalledProcessError, FileNotFoundError) as e:
            if args.slurm_root:
                logger.error(f"The specified --slurm-root {args.slurm_root} is incorrect.")
                exit(1)
            else:
                logger.error(f"You must specify --slurm-root or configure your environment so slurm is in the path or provide --sacct-file.")
            exit(1)
        logger.info(f"Slurm job data will be written to {args.sacct_output_file}")
    slurmLogParser = SlurmLogParser(args.sacct_input_file, args.sacct_output_file, args.output_csv, args.starttime, args.endtime)
    if not slurmLogParser.parse_jobs():
        exit(2)

if __name__ == '__main__':
    main()
