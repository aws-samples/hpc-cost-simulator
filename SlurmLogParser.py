#!/usr/bin/env python3
'''
Parse Slurm logfiles out write job information to a csv file.
'''

import argparse
import logging
from MemoryUtils import mem_string_to_float, mem_string_to_int, MEM_GB
from os import environ, makedirs, path, remove
from os.path import dirname, realpath
import re
from SchedulerJobInfo import logger as SchedulerJobInfo_logger, SchedulerJobInfo
from SchedulerLogParser import logger as SchedulerLogParser_logger, SchedulerLogParser
import subprocess # nosec
from subprocess import CalledProcessError, check_output # nosec
from sys import exit

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False

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

        self._sacct_fh = None
        self._line_number = 0
        self._eof = False
        self._parsed_lines = []

    SLURM_ACCT_FIELDS = [
        ('State', 's'),           # Displays the job status, or state. Put this first so can skip ignored states.
        ('JobID', 's'),     # The identification number of the job or job step
        # Job properties
        ('ReqCPUS', 'd'),  # Number of requested CPUs
        ('ReqMem', 'd'),   # Minimum required memory for the job
        ('ReqNodes', 'd'), # Requested minimum Node count for the job/step
        ('Constraints', 's'), #
        # Times
        ('Submit', 'dt'),    # The time the job was submitted. NOTE: If a job is requeued, the submit time is reset. This is handled by not overwriting fields with the batch step.
        ('Eligible', 'dt'),  # When the job became eligible to run
        ('Start', 'dt'),     # Initiation time of the job
        ('Elapsed', 'td'),   # The job's elapsed time: [DD-[HH:]]MM:SS
        ('Suspended', 'td'), # The amount of time a job or job step was suspended
        ('End', 'dt'),       # Termination time of the job

        ('ExitCode', 's'),        # The exit code returned by the job script or salloc
        ('DerivedExitCode', 's'), # The highest exit code returned by the job's job steps

        ('AllocNodes', 'd'), # Number of nodes allocated to the job/step
        ('NCPUS', 'd'),      # Total number of CPUs allocated to the job. Equivalent to AllocCPUS

        ('MaxDiskRead', 'd'),  # Maximum number of bytes read by all tasks in job')
        ('MaxDiskWrite', 'd'), # Maximum number of bytes written by all tasks in job
        ('MaxPages', 'd'),     # Maximum number of page faults of all tasks in job
        ('MaxRSS', 'd'),       # Maximum resident set size of all tasks in job
        ('MaxVMSize', 'd'),    # Maximum Virtual Memory size of all tasks in job
        ('CPUTime', 'td'),     # Time used (Elapsed time * CPU count) by a job or step in HH:MM:SS format
        ('UserCPU', 'td'),     # The amount of user CPU time used by the job or job step. Format is the same as Elapsed
        ('SystemCPU', 'td'),   # The amount of system CPU time used by the job or job step. Format is the same as Elapsed
        ('TotalCPU', 'td'),    # The sum of the SystemCPU and UserCPU time used by the job or job step
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

    def parse_jobs(self) -> None:
        '''
        Parse all the jobs from the Slurm sacct command.

        Returns:
            None
        '''
        self._parsed_lines = []
        self.errors = []
        job = True
        while job:
            job = self.parse_job()
        if self.errors:
            logger.error(f"{len(self.errors)} errors while parsing jobs")
        logger.info(f"Parsed {self.num_output_jobs()} jobs")

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

    def _call_sacct(self):
        '''
        Call sacct to get job information.

        Saves the output to a file and sets to the file.
        '''
        if self._sacct_input_file:
            raise RuntimeError("Cannot call _call_sacct when sacct_input_file given for input")

        # Create a file handle to write the sacct output to.
        sacct_write_fh = open(self._sacct_output_file, 'w')

        format_fields = []
        for field_tuple in self.SLURM_ACCT_FIELDS:
            format_fields.append(field_tuple[0])
        logger.debug(f"Calling sacct")
        args = ["sacct", '--allusers', '--parsable2', '--noheader', '--format', ','.join(format_fields)]
        starttime = self._starttime
        if not self._starttime:
            starttime = '1970-01-01T0:00:00'
        args.extend(['--starttime', starttime])
        if self._endtime:
            args.extend(['--endtime', self._endtime])
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
        if re.match(f'^\s*$', line):
            logger.debug("    Skipping blank line")
            return None
        if re.match(f'^\s*#', line):
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
                field_value = fields.pop(0)
                if field_value:
                    try:
                        if field_format == 'd':
                            field_value = mem_string_to_int(field_value)
                        elif field_format == 'f':
                            field_value = mem_string_to_float(field_value)
                        elif field_format == 'dt':
                            # Check value by trying to convert to datetime
                            SchedulerJobInfo.str_to_datetime(field_value)
                        elif field_format == 'td':
                            # Check value by trying to convert to timedelta
                            SchedulerJobInfo.str_to_timedelta(field_value)
                        elif field_format == 's':
                            pass
                        else:
                            raise ValueError(f"Invalid format of {field_format} for field {field_name}")
                    except ValueError as e:
                        field_errors += 1
                        msg = f"Unable to convert field {field_name} to format {field_format}: {field_value}: {e}\n{line}"
                        logger.error(f"{self._sacct_input_file}, line {self._line_number}: {msg}")
                        self.errors.append((self._sacct_input_file, self._line_number, msg))
                logger.debug(f"    {field_name}: '{field_value}' {type(field_value)}")
                job_fields[field_name] = field_value
        except Exception as e:
            field_errors += 1
            msg = f"Exception while processing fields, {field_name} ({field_format}): {e}\n{line}"
            logger.error(f"{self._sacct_input_file}, line {self._line_number}: {msg}")
            self.errors.append((self._sacct_input_file, self._line_number, msg))
        logger.debug(f"    job_fields: {job_fields}")

        if 'JobID' not in job_fields:
            return None
        job_fields['JobID'] = job_fields['JobID'].replace('.batch', '')
        job_id = job_fields['JobID']
        logger.debug(f"    job_id: {job_id}")
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
        logger.debug(f"Assembling job {job_id}")
        while self._parsed_lines and self._parsed_lines[0][0] == job_id:
            # Update fields. Don't overwrite with .update or else blank fields will overwrite non-blank fields.
            (job_id, next_job_fields, last_line_number, next_field_errors) = self._parsed_lines.pop(0)
            field_errors += next_field_errors
            logger.debug(f"    Updating job_fields")
            for field_tuple in self.SLURM_ACCT_FIELDS:
                field_name = field_tuple[0]
                if next_job_fields.get(field_name, None) and not job_fields.get(field_name, None):
                    job_fields[field_name] = next_job_fields[field_name]
        logger.debug(f"    Merged job fields: {job_fields}")

        if field_errors:
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
            return None

        if self._output_csv_fh:
            self.write_job_to_csv(job)
        return job

    def _create_job_from_job_fields(self, job_fields):
        '''
        Returns:
            SchedulerJobInfo: Parsed job info
        '''
        job = SchedulerJobInfo(
            job_id = job_fields['JobID'],
            resource_request = job_fields['Constraints'],
            num_cores = job_fields['ReqCPUS'],
            max_mem_gb = job_fields['ReqMem']/MEM_GB,
            num_hosts = job_fields['ReqNodes'],

            submit_time = job_fields['Submit'],
            eligible_time = job_fields['Eligible'],
            start_time = job_fields['Start'],
            run_time = job_fields['Elapsed'],
            finish_time = job_fields['End'],

            # ExitCode is {returncode}:{signal}
            exit_status = job_fields['ExitCode'].split(':')[0],
            ru_inblock = job_fields['MaxDiskRead'],
            ru_majflt = job_fields['MaxPages'],
            ru_maxrss = job_fields['MaxRSS'],
            # ru_minflt = job_fields['ru_minflt'],
            # ru_msgrcv = job_fields['ru_msgrcv'],
            # ru_msgsnd = job_fields['ru_msgsnd'],
            # ru_nswap = job_fields['ru_nswap'],
            ru_oublock = job_fields['MaxDiskWrite'],
            ru_stime = job_fields['SystemCPU'],
            ru_utime = job_fields['UserCPU'],
        )
        return job

    def _clean_up(self):
        '''
        Clean up after the last sacct line has been read.
        '''
        if not self._sacct_input_file:
            # Delete the tmp file
            remove(self._sacct_output_file)

def main():
    parser = argparse.ArgumentParser(description="Parse Slurm logs.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--slurm-root", required=False, help="Directory that contains the Slurm bin directory.")
    slurm_mutex_group = parser.add_mutually_exclusive_group(required=True)
    slurm_mutex_group.add_argument("--sacct-output-file", help="File where the output of sacct will be written. Cannot be used with --sacct-file. Required if --sacct-input-file not set.")
    slurm_mutex_group.add_argument("--sacct-input-file", help="File with the output of sacct so can process sacct output offline. Cannot be used with --sacct-output-file. Required if --sacct-output-file not set.")
    parser.add_argument("--output-csv", required=True, help="CSV file with parsed job completion records")
    parser.add_argument("--starttime", help="Select jobs after the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--endtime", help="Select jobs before the specified time. Format YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        SchedulerJobInfo_logger.setLevel(logging.DEBUG)
        SchedulerLogParser_logger.setLevel(logging.DEBUG)

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
    slurmLogParser.parse_jobs()

if __name__ == '__main__':
    main()
