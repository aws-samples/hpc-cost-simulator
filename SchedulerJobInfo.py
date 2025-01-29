'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0

Generic class to capture required information for scheduler jobs.

Different schedulers report time in different formats so this class
standardized on the Slurm formats instead of the epoch seconds used by
LSF because it is much easier to read.

There are 2 types of date/time formats used.

* Date and Time: YYYY-MM-DDTHH:MM::SS
* Duration: [DD-[HH:]]MM:SS

.. _Google Python Style Guide:
   http://google.github.io/styleguide/pyguide.html
'''
__docformat__ = 'google'

from datetime import datetime, timedelta, timezone
import logging
import re

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
#logger.propagate = False
logger.setLevel(logging.INFO)

class SchedulerJobInfo:
    '''
    Class used by the scheduler to store job information

    The field names are based on LSF names because that was the first parser we implemented.
    This data structure puts the data into a common format that isn't parser dependent so that analysis
    isn't parser dependent.

    Note that not all schedulers may provide all of the fields but they must at least provide the
    required fields.
    Scripts that use this should validate the existence of optional fields.
    '''

    invalid_submit_time_warning = False
    invalid_start_time_warning = False
    invalid_ineligible_pend_time_warning = False
    invalid_finish_time_warning = False
    finish_time_lt_start_time_warning = False

    def __init__(self,
        # Required fields
        job_id:int,
        num_cores:int,
        max_mem_gb:float,
        num_hosts:int,
        submit_time:str,
        start_time:str,
        finish_time:str,

        # Optional fields
        ineligible_pend_time:str=None,
        eligible_time:str=None,
        requeue_time:str=None,
        wait_time:str=None,
        run_time:str=None,

        queue:str=None,
        project:str=None,
        licenses:str=None,

        exit_status:int=None,

        ru_inblock:int=None,
        ru_majflt:int=None,
        ru_maxrss:int=None,
        ru_minflt:int=None,
        ru_msgrcv:int=None,
        ru_msgsnd:int=None,
        ru_nswap:int=None,
        ru_oublock:int=None,
        ru_stime:float=None,
        ru_utime:float=None,

        # Put resource_request at end because can contain ',' which is also the CSV separator
        resource_request:str='',
        ):
        '''
        Constructor

        datetime fields can be passed as a timestamp or in ISO format.
        The arg is checked by trying to create a datetime object.

        timedelta fields can be passed as an integer number of seconds or in the following format: `[DD-[HH:]]MM:SS`.
        The arg is checked by trying to create a timedelta object.

        If an arg is the wrong time and can't be converted then a ValueError exception is raised.

        Args:
            job_id (int): Unique Job Id
            num_cores (int): Number of cores requested for the job. This is the total number across all hosts/nodes.
            max_mem_gb (float): Amount of memory requested by the job. This is the total number across all hosts/nodes.
            num_hosts (int): Number of compute nodes requested by the job
            submit_time (str): Date and time that the job was submitted.
            start_time (str): Date and time that the job started on the compute node
            finish_time (str): Date and time that the job finished.

            ineligible_pend_time (str): LSF: The time that the job was pending because it was ineligible to run because of unmet dependencies
            eligible_time (str): Slurm: Date and time when the job became eligible to run. Can be used to calculate ineligible_pend_time.
            requeue_time (str): LSF: The job's requeue time.
            wait_time (str): The time that the job waited to start after it was eligible.
            run_time (str): The time that the job ran. It should be the difference between finish_time and start_time.

            queue (str): queue that the job was submitted to.
            project (str): project that the job belongs to
            licenses (str): comma separated list of licenses used by the job. Format: license1[:int][license2[:int]]

            exit_status (int):

            ru_inblock (int):
            ru_majflt (int):
            ru_maxrss (int):
            ru_minflt (int):
            ru_msgrcv (int):
            ru_msgsnd (int):
            ru_nswap (int):
            ru_oublock (int):
            ru_stime (int):
            ru_utime (int):

            resource_request (str): Additional resources requested by the job, for example, licenses

        Returns:
            `SchedulerJobInfo`

        Raises:
            ValueError: If arg can't be converted to the required type.
        '''
        # Required fields
        self.job_id = job_id
        self.num_cores = num_cores
        self.max_mem_gb = max_mem_gb
        self.num_hosts = num_hosts
        (self.submit_time, self.submit_time_dt) = SchedulerJobInfo.fix_datetime(submit_time)
        (self.start_time, self.start_time_dt) = SchedulerJobInfo.fix_datetime(start_time)
        (self.finish_time, self.finish_time_dt) = SchedulerJobInfo.fix_datetime(finish_time)

        if not self.submit_time:
            if not SchedulerJobInfo.invalid_submit_time_warning:
                logger.warning(f"Invalid submit time for job {self.job_id}. Submit time={submit_time}. Discarding job and suppressing additional warnings.")
                SchedulerJobInfo.invalid_submit_time_warning = True
            raise ValueError(f"Invalid submit time for job {self.job_id}. Submit time={submit_time}.")

        if not self.start_time or self.start_time_dt < self.submit_time_dt:
            if not SchedulerJobInfo.invalid_start_time_warning:
                logger.warning(f"Invalid start time for job {self.job_id}. Start time {self.start_time} < submit time {self.submit_time}. Setting to submit time and suppressing additional warnings. finish_time={finish_time} run_time={run_time}")
                SchedulerJobInfo.invalid_start_time_warning = True
            self.start_time = self.submit_time
            self.start_time_dt = self.submit_time_dt

        if not self.finish_time:
            if not SchedulerJobInfo.invalid_finish_time_warning:
                logger.warning(f"Invalid finish time for job {self.job_id}. Finish time={finish_time}. Discarding job and suppressing additional warnings.")
                SchedulerJobInfo.invalid_finish_time_warning = True
            raise ValueError(f"Invalid finish time for job {self.job_id}. Finish time={finish_time}.")

        if self.finish_time_dt < self.start_time_dt:
            if not SchedulerJobInfo.finish_time_lt_start_time_warning:
                logger.warning(f"Invalid finish time for job {self.job_id}. Finish time={self.finish_time} < Start time={self.start_time}. Discarding job and suppressing additional warnings.")
                SchedulerJobInfo.finish_time_lt_start_time_warning = True
            raise ValueError(f"Invalid finish time for job {self.job_id}. Finish time={self.finish_time} < Start time={self.start_time}.")

        # Optional fields
        try:
            (self.ineligible_pend_time, self.ineligible_pend_time_td) = SchedulerJobInfo.fix_duration(ineligible_pend_time)
        except:
            if not SchedulerJobInfo.invalid_ineligible_pend_time_warning:
                logger.warning(f"Invalid ineligible_pend_time for job {self.job_id}: {ineligible_pend_time}. Setting to None and suppressing additional warnings.")
                SchedulerJobInfo.invalid_ineligible_pend_time_warning = True
            self.ineligible_pend_time = self.ineligible_pend_time_td = None
        try:
            (self.eligible_time, self.eligible_time_dt) = SchedulerJobInfo.fix_datetime(eligible_time)
        except:
            logger.warning(f"Invalid eligible_time for job {self.job_id}: {eligible_time}")
            self.eligible_time = self.eligible_time_dt = None
        try:
            (self.requeue_time, self.requeue_time_td) = SchedulerJobInfo.fix_duration(requeue_time)
        except:
            logger.warning(f"Invalid requeue_time for job {self.job_id}: {requeue_time}")
            self.requeue_time = self.requeue_time_td = None
        try:
            (self.wait_time, self.wait_time_td) = SchedulerJobInfo.fix_duration(wait_time)
        except:
            logger.warning(f"Invalid wait_time for job {self.job_id}: {wait_time}")
            self.wait_time = self.wait_time_td = None
        try:
            (self.run_time, self.run_time_td) = SchedulerJobInfo.fix_duration(run_time)
        except:
            logger.warning(f"Invalid run_time for job {self.job_id}: {run_time}")
            self.run_time = self.run_time_td = None

        self.queue = queue
        self.project = project
        self.licenses = licenses

        self.exit_status = SchedulerJobInfo.fix_int(exit_status)

        self.ru_inblock = SchedulerJobInfo.fix_int(ru_inblock)
        self.ru_majflt = SchedulerJobInfo.fix_int(ru_majflt)
        self.ru_maxrss = SchedulerJobInfo.fix_int(ru_maxrss)
        self.ru_minflt = SchedulerJobInfo.fix_int(ru_minflt)
        self.ru_msgrcv = SchedulerJobInfo.fix_int(ru_msgrcv)
        self.ru_msgsnd = SchedulerJobInfo.fix_int(ru_msgsnd)
        self.ru_nswap = SchedulerJobInfo.fix_int(ru_nswap)
        self.ru_oublock = SchedulerJobInfo.fix_int(ru_oublock)
        self.ru_stime = SchedulerJobInfo.fix_duration(ru_stime)[0]
        self.ru_utime = SchedulerJobInfo.fix_duration(ru_utime)[0]

        self.resource_request = resource_request

        if not self.ineligible_pend_time:
            if self.eligible_time:
                self.ineligible_pend_time_td = self.eligible_time_dt - self.submit_time_dt
                self.ineligible_pend_time = timedelta_to_string(self.ineligible_pend_time_td)
            else:
                (self.ineligible_pend_time, self.ineligible_pend_time_td) = SchedulerJobInfo.fix_duration("00:00")
        if not self.eligible_time:
            if self.ineligible_pend_time:
                self.eligible_time_dt = self.submit_time_dt + self.ineligible_pend_time_td
                self.eligible_time = datetime_to_str(self.eligible_time_dt)
            else:
                self.eligible_time = self.submit_time
                self.eligible_time_dt = self.submit_time_dt

        # Bug 31 saved the start_time even if it was 0 and less than submit time.
        # Check for this condition and set the start time to the eligible time
        if self.start_time_dt < self.submit_time_dt:
            self.start_time = self.eligible_time
            self.start_time_dt = self.eligible_time_dt

        # Bug 22 incorrectly calculated the wait_time using start_time instead of submit_time so just always calculate it so it's correct.
        self.wait_time_td = self.start_time_dt - self.eligible_time_dt
        self.wait_time = timedelta_to_string(self.wait_time_td)

        if not self.run_time:
            self.run_time_td = self.finish_time_dt - self.start_time_dt
            self.run_time = timedelta_to_string(self.run_time_td)

    @staticmethod
    def from_dict(field_dict: dict):
        job_id = int(field_dict['job_id'])
        num_cores = int(field_dict['num_cores'])
        max_mem_gb = float(field_dict['max_mem_gb'])
        num_hosts = int(field_dict['num_hosts'])
        submit_time = str(field_dict['submit_time'])
        start_time = str(field_dict['start_time'])
        finish_time = str(field_dict['finish_time'])

        ineligible_pend_time = str(field_dict['ineligible_pend_time'])
        eligible_time = str(field_dict['eligible_time'])
        requeue_time = str(field_dict['requeue_time'])
        wait_time = str(field_dict['wait_time'])
        run_time = str(field_dict['run_time'])

        queue = field_dict.get('queue', None)
        project = field_dict.get('project', None)
        licenses = field_dict.get('licensese', None)

        exit_status = SchedulerJobInfo.fix_int(field_dict['exit_status'])

        ru_inblock = SchedulerJobInfo.fix_int(field_dict['ru_inblock'])
        ru_majflt = SchedulerJobInfo.fix_int(field_dict['ru_majflt'])
        ru_maxrss = SchedulerJobInfo.fix_int(field_dict['ru_maxrss'])
        ru_minflt = SchedulerJobInfo.fix_int(field_dict['ru_minflt'])
        ru_msgrcv = SchedulerJobInfo.fix_int(field_dict['ru_msgrcv'])
        ru_msgsnd = SchedulerJobInfo.fix_int(field_dict['ru_msgsnd'])
        ru_nswap = SchedulerJobInfo.fix_int(field_dict['ru_nswap'])
        ru_oublock = SchedulerJobInfo.fix_int(field_dict['ru_oublock'])
        ru_stime = str(field_dict['ru_stime'])
        ru_utime = str(field_dict['ru_utime'])

        resource_request = str(field_dict['resource_request'])

        return SchedulerJobInfo(
            job_id = job_id,
            num_cores = num_cores,
            max_mem_gb = max_mem_gb,
            num_hosts = num_hosts,
            submit_time = submit_time,
            start_time = start_time,
            finish_time = finish_time,
            # Optional fields
            ineligible_pend_time = ineligible_pend_time,
            eligible_time = eligible_time,
            requeue_time = requeue_time,
            wait_time = wait_time,
            run_time = run_time,

            queue = queue,
            project = project,
            licenses = licenses,

            exit_status = exit_status,

            ru_inblock = ru_inblock,
            ru_majflt = ru_majflt,
            ru_maxrss = ru_maxrss,
            ru_minflt = ru_minflt,
            ru_msgrcv = ru_msgrcv,
            ru_msgsnd = ru_msgsnd,
            ru_nswap = ru_nswap,
            ru_oublock = ru_oublock,
            ru_stime = ru_stime,
            ru_utime = ru_utime,

            resource_request = resource_request,
        )

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    MINUTE_SECONDS = 60
    HOUR_SECONDS = MINUTE_SECONDS * 60
    DAY_SECONDS = HOUR_SECONDS * 24

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        del d['submit_time_dt']
        del d['start_time_dt']
        del d['finish_time_dt']
        del d['eligible_time_dt']
        del d['wait_time_td']
        del d['run_time_td']
        del d['ineligible_pend_time_td']
        del d['requeue_time_td']
        return d

    def fields(self):
        return self.to_dict().keys()

    @staticmethod
    def fix_datetime(value):
        '''
        Check and fix a DateTime passed as an integer or string.

        DateTime should be stored in the ISO format: `YYYY-MM-DDTHH:MM::SS`

        This is used by the constructor.

        LSF passes times as an integer timestamp.
        If the value is -1 then return None.
        If the integer value is passed as a string then it will be converted to an integer.
        It is checked by converting it to a datetime.datetime object.

        Slurm passes times in ISO format.
        If the string is blank then return None.
        The value is checked by calling `str_to_datetime`.

        The datetime object is then converted back to a string using `timedelta_to_str` to ensure that is formatted correctly with padded integers.

        The value is checked by trying to create a datetime.datetime object using `str_to_timedelta`.

        Args:
            value (int|str): An integer timestamp or string representing a duration.

        Raises:
            ValueError: If value is not a supported type or value.

        Returns:
            tuple(str, datetime): typle with ISO format DateTime string: `YYYY-MM-DDTHH:MM::SS` and datetime object
        '''
        if value == None:
            return (None, None)
        dt_str = None
        dt = None
        if str(type(value)) == "<class 'int'>":
            # LSF provides a value of -1 to mean None. Otherwise seconds since the epoch.
            if value == -1:
                return (None, None)
            dt = timestamp_to_datetime(value)
        elif str(type(value)) == "<class 'str'>":
            if re.match(r'^\s*$', value) or value == '-1':
                return (None, None)
            # Check if integer passed with wrong type
            try:
                value = int(value)
                return SchedulerJobInfo.fix_datetime(value)
            except ValueError:
                pass
            # SLURM: Make sure it's the right format
            dt = str_to_datetime(value)
        else:
            raise ValueError(f"Invalid type for datetime: {value} has type '{type(value)}', expected int or str")
        if dt.timestamp() == 0:
            return (None, None)
        dt_str = datetime_to_str(dt)
        return (dt_str, dt)

    @staticmethod
    def fix_duration(duration):
        '''
        Check and fix a duration passed as an integer or string.

        Durations should be of the following format: `[DD-[HH:]]MM:SS`
        Chose to use this value instead of an integer for readability.

        This is used by the constructor.

        LSF passes durations in as an integer. If the duration is -1 then return None.
        If the integer duration is passed as a string then it will be converted to an integer.
        It is checked by converting it to a datetime.timedelta object.

        Slurm passes in a duration as a string formatted as above.
        The duration is checked by calling `str_to_timedelta`.

        The timedelta object is then converted back to a string using `timedelta_to_str` to ensure that is formatted correctly with padded integers.

        The duration is checked by trying to create a datetime.timedelta object using `str_to_timedelta`.

        Args:
            duration (int|str): An integer timestamp or string representing a duration.

        Raises:
            ValueError: If duration is not a supported type or value.

        Returns:
            tuple(str, timedelta): tuple with time formatted as `[DD-[HH:]]MM:SS` and corresponding timedelta object
        '''
        if duration == None:
            return (None, None)
        if str(type(duration)) == "<class 'int'>" or str(type(duration)) == "<class 'float'>":
            if duration == -1:
                return (None, None)
            seconds = float(duration)
            td = timedelta(seconds=seconds)
        elif str(type(duration)) == "<class 'str'>":
            # Check if integer or float passed as a string
            try:
                duration_int = int(duration)
                return SchedulerJobInfo.fix_duration(duration_int)
            except ValueError:
                pass
            try:
                duration_float = float(duration)
                return SchedulerJobInfo.fix_duration(duration_float)
            except ValueError:
                pass
            if duration in ['', 'None']:
                return (None, None)
            # Check format
            td = str_to_timedelta(duration)
        else:
            raise ValueError(f"Invalid type for duration: {duration} has type '{type(duration)}', expected int, float, or str")
        duration_str = timedelta_to_string(td)
        return (duration_str, td)

    @staticmethod
    def fix_int(value):
        '''
        Fix an integer arg

        Args:
            value (None | str | int | float): Value that should be converted to an integer or None.
        Returns:
            int|None: Returns None if value is None or an empty string.
        '''
        if value == None:
            return None
        if str(type(value)) == "<class 'int'>":
            return value
        if str(type(value)) == "<class 'str'>":
            if value in ['', 'None']:
                return None
        elif str(type(value)) != "<class 'float'>":
            raise ValueError(f"Invalid type for value: {value} has type '{type(value)}', expected int, float, or str")
        return int(float(value))

    @staticmethod
    def fix_float(value):
        '''
        Fix a float arg

        Args:
            value (None | str | int | float): Value that should be converted to a float or None.
        Returns:
            float|None: Returns None if value is None or an empty string.
        '''
        if value == None:
            return None
        if str(type(value)) == "<class 'float'>":
            return value
        if str(type(value)) == "<class 'str'>":
            if value in ['', 'None']:
                return None
        else:
            raise ValueError(f"Invalid type for value: {value} has type '{type(value)}', expected float or str")
        return float(value)


def timestamp_to_datetime(timestamp) -> datetime:
    '''
    Convert timestamp to a datetime object.

    Args:
        timestamp (int or float): Timestamp representing the number of seconds since the epoch.

    Raises:
        ValueError: If timestamp is the wrong type or can't be converted to a datetime object.

    Returns:
        datetime.datetime: The timestamp converted to a datetime object.
    '''
    if timestamp == None:
        return timestamp
    if str(type(timestamp)) not in ["<class 'int'>", "<class 'float'>"]:
        raise ValueError(f"Invalid type for timestamp: {timestamp} has type '{type(timestamp)}', expected int or float")
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)

def str_to_datetime(string_value: str) -> datetime:
    '''
    Convert an ISO format DateTime string to a datetime.datetime object.

    Args:
        string_value: ISO format TimeDate: `YYYY-MM-DDTHH:MM::SS`

    Raises:
        ValueError: if string_value is in the wrong format or type.

    Returns:
        datetime.datetime: datetime object created from the string.
    '''
    if str(type(string_value)) != "<class 'str'>":
        raise ValueError(f"Invalid type for string_value: {string_value} has type '{type(string_value)}', expected str")
    return datetime.strptime(string_value, SchedulerJobInfo.DATETIME_FORMAT).replace(tzinfo=timezone.utc)

def datetime_to_str(dt: datetime) -> str:
    '''
    Convert a datetime.datetime object to an ISO format string.

    Args:
        dt: datetime.datetime object

    Raises:
        AttributeError: If `dt` is not a datetime object
    Returns:
    '''
    if str(type(dt)) != "<class 'datetime.datetime'>":
        raise ValueError(f"Invalid type for dt: {dt} has type '{type(dt)}', expected datetime")
    return dt.strftime(SchedulerJobInfo.DATETIME_FORMAT)

def str_to_timedelta(string_value: str) -> timedelta:
    '''
    Convert a str representing a timedelta to a datetime.timedelta object.

    Args:
        string_value: Duration should be of the following format: [DD-[HH:]]MM:SS

    Raises:
        ValueError: If `string_value` is not a str.

    Returns:
        datetime.timedelta: A timedelta object
    '''
    if str(type(string_value)) != "<class 'str'>":
        raise ValueError(f"Invalid type for string_value: {string_value} has type '{type(string_value)}', expected str")
    values = string_value.split(':')
    seconds = float(values.pop())
    minutes = int(values.pop())
    hours = 0
    days = 0
    if values:
        values = values.pop().split('-')
        hours = int(values.pop())
    if values:
        days = int(values.pop())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

def timedelta_to_string(td: timedelta) -> str:
    '''
    Convert a datetime.timedelta object to a duration string.

    Args:
        td: datetime.timedelta object

    Raises:
        AttributeError: If `td` is not a datetime.timedelta object
    Returns:
        str: Duration in the following format: [DD-[HH:]]MM:SS
    '''
    if str(type(td)) != "<class 'datetime.timedelta'>":
        raise ValueError(f"Invalid type for td: {td} has type '{type(td)}', expected datetime")
    s = ''
    seconds = td.total_seconds()
    days = int(seconds / SchedulerJobInfo.DAY_SECONDS)
    seconds = seconds - (days * SchedulerJobInfo.DAY_SECONDS)
    td = td - timedelta(days=days)
    if days:
        s += f"{days:02}-"
    s += f"{str(td)}"
    return s
