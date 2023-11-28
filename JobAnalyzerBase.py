#!/usr/bin/env python3
'''
Analyze the jobs parsed from scheduler logs

This module holds all the supporting functions required to initialize the data structure required to parse scheduler log files
from all schedulers. It builds the list of instances by memory / physical cores / core speed and gets the pricing for them to
allow jobs to be mapped to instance types

It also holds the function to map a job to the right instance type.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

from botocore.exceptions import ClientError, NoCredentialsError
from config_schema import check_schema
from datetime import datetime
from EC2InstanceTypeInfoPkg.EC2InstanceTypeInfo import EC2InstanceTypeInfo
import json
import logging
from openpyxl import Workbook as XlsWorkbook
from openpyxl.chart import BarChart3D, LineChart as XlLineChart, Reference as XlReference
from openpyxl.styles import Alignment as XlsAlignment, Protection as XlsProtection
from openpyxl.styles.numbers import FORMAT_CURRENCY_USD_SIMPLE
from openpyxl.utils import get_column_letter as xl_get_column_letter
import operator
from os import listdir, makedirs, path, remove
from os.path import dirname, realpath
import re
from SchedulerJobInfo import logger as SchedulerJobInfo_logger, SchedulerJobInfo, str_to_datetime, timestamp_to_datetime
from SchedulerLogParser import SchedulerLogParser
from sys import exit
import yaml

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60
SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR

class JobAnalyzerBase:

    def __init__(self, scheduler_parser: SchedulerLogParser, config_filename: str, output_dir: str, starttime: str, endtime: str, queue_filters: str, project_filters: str) -> None:
        '''
        Constructor

        Args:
            scheduler_parser (SchedulerLogParser): Job parser
            config_filename (str): Configuration file
            output_dir (str): Output directory
            starttime (str): Select jobs after the specified time
            endtime (str): Select jobs after the specified time
            queue_filters (str): Queue filters
            project_filters (str): Project filters
        Returns:
            None
        '''
        self._scheduler_parser = scheduler_parser
        self._config_filename = config_filename
        self._output_dir = realpath(output_dir)

        self._starttime = starttime
        self._endtime = endtime

        if not path.exists(self._output_dir):
            logger.info(f"Output directory ({self._output_dir}) doesn't exist, creating")
            makedirs(self._output_dir)

        logger.info(f"Loading configuration from {config_filename}.")
        self.config = JobAnalyzerBase.read_configuration(config_filename)

        if self._starttime:
            self._starttime_dt = str_to_datetime(self._starttime)
            self._first_hour = int(self._starttime_dt.timestamp() // SECONDS_PER_HOUR)
            logger.info(f"Start time: {self._starttime_dt}")
            logger.info(f"First hour: {self._first_hour} = {timestamp_to_datetime(self._first_hour * SECONDS_PER_HOUR)}")
        else:
            self._starttime_dt = None
            self._first_hour = None
        if self._endtime:
            self._endtime_dt = str_to_datetime(self._endtime)
            self._last_hour = int(self._endtime_dt.timestamp() // SECONDS_PER_HOUR)
            logger.info(f"End   time: {self._endtime_dt}")
            logger.info(f"Last  hour: {self._last_hour} = {timestamp_to_datetime(self._last_hour * SECONDS_PER_HOUR)}")
        else:
            self._endtime_dt = None
            self._last_hour = None

        if queue_filters != None:
            self.config['Jobs']['QueueRegExps'] = queue_filters.split(',')
        if project_filters != None:
            self.config['Jobs']['ProjectRegExps'] = project_filters.split(',')
        self._init_queue_project_filters()

        self.minimum_cpu_speed = self.config['consumption_model_mapping']['minimum_cpu_speed']

        self.region = self.config['instance_mapping']['region_name']

        self.range_min = self.config['instance_mapping']['range_minimum']
        self.range_max = self.config['instance_mapping']['range_maximum']
        self.ram_ranges_GB = self.config['instance_mapping']['ram_ranges_GB']
        self.runtime_ranges_minutes = self.config['instance_mapping']['runtime_ranges_minutes']

        # the code assumes all range lists are sorted
        self.ram_ranges_GB.sort()
        self.runtime_ranges_minutes.sort()

        self.job_data_collector = self.generate_collection_dict()

        self.jobs_by_hours = {}
        self._hourly_jobs_to_be_written = 0
        self._clear_job_stats()

        self.instance_type_info = None
        self.instance_family_info = None

        self._instance_types_used = {}
        self._instance_families_used = {}
        for purchase_option in ['spot', 'on_demand']:
            self._instance_types_used[purchase_option] = {}
            self._instance_families_used[purchase_option] = {}
        
        self._hyperthreading = self.config['instance_mapping']['hyperthreading']
        
    @staticmethod
    def read_configuration(config_filename):
        try:
            with open(config_filename,'r') as config_file:
                config = yaml.safe_load(config_file)
        except Exception as e:
            logger.error(f"Failed to read config file: {e}")
            raise
        try:
            validated_config = check_schema(config)
        except Exception as e:
            logger.error(f"{config_filename} has errors\n{e}")
            exit(1)
        return validated_config

    def _init_queue_project_filters(self):
        self._queue_filter_regexps = []
        for queue_filter_regexp in self.config['Jobs']['QueueRegExps']:
            queue_filter_regexp = queue_filter_regexp.lstrip("'\"")
            queue_filter_regexp = queue_filter_regexp.rstrip("'\"")
            logger.debug(f"queue_filter_regexp: {queue_filter_regexp}")
            logger.debug(f"queue_filter_regexp[0]: {queue_filter_regexp[0]}")
            if queue_filter_regexp[0] == '-':
                include = False
                queue_filter_regexp = queue_filter_regexp[1:]
                logger.debug(f"Exclude queues matching {queue_filter_regexp}")
            else:
                include = True
            self._queue_filter_regexps.append((include, re.compile(queue_filter_regexp)))
        if len(self._queue_filter_regexps) == 0:
            # If no filters specified then include all queues
            logger.debug('Set default queue filter')
            self._queue_filter_regexps.append((True, re.compile(r'.*')))

        self._project_filter_regexps = []
        for project_filter_regexp in self.config['Jobs']['ProjectRegExps']:
            if project_filter_regexp[0] == '-':
                include = False
                project_filter_regexp = project_filter_regexp[1:]
                logger.debug(f"Exclude projects matching {project_filter_regexp}")
            else:
                include = True
            self._project_filter_regexps.append((include, re.compile(project_filter_regexp)))
        if len(self._project_filter_regexps) == 0:
            # If no filters specified then include all projects
            logger.debug('Set default project filter')
            self._project_filter_regexps.append((True, re.compile(r'.*')))

    def get_ranges(self, range_array):
        '''
        returns a list of ranges based based on the given range edges in the array
        '''
        ranges = []
        previous_value = self.range_min
        for r in range_array:
            ranges.append(f"{previous_value}-{r}")
            previous_value = r
        ranges.append(f"{r}-{self.range_max}")
        return ranges

    def get_instance_type_info(self):
        logger.info('Getting EC2 instance type info')
        json_filename = 'instance_type_info.json'
        try:
            self.eC2InstanceTypeInfo = EC2InstanceTypeInfo([self.region], json_filename=json_filename)
        except NoCredentialsError as e:
            logger.exception(f'Failed to get EC2 instance types: {e}.')
            logger.error('Configure your AWS CLI credentials.')
            exit(1)
        except ClientError as e:
            logger.exception(f'Failed to get EC2 instance types: {e}.')
            logger.error('Update your AWS CLI credentials.')
            exit(1)
        except Exception as e:
            logger.exception(f'Failed to get EC2 instance types: {e}')
            exit(1)
        self.instance_type_info = self.eC2InstanceTypeInfo.instance_type_and_family_info[self.region]['instance_types']
        self.instance_family_info = self.eC2InstanceTypeInfo.instance_type_and_family_info[self.region]['instance_families']
        self.instance_types = {}
        for instance_type in self.instance_type_info:
            if self.instance_type_info[instance_type]['Hypervisor'] != 'nitro':
                continue
            for instance_prefix in self.config['instance_mapping']['instance_prefix_list']:
                if instance_type.lower().startswith(instance_prefix):
                    self.instance_types[instance_type] = 1
                    break
        self.instance_types = sorted(self.instance_types.keys())
        if not self.instance_types:
            logger.error(f"No instance types selected by instance_mapping['instance_prefix_list'] in {self._config_filename}")
            exit(2)
        logger.info(f"{len(self.instance_types)} instance types selected: {self.instance_types}")

    def get_instance_by_spec(self, required_ram_GiB: float, required_cores: int, required_speed: float=0):
        '''
            returns the list of instances that meet the provided spec (RAM, Core count, core speed).
            Instances are ONLY selected from those that meet the prefix filter in config.yaml

            Args:
                required_ram_GiB (float): required_ram_GiB
                required_cores (int): Number of required cores
                required_speed (float): Minimum required speed of the cores in GHz
            Returns:
                [str]: List of instance type names (e.g. ['c5.xlarge', 'm5.2xlarge'])
        '''
        if not self.instance_type_info:
            self.get_instance_type_info()

        relevant_instances = []
        for instance_type in self.instance_types:
            info = self.instance_type_info[instance_type]
            if (info['MemoryInMiB'] / 1024) >= required_ram_GiB:
                if info['SustainedClockSpeedInGhz'] >= required_speed:
                    if (not self._hyperthreading and info['DefaultCores'] >= required_cores) or (self._hyperthreading and info['DefaultVCpus'] >= required_cores):
                        relevant_instances.append(instance_type)

        logger.debug (f'instances with {required_cores} cores, {required_ram_GiB} GiB RAM and {required_speed} GhZ: {relevant_instances}')

        return relevant_instances

    def get_lowest_priced_instance(self, instance_types: [str], spot: bool):
        '''
            Returns the most cost effective instance and its hourly price of the instance types in instance_types in US dollars

            aws ec2 describe-spot-price-history \
                --availability-zone zone \
--instance-types
            Args:
                instance_types ([str]): List of instance types. E.g. ['m5.xlarge', 'c5.4xlarge']
            Returns:
                (str, float): Tuple with instance type and on-demand rate
        '''
        if not self.instance_type_info:
            self.get_instance_type_info()

        logger.debug(f"Finding cheapest instance: spot={spot} {instance_types}")
        min_price = 999999
        cheapest_instance_type = None

        for instance_type in instance_types:
            if spot:
                try:
                    price = self.instance_type_info[instance_type]['pricing']['spot']['max']
                except KeyError:
                    continue
            else:
                price = self.instance_type_info[instance_type]['pricing']['OnDemand']
            logger.debug(f"{instance_type}: price: {price}")
            if price < min_price:
                min_price = price
                cheapest_instance_type = instance_type
                logger.debug("cheaper")
        return (cheapest_instance_type, min_price)

    def generate_collection_dict(self):
        '''
        generates  a  dict for aggregating job runtime data by RAM, Runtime minutes

        '''
        collection_structure = {}
        for i in self.get_ranges(self.ram_ranges_GB):
            collection_structure[i] = {}
            for j in self.get_ranges(self.runtime_ranges_minutes):
                collection_structure[i][j] = {
                    'number_of_jobs': 0,
                    'total_duration_minutes': 0,
                    'total_wait_minutes': 0
                }
        return collection_structure

    def select_range(self, value, range_array):
        '''
            Chooses the correct range for the specified value
        '''
        r = 0
        previous_value = self.range_min
        range = None
        while range == None and r < len(range_array):
            if value <= range_array[r]:
                range = f'{previous_value}-{range_array[r]}'
            else:
                previous_value = range_array[r]
            r+=1
        if range == None:       # value is above range, use range_max
            range = f'{previous_value}-{self.range_max}'
        return range

    def _add_job_to_collector(self, job: SchedulerJobInfo) -> None:
        logger.debug(f"_add_job_to_collector({job})")
        runtime_minutes = job.run_time_td.total_seconds()/60
        logger.debug(f"runtime_minutes: {runtime_minutes}")
        wait_time_minutes = job.wait_time_td.total_seconds()/60
        logger.debug(f"wait_time_minutes: {wait_time_minutes}")
        job_RAM_range = self.select_range(job.max_mem_gb/job.num_hosts, self.ram_ranges_GB)
        logger.debug(f"job_RAM_range: {job_RAM_range}")
        job_runtime_range = self.select_range(runtime_minutes, self.runtime_ranges_minutes)
        logger.debug(f"job_runtime_range: {job_runtime_range}")
        logger.debug(f"Status of [{job_RAM_range}][{job_runtime_range}] BEFORE adding job {job.job_id}: {self.job_data_collector[job_RAM_range][job_runtime_range]}")
        logger.debug(f"job_id {job.job_id}, runtime {job.run_time}, waitime {job.wait_time}")
        self.job_data_collector[job_RAM_range][job_runtime_range]['number_of_jobs'] += 1
        self.job_data_collector[job_RAM_range][job_runtime_range]['total_duration_minutes'] += runtime_minutes # TODO: check code adhers to IBM LSF logic, handle multiple hosts
        self.job_data_collector[job_RAM_range][job_runtime_range]['total_wait_minutes'] += wait_time_minutes   # TODO: check code adhers to IBM LSF logic, handle multiple hosts
        logger.debug(f"Status of [{job_RAM_range}][{job_runtime_range}] AFTER adding job {job.job_id}: {self.job_data_collector[job_RAM_range][job_runtime_range]}")
        logger.debug('-------------------------------------------------------------------------------------------------')

    def _dump_job_collector_to_csv(self):
        '''
            Dumps the job_data_collector dict into a CSV file with similar name to the job's .out file
        '''
        logger.debug(f"Final job_data collector:{json.dumps(self.job_data_collector, indent=4)}")
        try:
            filename = path.join(self._output_dir, f"summary.csv")
            with open (filename, 'w+') as output_file:

                output = '''\nNote: All memory sizes are in GB, all times are in MINUTES (not hours)
                \nMemorySize,'''
                for runtime in self.get_ranges(self.runtime_ranges_minutes):
                    output += f"{runtime} Minutes,<--,<--,"
                output +="\n"
                output += ","+"Job count,Total duration,Total wait time,"*len(self.get_ranges(self.runtime_ranges_minutes))+"\n"
                for ram in self.get_ranges(self.ram_ranges_GB):
                    output += f"{ram}GB,"
                    for runtime in self.get_ranges(self.runtime_ranges_minutes):
                        summary = self.job_data_collector[ram][runtime]
                        output += f"{summary['number_of_jobs']},{summary['total_duration_minutes']},{summary['total_wait_minutes']},"
                    output += "\n"
                    output_file.write(output)
                    output = ''
        except PermissionError as e:
            logger.exception(f"Permission error accessing {filename}")
            exit(1)
        except IndexError as e:
            logger.exception(f"Index Error when trying to access job_data_collector[{ram}][{runtime}")
            exit(1)
        except Exception as e:
            logger.exception(f"Unknown Exception in dump_job_collector")
            exit(1)

    def _clear_job_stats(self):
        for ram in self.job_data_collector:
            for runtime in self.job_data_collector[ram]:
                for metric in self.job_data_collector[ram][runtime]:
                    self.job_data_collector[ram][runtime][metric] = 0
        self.hourly_stats = {}
        if self._starttime:
            round_hour = int(self._starttime_dt.timestamp() // 3600)
            self._init_hourly_stats_hour(round_hour)
        if self._endtime:
            round_hour = int(self._endtime_dt.timestamp() // 3600)
            self._init_hourly_stats_hour(round_hour)

        self.total_stats = {
            'spot': 0.0,
            'on_demand': {
                'total': 0.0,
                'instance_families': {}
            }
        }

    def _init_hourly_stats_hour(self, round_hour: int) -> None:
        round_hour = int(round_hour)
        if round_hour not in self.hourly_stats:
            logger.debug(f"Added {round_hour} to hourly_stats")
            self.hourly_stats[round_hour] = {
                'on_demand': {
                    'total': 0,
                    'core_hours': {}
                },
                'spot': 0
            }

    def _filter_job_queue(self, job: SchedulerJobInfo) -> bool:
        '''
        Filter the job queue

        Args:
            job (SchedulerJobInfo): Parsed job information
        Returns:
            bool: True if the job should be analyzed
        '''
        logger.debug(f"Filtering job {job.job_id} queue {job.queue}")
        if job.queue == None:
            queue = ''
        else:
            queue = job.queue
        for (include_filter, filter_regexp) in self._queue_filter_regexps:
            if filter_regexp.match(queue):
                logger.debug(f"job {job.job_id} queue={queue} matched {filter_regexp} include={include_filter}")
                return include_filter
        logger.debug(f"job {job.job_id} queue={queue} didn't match any filters")
        return False

    def _filter_job_project(self, job: SchedulerJobInfo) -> bool:
        '''
        Filter the job project

        Args:
            job (SchedulerJobInfo): Parsed job information
        Returns:
            bool: True if the job should be analyzed
        '''
        logger.debug(f"Filtering job {job.job_id} project {job.project}")
        if job.project == None:
            project = ''
        else:
            project = job.project
        for (include_filter, filter_regexp) in self._project_filter_regexps:
            if filter_regexp.match(project):
                logger.debug(f"job {job.job_id} project={project} matched {filter_regexp} include={include_filter}")
                return include_filter
        logger.debug(f"job {job.job_id} project={project} didn't match any filters.")
        return False
