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

from AcceleratorLogParser import AcceleratorLogParser, logger as AcceleratorLogParser_logger
import argparse
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from colored import fg
from copy import deepcopy
import csv
from CSVLogParser import CSVLogParser, logger as CSVLogParser_logger
from datetime import datetime, time, timedelta
from EC2InstanceTypeInfoPkg.EC2InstanceTypeInfo import EC2InstanceTypeInfo
import json
from LSFLogParser import LSFLogParser, logger as LSFLogParser_logger
import logging
from math import ceil
from os import listdir, makedirs, path, remove
from os.path import dirname, realpath
from SchedulerJobInfo import logger as SchedulerJobInfo_logger, SchedulerJobInfo
from SchedulerLogParser import logger as SchedulerLogParser_logger
from SlurmLogParser import SlurmLogParser, logger as SlurmLogParser_logger
from sys import exit
import typing
import yaml

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

class JobCost:
    def __init__(self, job: SchedulerJobInfo, spot: bool, instance_family: str, instance_type: str, rate: float):
        self.job = job
        self.spot = spot
        self.instance_family = instance_family
        self.instance_type = instance_type
        self.rate = rate

class JobAnalyzer:

    def __init__(self, scheduler_parser, config_filename, output_dir):
        self._scheduler_parser = scheduler_parser
        self._config_filename = config_filename
        self._output_dir = realpath(output_dir)

        if not path.exists(self._output_dir):
            logger.info(f"Output directory ({self._output_dir}) doesn't exist, creating")
            makedirs(self._output_dir)

        # Configure logfile
        self.timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file_name = path.join(self._output_dir, f"JobAnalyzer-{self.timestamp_str}.log")
        logger_FileHandler = logging.FileHandler(filename=log_file_name)
        logger_FileHandler.setFormatter(logger_formatter)
        logger.addHandler(logger_FileHandler)

        logger.info(f"Loading configuration from {config_filename}.")
        self.config = JobAnalyzer.read_configuration(config_filename)

        self.minimum_cpu_speed = self.config['consumption_model_mapping']['minimum_cpu_speed']

        self.region = self.config['instance_mapping']['region_name']

        self.range_min = self.config['instance_mapping']['range_minimum']
        self.range_max = self.config['instance_mapping']['range_maximum']
        self.ram_ranges_GB = self.config['instance_mapping']['ram_ranges_GB']
        self.runtime_ranges_minutes = self.config['instance_mapping']['runtime_ranges_minutes']

        # the code assumes all range lists are sorted
        self.ram_ranges_GB.sort()
        self.runtime_ranges_minutes.sort()

        self.jobs_by_hours = {}
        self._hourly_jobs_to_be_written = 0
        self.hourly_stats = {}

        self.job_data_collector = self.generate_collection_dict()

        self.instance_type_info = None

        self._instance_types_used = {}
        self._instance_families_used = {}
        for purchase_option in ['spot', 'on_demand']:
            self._instance_types_used[purchase_option] = {}
            self._instance_families_used[purchase_option] = {}

    @staticmethod
    def read_configuration(config_filename):
        try:
            with open(config_filename,'r') as config_file:
                return yaml.safe_load(config_file)
        except Exception as e:
            logger.error(f"Failed to read config file: {e}")
            raise

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
            self.instance_type_info = EC2InstanceTypeInfo([self.region], json_filename=json_filename)
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
        self.instance_types = {}
        for instance_type in self.instance_type_info.instance_type_info[self.region]:
            if self.instance_type_info.instance_type_info[self.region][instance_type]['Hypervisor'] != 'nitro':
                continue
            for instance_prefix in self.config['instance_mapping']['instance_prefix_list']:
                if instance_type.lower().startswith(instance_prefix):
                    self.instance_types[instance_type] = 1
                    break
        self.instance_types = sorted(self.instance_types.keys())

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
            info = self.instance_type_info.instance_type_info[self.region][instance_type]
            if (info['MemoryInMiB'] / 1024) >= required_ram_GiB:
                if info['SustainedClockSpeedInGhz'] >= required_speed:
                    if info['CoreCount'] >= required_cores:
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
                (str, float): Tuple with instance type and hourly rate
        '''
        if not self.instance_type_info:
            self.get_instance_type_info()

        min_price = 999999
        cheapest_instance_type = None

        for instance_type in instance_types:
            if spot:
                price = self.instance_type_info.instance_type_info[self.region][instance_type]['pricing']['spot']['max']
            else:
                price = self.instance_type_info.instance_type_info[self.region][instance_type]['pricing']['OnDemand']
            if price < min_price:
                min_price = price
                cheapest_instance_type = instance_type
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
        runtime_minutes = job.run_time_td.total_seconds()/60
        wait_time_minutes = job.wait_time_td.total_seconds()/60
        job_RAM_range = self.select_range(job.max_mem_gb, self.ram_ranges_GB)
        job_runtime_range = self.select_range(runtime_minutes, self.runtime_ranges_minutes)
        logger.debug(f"Status of [{job_RAM_range}][{job_runtime_range}] BEFORE adding job {job.job_id}: {self.job_data_collector[job_RAM_range][job_runtime_range]}")
        logger.debug(f"job_id {job.job_id}, runtime {job.run_time}, waitime {job.wait_time}")
        self.job_data_collector[job_RAM_range][job_runtime_range]['number_of_jobs'] += job.num_hosts
        self.job_data_collector[job_RAM_range][job_runtime_range]['total_duration_minutes'] += job.num_hosts * runtime_minutes # TODO: check code adhers to IBM LSF logic, handle multiple hosts
        self.job_data_collector[job_RAM_range][job_runtime_range]['total_wait_minutes'] += job.num_hosts * wait_time_minutes   # TODO: check code adhers to IBM LSF logic, handle multiple hosts
        logger.debug(f"Status of [{job_RAM_range}][{job_runtime_range}] AFTER adding job {job.job_id}: {self.job_data_collector[job_RAM_range][job_runtime_range]}")
        logger.debug('-------------------------------------------------------------------------------------------------')

    def _add_job_to_hourly_bucket(self, job_cost_data: JobCost) -> None:
        '''
        Put job into an hourly bucket

        The hourly buckets get written into files for scalability, but for performance reasons they are only
        written when the bucket contains a configurable number of jobs.
        This prevents a file open, write, close for each job.
        '''
        job = job_cost_data.job
        round_hour = int(job.start_time_dt.timestamp()//3600)
        if round_hour not in self.jobs_by_hours:
            self.jobs_by_hours[round_hour] = [job_cost_data]
        else:
            self.jobs_by_hours[round_hour].append(job_cost_data)
        self._hourly_jobs_to_be_written +=1
        if self._hourly_jobs_to_be_written >= self.config['consumption_model_mapping']['job_file_batch_size']:
            self._write_hourly_jobs_buckets_to_file()

    def _write_hourly_jobs_buckets_to_file(self) -> None:
        '''
        Write hourly jobs to files

        This is done in batches to reduce the number of file opens and closes.
        '''
        for round_hour, jobs in self.jobs_by_hours.items():
            hourly_file_name = path.join(self._output_dir, f"hourly-{round_hour}.csv")
            with open(hourly_file_name, 'a+') as job_file:
                if job_file.tell() == 0:    # Empty file - add headers
                    job_file.write('start_time,Job id,Num Hosts,Runtime (minutes),memory (GB),Wait time (minutes),Instance type,Instance Family,Spot,Hourly Rate,Total Cost\n')
                for job_cost_data in jobs:
                    job = job_cost_data.job
                    runtime_minutes = round(job.run_time_td.total_seconds()/60, 4)
                    runtime_hours = runtime_minutes / 60
                    total_cost = round(job.num_hosts * runtime_hours * job_cost_data.rate, 6)
                    wait_time_minutes = round(job.wait_time_td.total_seconds()/60, 4)
                    job_file.write(f"{SchedulerJobInfo.datetime_to_str(job.start_time_dt)},{job.job_id},{job.num_hosts},{runtime_minutes},{job.max_mem_gb},{wait_time_minutes},{job_cost_data.instance_type},{job_cost_data.instance_family},{job_cost_data.spot},{job_cost_data.rate},{total_cost}\n")
        self._hourly_jobs_to_be_written = 0
        self.jobs_by_hours = {}

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

    def get_hourly_files(self):
        '''
        Gets the hourly output files for the current job

        Input: none

        Output:
            Sorted list of output filenames
        '''
        all_files = listdir(self._output_dir)
        output_files = []
        prefix = path.basename("hourly-")
        for file in all_files:
            if file.startswith(prefix) and file[-4:] == ".csv":
                output_file = f"{self._output_dir}/" + file
                output_files.append(output_file)
        output_files.sort()
        return output_files

    def _cleanup_hourly_files(self):
        '''
        Delete the hourly output files so old results aren't included in new run.

        Input: none

        Output:
            Sorted list of output filenames
        '''
        hourly_files = self.get_hourly_files()
        for hourly_file in hourly_files:
            remove(hourly_file)

    def _clear_job_stats(self):
        for ram in self.job_data_collector:
            for runtime in self.job_data_collector[ram]:
                for metric in self.job_data_collector[ram][runtime]:
                    self.job_data_collector[ram][runtime][metric] = 0
        self.hourly_stats = {}

    def _update_hourly_stats(self, round_hour: int, minutes_within_hour: float, total_cost_per_hour: float, spot: bool, instance_family: str) -> None:
        '''
        Update the hourly stats dict with a portion of the cost of a job that fits within a round hour.

        A single job's cost may complete within the same round hour or span beyond it to multiple hours.
        Jobs are broken down by the Spot threshold.
        Args:
            round_hour (int): the hour of the HH:00:00 start
            minutes_within_hour (float): number of minutes the job ran within that round hour
            total_cost_per_hour (float): the total cost of all instances used to run the job if they ran for a full hour.
            spot (bool): True if spot instance
            instance_family (str): Instance family used for the job
        '''
        if round_hour not in self.hourly_stats:
            logger.debug(f"Added {round_hour} to hourly_stats")
            self.hourly_stats[round_hour] = {}
            self.hourly_stats[round_hour]['spot'] = 0
            self.hourly_stats[round_hour]['on_demand'] = {}
            self.hourly_stats[round_hour]['on_demand']['total'] = 0
        purchase_option = 'spot' if spot == True else 'on_demand'
        cost = minutes_within_hour / 60 * total_cost_per_hour
        if spot:
            self.hourly_stats[round_hour][purchase_option] += cost
        else:
            self.hourly_stats[round_hour][purchase_option]['total'] += cost
            self.hourly_stats[round_hour][purchase_option][instance_family] = cost + self.hourly_stats[round_hour][purchase_option].get(instance_family, 0)

    def _process_hourly_jobs(self) -> None:
        '''
        Process hourly job CSV files

        Sequentially processes the hourly output files to build an hourly-level cost simulation
        '''
        logger.info('')
        logger.info(f"Post processing hourly jobs data:\n")

        hourly_files = self.get_hourly_files()
        if len(hourly_files) == 0:
            logger.error(f"No hourly jobs files found")
            exit(2)

        for hourly_file in hourly_files:
            logger.info(f"Processing {hourly_file}")
            with open(hourly_file, 'r') as hourly_job_file_fh:
                csv_reader = csv.reader(hourly_job_file_fh, dialect='excel')
                field_names = next(csv_reader)
                num_jobs = 0
                line_number = 1
                while True:
                    try:
                        job_field_values_array = next(csv_reader)
                    except StopIteration:
                        break
                    num_jobs += 1
                    line_number += 1
                    job_field_values = {}
                    for i, field_name in enumerate(field_names):
                        job_field_values[field_name] = job_field_values_array[i]
                    start_time = SchedulerJobInfo.str_to_datetime(job_field_values['start_time']).timestamp()
                    job_id = job_field_values['Job id']
                    num_hosts = int(job_field_values['Num Hosts'])
                    job_runtime_minutes = float(job_field_values['Runtime (minutes)'])
                    instance_type = job_field_values['Instance type']
                    instance_family = job_field_values['Instance Family']
                    spot_eligible = job_field_values['Spot'] == 'True'
                    on_demand_rate = float(job_field_values['Hourly Rate'])
                    total_on_demand_cost = job_field_values['Total Cost']

                    end_time = start_time + job_runtime_minutes * 60
                    total_hourly_rate = on_demand_rate * num_hosts

                    logger.debug(f"    job {job_id}: line {line_number}")
                    logger.debug(f"        start_time={start_time}")
                    logger.debug(f"        start_time={start_time}")
                    logger.debug(f"        end_time  ={end_time}")
                    logger.debug(f"        instance_family={instance_family}")
                    logger.debug(f"        total cost={total_on_demand_cost}")
                    logger.debug(f"        spot_eligible={spot_eligible}")
                    logger.debug(f"        job_runtime_minutes={job_runtime_minutes}")
                    logger.debug(f"        total_hourly_rate={total_hourly_rate}")

                    round_hour = int(start_time//3600)
                    round_hour_seconds = round_hour * 3600
                    logger.debug(f"        round_hour: {round_hour}")
                    logger.debug(f"        round_hour_seconds: {round_hour_seconds}")
                    while round_hour_seconds <= end_time:
                        next_round_hour = round_hour + 1
                        next_round_hour_seconds = next_round_hour * 3600
                        if round_hour_seconds <= start_time < next_round_hour_seconds:
                            logger.debug(f"        Job started in this hour")
                            if end_time <= next_round_hour_seconds:
                                logger.debug(f"        job ended within hour")
                                runtime_minutes = (end_time - start_time)/60
                            else:
                                logger.debug(f"        job spills into the next hour")
                                runtime_minutes = (next_round_hour_seconds - start_time)/60
                        elif start_time <= round_hour_seconds and end_time > next_round_hour_seconds:
                            logger.debug(f"        Job started before this hour and runs throughout the hour")
                            runtime_minutes = 60
                        elif start_time < round_hour_seconds and end_time <= next_round_hour_seconds:
                            logger.debug(f"        Job started in prev hour, ends in this one")
                            runtime_minutes = (end_time - round_hour_seconds)/60
                        else:
                            logger.error(f"{file}, line {line_number}: Record failed to process correctly: {','.join(job_field_values_array)}")
                        self._update_hourly_stats(round_hour, runtime_minutes, total_hourly_rate, spot_eligible, instance_family)
                        round_hour += 1
                        round_hour_seconds = round_hour * 3600
            logger.info(f"    Finished processing ({num_jobs} jobs)")

    def _write_hourly_stats_csv(self):
        '''
        Write hourly stats to CSV file
        '''
        hourly_stats_csv = path.join(self._output_dir, 'hourly_stats.csv')
        logger.info('')
        logger.info(f"Writing hourly stats to {hourly_stats_csv}")

        hour_list = list(self.hourly_stats.keys())
        hour_list.sort()
        with open(hourly_stats_csv, 'w+') as hourly_stats_fh:
            # convert from absolute hour to relative one (obfuscation)
            csv_writer = csv.writer(hourly_stats_fh, dialect='excel')
            instance_families = sorted(self._instance_families_used['on_demand'].keys())
            field_names = ['Relative Hour','Total OnDemand Costs','Total Spot Costs'] + instance_families
            csv_writer.writerow(field_names)
            first_hour = hour_list[0]
            prev_relative_hour = 0
            for hour in hour_list:
                relative_hour = hour - first_hour
                logger.debug(f"    relative_hour: {relative_hour}")
                while prev_relative_hour < relative_hour - 1:   # add zero values for all missing hours
                    field_values = [prev_relative_hour + 1, 0, 0]
                    for instance_family in instance_families:
                        field_values.append(0)
                    csv_writer.writerow(field_values)
                    logger.debug(f"    empty hour: {field_values}")
                    prev_relative_hour += 1
                field_values = [relative_hour, round(self.hourly_stats[hour]['on_demand']['total'], 6), round(self.hourly_stats[hour]['spot'], 6)]
                for instance_family in instance_families:
                    field_values.append(round(self.hourly_stats[hour]['on_demand'].get(instance_family, 0), 6))
                csv_writer.writerow(field_values)
                prev_relative_hour = relative_hour

    def analyze_jobs(self):
        '''
        Analyze jobs

        Analyze jobs 1 by 1.
        Select a pricing plan and an instance type for each job and calculate the job's cost.
        Bin jobs by the hour in which they started and save the hourly jobs in separate CSV files.
        Process the hourly files to accumulate the hourly on-demand and spot costs broken out by instance family.
        Write the hourly costs into a separate CSV file.
        Write an overall job summary.

        Scalability is a key consideration because millions of jobs must be processessed so the analysis cannot be
        done in memory.
        First breaking the jobs out into hourly chunks makes the process scalable.
        '''
        if not self.instance_type_info:
            self.get_instance_type_info()

        self._clear_job_stats()   # allows calling multiple times in testing
        self._cleanup_hourly_files()

        total_jobs = 0
        total_failed_jobs = 0
        while True:
            job = self._scheduler_parser.parse_job()
            if not job:
                logger.debug(f"No more jobs")
                break
            total_jobs += 1
            try:
                job_cost_data = self.analyze_job(job)
            except RuntimeError:
                total_failed_jobs += 1
                continue
            self._add_job_to_collector(job)
            self._add_job_to_hourly_bucket(job_cost_data)
        logger.info(f"Finished processing {total_jobs-total_failed_jobs}/{total_jobs} jobs")

        # Dump pending jobs and summary to output files
        self._write_hourly_jobs_buckets_to_file()
        self._dump_job_collector_to_csv()

        self._process_hourly_jobs()
        self._write_hourly_stats_csv()

    def analyze_job(self, job: SchedulerJobInfo) -> JobCost:
        '''
        process a single job

        Args:
            job (SchedulerJobInfo): Parsed job information
        Returns:
            JobCost: Job cost information
        '''
        # Find the right instance type to run the job + its price
        min_memory_per_instance = ceil(job.max_mem_gb)
        potential_instance_types = self.get_instance_by_spec(min_memory_per_instance, job.num_cores, self.minimum_cpu_speed)
        if len(potential_instance_types) == 0:
            raise RuntimeError(f"Job {job.job_id} with {min_memory_per_instance} GB is too big to fit in a single instance.")
        job_runtime_minutes = job.run_time_td.total_seconds()/60
        spot_threshold = self.config['consumption_model_mapping']['maximum_minutes_for_spot']
        spot = job_runtime_minutes < spot_threshold
        (instance_type, rate) = self.get_lowest_priced_instance(potential_instance_types, spot)
        instance_family = EC2InstanceTypeInfo.get_instance_family(instance_type)
        job_cost_data = JobCost(job, spot, instance_family, instance_type, rate)

        if spot:
            purchase_option = 'spot'
        else:
            purchase_option = 'on_demand'
        self._instance_types_used[purchase_option][instance_type] = self._instance_types_used[purchase_option].get(instance_type, 0) + job.num_hosts
        self._instance_families_used[purchase_option][instance_family] = self._instance_families_used[purchase_option].get(instance_family, 0) + job.num_hosts
        return job_cost_data

def main():
    try:
        parser = argparse.ArgumentParser(description="Analyze jobs", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument("--starttime", help="Select jobs after the specified time. Format YYYY-MM-DDTHH:MM:SS")
        parser.add_argument("--endtime", help="Select jobs before the specified time. Format YYYY-MM-DDTHH:MM:SS")
        parser.add_argument("--config", required=False, default=f'{dirname(__file__)}/config.yml', help="Configuration file.")
        parser.add_argument("--output-dir", required=False, default="output", help="Directory where output will be written")
        parser.add_argument("--output-csv", required=False, default=None, help="CSV file with parsed job completion records")

        # Subparsers for scheduler specific arguments
        # required=True requires python 3.7 or later. Remove to make the installation simpler.
        subparsers = parser.add_subparsers(metavar='parser', dest='parser', help=f'Choose the kind of information to parse. {__file__} <parser> -h for parser specific arguments.')

        accelerator_parser = subparsers.add_parser('accelerator', help='Parse Accelerator (nc) job information', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        accelerator_parser.add_argument("--default-mem-gb", required=False, default=AcceleratorLogParser.DEFAULT_MEMORY_GB, help="Default amount of memory (in GB) requested for jobs.")
        accelerator_mutex_group = accelerator_parser.add_mutually_exclusive_group(required=True)
        accelerator_mutex_group.add_argument("--sql-output-file", help=f"File where the output of sql query will be written. Cannot be used with --sql-input-file. Required if --sql-input-file not set. \nCommand to create file:\n{AcceleratorLogParser._VOVSQL_QUERY_COMMAND} > SQL_OUTPUT_FILE")
        accelerator_mutex_group.add_argument("--sql-input-file", help="File with the output of sql query so can process it offline. Cannot be used with --sql-output-file. Required if --sql-output-file not set.")

        csv_parser = subparsers.add_parser('csv', help='Parse CSV from already parsed job information.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        csv_parser.add_argument("--input-csv", required=True, help="CSV file with parsed job info from scheduler parser.")

        lsf_parser = subparsers.add_parser('lsf', help='Parse LSF logfiles', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        lsf_parser.add_argument("--logfile-dir", required=False, help="LSF logfile directory")
        lsf_parser.add_argument("--default-max-mem-gb", type=float, required=True, help="Default maximum memory for a job in GB.")

        slurm_parser = subparsers.add_parser('slurm', help='Parse Slurm job information', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        slurm_parser.add_argument("--slurm-root", required=False, help="Directory that contains the Slurm bin directory.")
        slurm_mutex_group = slurm_parser.add_mutually_exclusive_group()
        slurm_mutex_group.add_argument("--sacct-output-file", required=False, help="File where the output of sacct will be written. Cannot be used with --sacct-input-file. Required if --sacct-input-file not set.")
        slurm_mutex_group.add_argument("--sacct-input-file", required=False, help="File with the output of sacct so can process sacct output offline. Cannot be used with --sacct-output-file. Required if --sacct-output-file not set.")

        parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
        args = parser.parse_args()

        if args.debug:
            logger.setLevel(logging.DEBUG)
            AcceleratorLogParser_logger.setLevel(logging.DEBUG)
            CSVLogParser_logger.setLevel(logging.DEBUG)
            LSFLogParser_logger.setLevel(logging.DEBUG)
            SchedulerJobInfo_logger.setLevel(logging.DEBUG)
            SchedulerLogParser_logger.setLevel(logging.DEBUG)
            SlurmLogParser_logger.setLevel(logging.DEBUG)

        if not args.parser:
            logger.error("The following arguments are required: parser")
            exit(2)
        logger.info('Started job analyzer')

        if args.parser == 'csv':
            logger.info(f"Reading job data from {args.input_csv}")
            scheduler_parser = CSVLogParser(args.input_csv, args.output_csv, args.starttime, args.endtime)
        elif args.parser == 'accelerator':
            scheduler_parser = AcceleratorLogParser(default_mem_gb=float(args.default_mem_gb), sql_input_file=args.sql_input_file, sql_output_file=args.sql_output_file, output_csv=args.output_csv, starttime=args.starttime, endtime=args.endtime)
        elif args.parser == 'lsf':
            if not args.logfile_dir or not args.output_csv:
                logger.error(f"You must provide --logfile-dir and --output-csv for LSF.")
                exit(1)
            scheduler_parser = LSFLogParser(args.logfile_dir, args.output_csv, args.default_max_mem_gb, args.starttime, args.endtime)
        elif args.parser == 'slurm':
            scheduler_parser = SlurmLogParser(args.sacct_input_file, args.sacct_output_file, args.output_csv, args.starttime, args.endtime)

        if args.output_csv:
            logger.info(f"Writing job data to {args.output_csv}")

        jobAnalyzer = JobAnalyzer(scheduler_parser, args.config, args.output_dir)
        jobAnalyzer.analyze_jobs()
    except Exception as e:
        logger.exception(f"Unhandled exception")
        exit(1)

if __name__ == '__main__':
    main()
