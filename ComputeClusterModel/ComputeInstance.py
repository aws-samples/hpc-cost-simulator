#!/usr/bin/env python3
'''
ComputeInstance object

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

from datetime import datetime
import logging
from SchedulerJobInfo import SchedulerJobInfo

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

class ComputeInstance:

    # Constants
    LOCATION_EC2 = 'EC2'
    LOCATION_ON_PREM = 'OnPrem'
    LOCATIONS = [LOCATION_EC2, LOCATION_ON_PREM]
    VALID_THREADS_PER_CORE = [1, 2]
    ON_DEMAND = 'OnDemand'
    SPOT = 'spot'
    PRICING_OPTIONS = [ON_DEMAND, SPOT]
    BOOTING = 'Booting'
    RUNNING = 'Running'
    IDLE = 'Idle'
    STATES = [BOOTING, RUNNING, IDLE]
    INSTANCE_ID = 1

    def __init__(self, location: str, instance_type: str, number_of_cores: int, threads_per_core: int, ht_enabled: bool, mem_gb: float, pricing_option: str, hourly_cost: float, start_time: datetime, running_time: datetime) -> None:
        assert(location in ComputeInstance.LOCATIONS)
        assert threads_per_core in ComputeInstance.VALID_THREADS_PER_CORE, f"{threads_per_core} not in {ComputeInstance.VALID_THREADS_PER_CORE}"
        assert(pricing_option in ComputeInstance.PRICING_OPTIONS)

        self.instance_id = ComputeInstance.INSTANCE_ID
        ComputeInstance.INSTANCE_ID += 1

        self.instance_type = instance_type
        self.number_of_cores = number_of_cores
        self.threads_per_core = threads_per_core
        self.ht_enabled = ht_enabled
        self.mem_gb = mem_gb
        self.pricing_option = pricing_option
        self.hourly_cost = hourly_cost
        self.cost_per_core = self.hourly_cost / self.number_of_cores
        self.start_time = start_time
        self.running_time = running_time

        self.jobs = []
        if self.ht_enabled:
            self.number_of_cpus = self.number_of_cores * self.threads_per_core
        else:
            self.number_of_cpus = self.number_of_cores
        self.available_cpus = self.number_of_cpus
        self.available_mem_gb = self.mem_gb

        self._calculate_utilization()

    def allocate_job(self, job: SchedulerJobInfo, pricing_option: str, current_time: datetime) -> None:
        '''
        Add job to list of jobs running on the instance

        Adjust the instance's utilization.
        Adjust the job's start_time to the later of the current time and the time that the instance is running if the instance is still booting.
        The submit_time and eligible_time don't need to be adjusted, only the start_time. wait_time, and finish_time.
        '''
        assert job.num_cores <= self.available_cpus, f"{job.num_cores} > {self.available_cpus}"
        assert job.max_mem_gb <= self.available_mem_gb, f"{job.max_mem_gb} > {self.available_mem_gb}"
        assert pricing_option == self.pricing_option, f"{pricing_option} != {pricing_option}"
        self.jobs.append(job)
        self.available_cpus -= job.num_cores
        self.available_mem_gb -= job.max_mem_gb
        self._calculate_utilization()

        job.start_time_dt = max(self.running_time, current_time)
        job.wait_time_td = job.start_time_dt - job.eligible_time_dt
        job.finish_time_dt = job.start_time_dt + job.run_time_td

    def finish_job(self, job: SchedulerJobInfo) -> None:
        '''
        Remove finished jobs
        '''
        self.jobs.remove(job)
        self.available_cpus += job.num_cores
        self.available_mem_gb += job.max_mem_gb
        self._calculate_utilization()

    def _calculate_utilization(self):
        self.cpu_utilization = (self.number_of_cpus - self.available_cpus)/self.number_of_cpus
        self.mem_utilization = (self.mem_gb - self.available_mem_gb)/self.mem_gb
        self.utilization = max(self.cpu_utilization, self.mem_utilization)
        self.unutilization = 1.0 - self.utilization

    def __str__(self) -> str:
        return f"instance {self.instance_id}({self.instance_type} {self.pricing_option}) utilization={self.utilization} {len(self.jobs)} jobs start={self.start_time} running_time={self.running_time}"
