#!/usr/bin/env python3

from abc import ABC, abstractmethod
from ComputeClusterModel.ComputeInstance import ComputeInstance
from datetime import datetime, timedelta
import logging
from SchedulerJobInfo import SchedulerJobInfo

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

class ComputeClusterEvent(ABC):

    def __init__(self, event_time: datetime) -> None:
        self.time = event_time

class ScheduleJobsEvent(ComputeClusterEvent):
    '''
    ScheduleJobsEvent: Event triggering the job scheduling loop
    '''
    def __init__(self, event_time: datetime) -> None:
        super().__init__(event_time)

    def __str__(self) -> str:
        return f"{self.time}: ScheduleJobsEvent"

class JobFinishEvent(ComputeClusterEvent):
    '''
    JobFinishEvent: Includes the instance running the job.
    '''
    def __init__(self, event_time: datetime, job: SchedulerJobInfo, instance: ComputeInstance) -> None:
        super().__init__(event_time)
        self.job = job
        self.instance = instance

    def __str__(self) -> str:
        return f"{self.time}: JobFinishEvent job {self.job.job_id} start={self.job.start_time_dt} finish={self.job.finish_time} {self.instance}"

class InstanceTerminateEvent(ComputeClusterEvent):
    '''
    InstanceTerminateEvent: Set when last job on instance terminates. Cancel instance termination if job has been allocated.
    '''
    def __init__(self, event_time: datetime, instance: ComputeInstance) -> None:
        super().__init__(event_time)
        self.instance = instance

    def __str__(self) -> str:
        return f"{self.time}: InstanceTerminateEvent {self.instance}"

class EndOfHourEvent(ComputeClusterEvent):
    '''
    EndOfHourEvent: Update HourlyCost with cost of instances still running at the end of the hour. Create next EndOfHourEvent.
    '''
    def __init__(self, event_time: datetime) -> None:
        super().__init__(event_time)

    def __str__(self) -> str:
        return f"{self.time}: EndOfHourEvent"
