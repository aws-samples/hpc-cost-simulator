# Model How Jobs Would Be Scheduled on AWS

Once you have parsed the scheduler logs, you can model how the same jobs might run on a scalable AWS compute cluster.
The ModelComputeCluster.py script will model how the jobs would run on an ideal AWS cluster with no scaling limitations.
The script models the jobs starting at the point that they are eligible to start and models the instances required
to run those jobs.
The output of the model is the total compute costs for running the jobs and the amount of wait time saved by running
on AWS.

Initially, the model will not have any instances running.
The jobs will be sorted by their eligible time and when that time arrives the scheduler will allocate instances for the job.
The resource allocation strategy is to pick the least expensive instance that has the required number of cores and memory available.
If no instance is found, then a new instance will be started in the model for the job to run on.
The model will start the least expensive instance type that the job can run on.
When the model adds a new instance it is not available to run jobs for a configurable bootup time (default 2 minutes).
When an instance is idle for longer than a configurable idle time (default 5 minutes), then it will removed from the model.

The model tracks the costs for bootup and idle times and the time spent running jobs.
The model also reports hourly spot and on-demand costs.

Since an instance can run multiple jobs at the same time, we have to decide whether costs need to be allocated to individual jobs.
This is unnecessary since all we are concerned with are the overall compute costs and do not break out job costs at the end.
If we did want to allocate job costs it is complicated because some jobs may be core limited, others memory limited, and there are boot and
idle times that have to be allocated.
An instance's cost is allocated to a job based on the percentage of cores and memory used by the job relative to the total percentage of allocated cores and memory.
Since jobs start and stop at different times, the cost of a job will vary over time based on the jobs that are running concurrently with it.
For example, if a job is the only job running for the first 10 minutes, then it is charged the full cost of the instance for those 10 minutes.
If another job starts with the same resource requirements, then 50% of the instance costs will be allocated to each job while the jobs
are running concurrently.
The jobs' costs are recalculated each time a job starts or finishes on the instance.

The percentage of the instance's resources that are used by multiple jobs is not straightforward because jobs can be core limited or memory limited.
If cores are the limiting resource, then memory will be underutilized.
If memory is the limiting resource, then cores will be underutilized.
So, the first step in allocating instance costs is to determine the limiting resource by calculating the percentage of cores and memory allocated to all the jobs running on the instance and picking the resource with the higher allocation.
For example, if all of the cores are allocated then the costs will be allocated to each job based on the percentage of allocated cores that it is using.
If 90% of the memory is allocated and only 25% of the cores, then memory is considered the limiting resource and the costs will be allocated to a job based on the percentage of total allocated memory that it is using.

The model can also be configured with reserved instances (RIs).
RIs in the model will always be available to run jobs.

The model can also be configured with on-premises servers.
The configuration must include an inventory of the server types with the number of cores, amount of memory, and hourly cost.

The model can also use spot pricing on the instances that it uses.
You can configure the profile of jobs that will utilize spot based on the runtime of the job.
For example, you can configure the model to use spot pricing instead of on-demand pricing for all jobs
that run for less than 5 hours.
The model will also randomly terminate spot jobs and requeue any jobs that were running on the instance when it was terminated.
The model has a spot termination probability per minute that you can configure.
Note that this is for modeling purposes only and that AWS provides no SLA for spot termination rates.
The actual termination probability can vary significantly based on availability in the spot pools which can vary significantly over time.
The default probability is chosen so that a job that runs for 5 hours has a 5% chance of being terminated which in line with real world experiences, although, again, this may differ markedly in real life.

## Model Configuration

```
ComputeClusterModel:
  BootTime: '2:00'
  IdleTime: '4:00'
  SchedulingFrequency: '0:00'
  ShareInstances: False
  ReservedInstances:
    - InstanceType: Count
```

### Configure On-Premises Servers (Optional)

### Model RIs (Optional)

## Design

### Main

* Sort all jobs by eligible_time
* Create ComputeClusterModel
* while get_job()
  * if job.eligible_time > current_time:
    * computeClusterModel.advance_time(job.eligible_time)
  * computeClusterModel.schedule_job(job)

### ComputeClusterModel

* List of ComputeInstance sorted by cost. Group by PricingOption and InstanceType? For same cost sorted reverse by Utilization
* HourlyCost: Update when instance terminated or at the end of an hour. Cost at termination is only the cost since the previous hour.
  * Save Spot and OnDemand costs separately
  * Save number of Spot and OnDemand core hours
  * Save OnDemand costs split out by instance family
* Event list sorted by datetime
  * JobFinishEvent: Includes the instance running the job.
  * InstanceTerminateEvent: Set when last job on instance terminates. Cancel instance termination if job has been allocated.
  * EndOfHourEvent: Update HourlyCost with cost of instances still running at the end of the hour. Create next EndOfHourEvent.
* advance_time(new_time: datetime): Process all events up to new_time
* schedule_job: allocate to existing instance or to a new instance. Create JobFinish event for the job. If new instance JobFinish event should include boot time.
* finish_job: Delete job from instance. If no more jobs then create TerminateInstance event.
* terminate_instance: If no jobs running then update HourlyCost and remove instance from list.

### ComputeInstance

* EC2/OnPrem
* InstanceType
* NumberOfCores
* ThreadsPerCore
* HTEnabled
* Memory (GB)
* PricingOption: OnDemand or Spot
* HourlyCost
* State: Booting, Running, Idle
* List of Jobs
* Available cores
* Available memory
* Core Utilization
* Memory Utilization
* Instance Utilization (Max of core/memory utilization)
