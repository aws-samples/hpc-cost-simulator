# HPC Cost Simulator (HCS)

HCS performs an hourly cost simulation for existing High Performance Computing (HPC) workloads by analyzing past job records (scheduler accounting database).

Supported schedulers: 

* IBM LSF 
* SchedMD Slurm
* Altair Accelerator

The output allows customers to:
1. Estimate their Amazon Elastic Compute Cloud (Amazon Ec2) costs hour-by-hour
2. Choose the optimal consumption model to minimize costs, including [Savings Plans](https://aws.amazon.com/savingsplans/), [Reserved Instances](https://aws.amazon.com/ec2/pricing/reserved-instances/) and [Spot Instaces](https://aws.amazon.com/ec2/spot/) 
3. Get statistics about their workloads, and which workloads exerience long queue time (wait time), reducing engineering productivity.

## Data privacy
HCS avoids the need to share scheduler logs, which include business sensitive data (tape out dates, project names) and PII (user names). 
Instead, customers run HCS on premises, creating an anonymized output that may be shared with your AWS account team to get additional guidance.

The output file (located under the `output/` directory if not otherwise specified) is a simple Excel (.xslx), allosing you to audit its contents before sharing it.


## How data is analyzed
The analysis is a 3-step process:

| # | Step            | Description                                                                                                                                                     |
|   |-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | Data Collection | Collect accounting records from the database.                                                                                                                   |
| 2 | Formatting      | Parsing the scheduler accounting database - this takes the scheudler specific format, and converts the job records to a uniform format (not scheduler specific).|
| 3 | Cost Simulation | Performing cost simulation using the output from step 2.                                                                                                        |
| 4 | Analysis        | Analyzing the output, building a complete cost model. This can be done using your AWS account team.                                                             | 


## Prerequisits

* Python 3.6 or newer
* sudo access to install any required tools
* Recommended: CentOS 7 or higher

A bash setup script is provided to install the required yum packages, create a Python virtual environment with all required Python packages, and activate the virtual environment.
The pip packages and versions are listed in [requirements.txt](https://github.com/aws-samples/hpc-cost-simulator/blob/main/requirements.txt).
The setup script must be sourced, not executed, to set up the environment to run the tool.

```
source setup.sh
```

## Configuration
HCS comes preconfigured with a few best practices for analyzing an Electronics Design Automation (EDA) workload. You can edit the configuration by editing the `config.yml` file.
We encourage you to read the file before running to tool, to know what parameters are used to simulate the workload.

A **partial** list of these configuration parameters includes:

- AWS region
- Instance selection guidelines
- Savings Plans selection guidelines
- Spot selection guidelines

## Instance Type Information

To perform cost simulation, HCS requires a list of available instance types in the region and their prices that is stored in [instance_type_info.json](https://github.com/aws-samples/hpc-cost-simulator/blob/main/instance_type_info.json).
HCS comes pre-populated with the databased, and we priodically update it to reflect new instance types, prices etc. 
However, to update it youself , see [Updating Instance Type Information](UpdateInstanceDatabase.md)


# Running the Tool

## Steps #1 #2  
Both steps are scheduler specific and are explained in each scheduler's documentation page:

- [IBM LSF](LSFLogParser.md)
- [SchedMD Slurm](SlurmLogParser.md)
- [Altair Engineering Accelerator](AcceleratorLogParser.md)

Once completed, you can run step #3 (cost simulation) by bollowing the instructions in for the [JobAnalyzer](JobAnalyzer.md)
For analysis (step #4) you can either share the Excel file from step #3 with your AWS account team, or read the [Output](Output.md) page for more details.

**Notes for step #3**

1. The job analyzer can call the parser, however, parsing is the longest step so it is recommended that it be run separately from the job analyzer.
2. The job analyzer runs relatively quickly and can be run with different configuration file (`config.yml`) on the same job data. For example: simulating your costs with different instance families.

