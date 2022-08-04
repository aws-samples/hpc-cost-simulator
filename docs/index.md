# HPC Cost Simulator (HCS)

HPC Cost Simulator (HCS) enables custoemrs to simulate their High Performance Computing (HPC) costs on AWS.
To simulate your future costs, HCS analyzes past job records (scheduler accounting database) and performs hour-by-hour cost analysis.

Supported schedulers: 

* IBM LSF 
* SchedMD Slurm
* Altair Accelerator

The output allows customers to:

1. Estimate their Amazon Elastic Compute Cloud (Amazon EC2) costs hour-by-hour
2. Choose the optimal consumption model to minimize costs, including [Savings Plans](https://aws.amazon.com/savingsplans/), [Reserved Instances](https://aws.amazon.com/ec2/pricing/reserved-instances/) and [Spot Instaces](https://aws.amazon.com/ec2/spot/) 
3. Get statistics about their workloads, locating specific job configurations that are waiting in queue longer, reducing engineering productivity.

## Data privacy
HCS avoids the need to share scheduler logs with AWS. Scheduler logs include business sensitive data (tape out dates, project names) and PII (user names). 
Instead, customers run HCS on premises, creating an anonymized output that may be shared with your AWS account team to get additional cost optimizaiton guidance.

The output file (located under the `output/` directory if not otherwise specified) is a simple Excel (.xslx), allowing you to audit its contents before sharing it.


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

A bash setup script is provided to install the required packages and create a Python virtual environment. The pip packages and versions are listed in [requirements.txt](https://github.com/aws-samples/hpc-cost-simulator/blob/main/requirements.txt).
The setup script must be sourced, not executed, to set up the environment to run the tool.

```
source setup.sh
```

## Configuration
HCS comes preconfigured with a few best practices for analyzing an Electronics Design Automation (EDA) workload. You can edit the configuration by editing `config.yml` before running HCS. We encourage you to read the file before running to tool, to know what parameters are used to simulate the workload.

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

## Steps 1 & 2

These steps are scheduler-specific and are explained in each scheduler's documentation page:

- [IBM LSF](LSFLogParser.md)
- [SchedMD Slurm](SlurmLogParser.md)
- [Altair Engineering Accelerator](AcceleratorLogParser.md)

If you want to parse data from an unsupported scheduler, you can export the accounting data from that scheudler generating your own CSV, as long as you CSV structure defined in [Understanding the CSV format](UnderstandingCSVformat.md)

## Step 3

Once completed, you can run step #3 (cost simulation) by following the instructions in for the [JobAnalyzer](JobAnalyzer.md)


**Notes for step #3**

1. The job analyzer can call the parser (step 2), however, parsing is the longest step so it is recommended that you run it separately.
2. The job analyzer can be run with different configuration file (`config.yml`) on the same job data. 
For example: 
 - Simulating your workload costs with and without Spot Instaces
 - Simulating your costs with different instance families

## Step 4

For analysis, you can either share the Excel file from step #3 with your AWS account team, or read the [Output](Output.md) page for more details.
