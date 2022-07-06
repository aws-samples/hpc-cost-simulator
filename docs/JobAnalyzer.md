# Job Analyzer

The [JobAnalyzer.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/JobAnalyzer.py) script post-processes the output of the parsers to provide an analysis of running the jobs on AWS.
For convenience, the analyzer can call the parser and analyze the output in 1 step.

## Prerequisites

[JobAnalyzer.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/JobAnalyzer.py) relies on  [instance_type_info.json](https://github.com/aws-samples/hpc-cost-simulator/blob/main/instance_type_info.json) which contains instance type details and pricing.
If the file doesn't exist in the same directory as the script then it will attempt to create it using the AWS API.
For details on the required IAM permissions see the [main documentation page](index.md#instance-type-information).

## Configuration

All configuration parameters are stored in [config.yml](https://github.com/aws-samples/hpc-cost-simulator/blob/main/config.yml).
The schema of the configuration file is contained in [config_schema.yml](https://github.com/aws-samples/hpc-cost-simulator/blob/main/config_schema.yml)
and contains a list of all the options.
See comments within the files for details of each parameter's use.

Key configuration parameters that you may want to change include.

| Parameter | Default | Description |
|-----------|---------|-------------|
| instance_mapping: region_name | eu-west-1| The region where the AWS instances will run. |
| instance_mapping: instance_prefix_list | [c6i, m5., r5., c5., z1d, x2i] | Instance types to be used during the analysis |
| consumption_model_mapping: maximum_minutes_for_spot | 60 | Threshold for using on-demand instances instead of spot instances.|
| consumption_model_mapping: ec2_savings_plan_duration | 3 | Duration of the savings plan. Valid values: [1, 3] |
| consumption_model_mapping: ec2_savings_plan_payment_option | 'All Upfront' | Payment option. Valid values: ['All Upfront', 'Partial Upfront', 'No Upfront'] |
| consumption_model_mapping: | 3 | Duration of the savings plan. Valid values: [1, 3] |
| consumption_model_mapping: | 'All Upfront' | Payment option. Valid values: ['All Upfront', 'Partial Upfront', 'No Upfront'] |

## Usage

Arguments provided to `JobAnalyzer` are required in a specific order:

```
    ./JobAnalyzer.py <Arguments that apply to all schedulers> <Parser type> <Parser-specific Arguments>
```

These are the common arguments.

```
usage: JobAnalyzer.py [-h] [--starttime STARTTIME] [--endtime ENDTIME]
                      [--config CONFIG] [--output-dir OUTPUT_DIR]
                      [--output-csv OUTPUT_CSV] [--debug]
                      parser ...

Analyze jobs

positional arguments:
  parser                Choose the kind of information to parse.
                        ./JobAnalyzer.py <parser> -h for parser specific
                        arguments.
    accelerator         Parse Accelerator (nc) job information
    csv                 Parse CSV from already parsed job information.
    lsf                 Parse LSF logfiles
    slurm               Parse Slurm job information

optional arguments:
  -h, --help            show this help message and exit
  --starttime STARTTIME
                        Select jobs after the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --endtime ENDTIME     Select jobs before the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --config CONFIG       Configuration file. (default: ./config.yml)
  --output-dir OUTPUT_DIR
                        Directory where output will be written (default:
                        output)
  --output-csv OUTPUT_CSV
                        CSV file with parsed job completion records (default:
                        None)
  --debug, -d           Enable debug mode (default: False)
```

### Arguments that apply to all schedulers

When used, these parameters must precede the parser type:

```
  --starttime STARTTIME     Select jobs after the specified time. Format YYYY-MM-DDTHH:MM:SS (default: None)
  --endtime ENDTIME         Select jobs before the specified time. Format YYYY-MM-DDTHH:MM:SS (default: None)
  --config CONFIG           Configuration file. (default: ./config.yml)
  --output-dir OUTPUT_DIR   Directory where output will be written (default: output)
  --output-csv OUTPUT_CSV   CSV file with parsed job completion records (default: None)
  --debug, -d               Enable debug mode (default: False)
  --help, -h                Show help message
```

###  Parser Type

The tool supports 4 parser types:

```
    accelerator         Parse Accelerator (nc) job information
    lsf                 Parse LSF accounting ercords (lsb.acct fiels)
    slurm               Parse Slurm job information
    csv                 Parse CSV from a previously parsed job information.
```

### Parser-specific Arguments - Accelerator

```
usage: JobAnalyzer.py accelerator [-h] [--default-mem-gb DEFAULT_MEM_GB]
                                  (--sql-output-file SQL_OUTPUT_FILE | --sql-input-file SQL_INPUT_FILE)

optional arguments:
  -h, --help            show this help message and exit
  --default-mem-gb DEFAULT_MEM_GB
                        Default amount of memory (in GB) requested for jobs.
                        (default: 0.098)
  --sql-output-file SQL_OUTPUT_FILE
                        File where the output of sql query will be written.
                        Cannot be used with --sql-input-file. Required if
                        --sql-input-file not set. Command to create file: nc
                        cmd vovsql_query -e "select jobs.id, jobs.submittime,
                        jobs.starttime, jobs.endtime, resources.name,
                        jobs.exitstatus, jobs.maxram, jobs.maxvm,
                        jobs.cputime, jobs.susptime from jobs inner join
                        resources on jobs.resourcesid=resources.id" >
                        SQL_OUTPUT_FILE (default: None)
  --sql-input-file SQL_INPUT_FILE
                        File with the output of sql query so can process it
                        offline. Cannot be used with --sql-output-file.
                        Required if --sql-output-file not set. (default: None)
```

### Parser-specific Arguments - LSF

```
usage: JobAnalyzer.py lsf [-h] [--logfile-dir LOGFILE_DIR]
                          --default-max-mem-gb DEFAULT_MAX_MEM_GB

optional arguments:
  -h, --help            show this help message and exit
  --logfile-dir LOGFILE_DIR
                        LSF logfile directory (default: None)
  --default-max-mem-gb DEFAULT_MAX_MEM_GB
                        Default maximum memory for a job in GB. (default:
                        None)
```

### Parser-specific Arguments - Slurm

```
usage: JobAnalyzer.py slurm [-h] [--slurm-root SLURM_ROOT]
                            [--sacct-output-file SACCT_OUTPUT_FILE | --sacct-input-file SACCT_INPUT_FILE]

optional arguments:
  -h, --help            show this help message and exit
  --slurm-root SLURM_ROOT
                        Directory that contains the Slurm bin directory.
                        (default: None)
  --sacct-output-file SACCT_OUTPUT_FILE
                        File where the output of sacct will be written. Cannot
                        be used with --sacct-input-file. Required if --sacct-
                        input-file not set. (default: None)
  --sacct-input-file SACCT_INPUT_FILE
                        File with the output of sacct so can process sacct
                        output offline. Cannot be used with --sacct-output-
                        file. Required if --sacct-output-file not set.
                        (default: None)
```

Note: The tool will call `sacct` to get accounting logs, if you don't have it installed on the machine, please see the [SlurmLogParser.py documentation](SlurmLogParser.md) for details on how to save them to a CSV file.

### Analyzing Pre-Parsed CSV Job Data

```
usage: JobAnalyzer.py csv [-h] --input-csv INPUT_CSV

optional arguments:
  -h, --help            show this help message and exit
  --input-csv INPUT_CSV
                        CSV file with parsed job info from scheduler parser.
                        (default: None)
```

## Data Used

The parser parses out and saves the minimum amount of data required to do the analysis.
The fields are documented in the [SchedulerJobInfo.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/SchedulerJobInfo.py) module.
The intent is only to store timestamp information, the number of cores and amount of memory requested by each job,
and actual resource usage information, if available such as the user time, system time, and max memory used.

## Jobs CSV File Format

The format out the CSV files that the parsers write is flexible and JobAnalyzer.py can use any CSV file that containes the required information.
This can be useful if you are using a different scheduler or storing the job information in something like Splunk.
As long as you can export the required data into a CSV file, then JobAnalyzer.py can parse it.

The order of the fields in the CSV file do not matter, but the names of the fields do.
There is no standard for CSV files, but the JobAnalyzer.py expects the files to be written in the "Microsoft Excel" dialect.
The field names must be in the first row.

The required fields are:

| Field | Type | Description
|-------|------|-------------
| job_id | string | Job id
| num_cores | int | Total number of cores allocated to the job
| max_mem_gb | float | Total amount of memory allocated to the job in GB
| num_hosts | int |Number of hosts. In Slurm this is the number of nodes. The number of cores should be evenly divisible by the number of hosts. For LSF this is typically 1.
| submit_time | datetime string | Time that the job was submitted. All times are expected to be in the format *YYYY*-*MM*-*DD*T*hh*:*mm*:*ss*
| start_time  | datetime string | Time that the job started.
| finish_time | datetime string | Time that the job finished.

The optional fields are:

| Field | Type | Description
|-------|------|-------------
| resource_request | string | Resources requested by the job. For LSF this is the effective resource request. For Slurm it is the job constraints.
| ineligible_pend_time | timedelta string | Duration that the job was ineligible to run. This is what LSF writes. Defaults to max(0, eligible_time - start_time). Format for timedelta strings is *h*:*mm*:*ss*.
| eligible_time | datetime string | Time that the job became eligible to run. Defaults to the start_time + ineligible_pend_time.
| requeue_time | datetime string | Time that the job was requeued. Currently not used.
| wait_time | timedelta string | Time that job was pending after it was eligible to run. Default start_time - eligible_time.
| run_time | timedelta string | Time that the job ran. Default: finish_time - start_time
| exit_status | int | Effective return code of the job. Default: 0
| ru_majflt  | float | Number of page faults
| ru_maxrss  | float | Maximum shared text size
| ru_minflt  | float | Number of page reclaims
| ru_msgsnd  | float | Number of System V IPC messages sent
| ru_msgrcv  | float | Number of messages received
| ru_nswap   | float | Number of times the process was swapped out
| ru_inblock | float | Number of block input operations
| ru_oublock | float | Number of block output operations
| ru_stime   | float | System time used
| ru_utime   | float | User time used

## Savings Plan Optimization

The script writes an Excel Workbook that allows you to view the analysis and analyze the impact of savings plan on the overall costs.
Excel also has a Solver add-in that will automatically optimize the savings plan values to minimize the costs.
The steps to configure the solver are included in the spreadsheet and are on the [main page](index.md#optimize-savings-plans-using-excel-solver).
