# Job Analyzer

The [JobAnalyzer.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/JobAnalyzer.py) performs the cost simulation using the outputs from 
[AcceleratorLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/AcceleratorLogParser.py), 
[LSFLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/LSFLogParser.py) or 
[SlurmLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/SlurmLogParser.py) 

It produces an hour-by-hour cost simulation, placing the output in the `output/` subfolder (by default).

For convenience, the analyzer can call the parser and analyze the output in 1 step, however we recommend calling perfoming the analysis in separate stages (see "How data is analyzed") in [index.md](index.md).

## Prerequisites

[JobAnalyzer.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/JobAnalyzer.py) relies on:

1. the virtual environment created by the setup script. run `source setup.sh` to setup the virtual environment.
2. `config.yml` which defines the configuration of the analysis. 
For more detials on the configuration file, see the [configuration documentation](config.md)
3. [instance_type_info.json](https://github.com/aws-samples/hpc-cost-simulator/blob/main/instance_type_info.json) which contains instance type details and pricing.
The file is part of the repository, but if you want to download an update list of instances and their prices, please see [Updating the Instance Type Information](UpdateInstanceDatabase.md)

# Performing the cost simulation
Once you generated a CSV file from your Scheduler (See instructions for [IBM LSF](LSFLogParser.md), [SchedMD Slurm](SlurmLogParser.md) abd [Altair Engineering Accelerator](AcceleratorLogParser.md)) you can perform the cost simulation (step 3) using [JobAnalyzer.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/JobAnalyzer.py)

To parse the CSV file into the final anonymized Excel report, run:
```
source setup.sh
./JobAnalyzer.py csv --input-csv INPUT_CSV_FILE_NAME
```

## Outputs

By default, HCS places all output files in the `output/` folder (this can be changed usign the `--output-dir` parameter to `JobAnalyzer.py`). 
**Note:** The output folder will get overwritten without prompting you for approval.

## Full Syntax
Arguments provided to `JobAnalyzer` are required in a specific order:

```
    ./JobAnalyzer.py <Arguments that apply to all schedulers> <Parser type> <Parser-specific Arguments>
```

These are the common arguments.

```
usage: JobAnalyzer.py [-h] [--starttime STARTTIME] [--endtime ENDTIME]
                      [--config CONFIG] [--acknowledge-config]
                      [--output-dir OUTPUT_DIR] [--output-csv OUTPUT_CSV]
                      [--debug]
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
    hourly_stats        Parse hourly_stats file so can create Excel workbook
                        (xlsx).

optional arguments:
  -h, --help            show this help message and exit
  --starttime STARTTIME
                        Select jobs after the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --endtime ENDTIME     Select jobs before the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --config CONFIG       Configuration file. (default: ./config.yml)
  --acknowledge-config  Acknowledge configuration file contents so don't get
                        prompt. (default: False)
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

The tool supports 5 parser types:

```
    accelerator         Parse Accelerator (nc) job information
    lsf                 Parse LSF accounting ercords (lsb.acct fiels)
    slurm               Parse Slurm job information
    csv                 Parse CSV from a previously parsed job information.
    hourly_stats        Parse the hourly output files from a previous run
```

### Parser-Specific Arguments - Accelerator

```
usage: JobAnalyzer.py accelerator [-h] [--default-mem-gb DEFAULT_MEM_GB]
                                  (--sql-output-file SQL_OUTPUT_FILE | --sql-input-file SQL_INPUT_FILE)

optional arguments:
  -h, --help            show this help message and exit
  --default-mem-gb DEFAULT_MEM_GB
                        Default amount of memory (in GB) requested for jobs.
                        (default: 0.0)
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

### Parser-Specific Arguments - LSF

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

### Parser-Specific Arguments - Slurm

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


## What's Next?
Once completed, you can find your Excel report under `output/hourly_stats.xlsx`. You can share it with your AWS acocunt team (Recommended) for further cost optimization guidance, or you can learn more about the data in the Excel in the [Output](Output.md) documentation page.