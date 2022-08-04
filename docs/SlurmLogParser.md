# Slurm

[SlurmLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/SlurmLogParser.py) parses the Slurm Accounting Database records, and its output is then used by [JobAnalyzer.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/JobAnalyzer.py) for cost simulation.

## Modes of running the tool
[SlurmLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/SlurmLogParser.py) has 2 modes of operation, online and offline.

### Offline - Recommended

In offline mode, the job completion data is extracted using `sacct` ahead of processing (not as part of the script execution). This allows processing to happen on a Linux machine not configured as part of the Slurm cluster (non production machine).

When running in this mode, provide the file path to HCS using the  `--sacct-input-file` argument.


### Online

Online means HCS will call the `sacct` command directly, which requires the Linux machine to have `sacct` installed and configured as well as the permissions to access accounting records from **all users**.

Running in this mode will store the output of the `sacct` command in a CSV file to allow additional analysis to be done offline (minimizing calls the Slurm headnode). You need to provide the name of this CSV file using the `--sacct-output-file` argument. 

If the Slurm `bin` folder is not in your path, you will also need to provide the `--slurm-root` argumarnt, pointing to the Slurm `bin` folder.

**Note**: You can't use both `--sacct-input-file` and `--sacct-output-file` together.

## Collecting the data
To see data collection command syntax, run:
```
source setup.sh
./SlurmLogParser.py --show-data-collection-cmd
```

As of this writing, the command is:
```
sacct --allusers --parsable2 --noheader --format State,JobID,ReqCPUS,ReqMem,ReqNodes,Constraints,Submit,Eligible,Start,Elapsed,Suspended,End,ExitCode,DerivedExitCode,AllocNodes,NCPUS,MaxDiskRead,MaxDiskWrite,MaxPages,MaxRSS,MaxVMSize,CPUTime,UserCPU,SystemCPU,TotalCPU,Partition --starttime 1970-01-01T0:00:00 > SlurmAccounting.csv
```

## Parsing the Job Completion Data
First you must source the setup script to activate the virtual environment and install all the dependedncies in it.

```
source setup.sh
./SlurmLogParser.py \
    --sacct-input-file SlurmAccounting.csv \
    --output-csv jobs.csv
```

## Full Syntax
```
usage: SlurmLogParser.py [-h]
                         (--show-data-collection-cmd | --sacct-output-file SACCT_OUTPUT_FILE | --sacct-input-file SACCT_INPUT_FILE)
                         [--output-csv OUTPUT_CSV] [--slurm-root SLURM_ROOT]
                         [--starttime STARTTIME] [--endtime ENDTIME] [--debug]

Parse Slurm logs.

optional arguments:
  -h, --help            show this help message and exit
  --show-data-collection-cmd
                        Show the command to create SACCT_OUTPUT_FILE.
                        (default: False)
  --sacct-output-file SACCT_OUTPUT_FILE
                        File where the output of sacct will be written. Cannot
                        be used with --sacct-file. Required if --sacct-input-
                        file not set. (default: None)
  --sacct-input-file SACCT_INPUT_FILE
                        File with the output of sacct so can process sacct
                        output offline. Cannot be used with --sacct-output-
                        file. Required if --sacct-output-file not set.
                        (default: None)
  --output-csv OUTPUT_CSV
                        CSV file with parsed job completion records (default:
                        None)
  --slurm-root SLURM_ROOT
                        Directory that contains the Slurm bin directory.
                        (default: None)
  --starttime STARTTIME
                        Select jobs after the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --endtime ENDTIME     Select jobs before the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --debug, -d           Enable debug mode (default: False)
```


## What's next?

Once completed, you can run step #3 (cost simulation) by following the instructions in for the [JobAnalyzer](JobAnalyzer.md).
Since you already generated a CSV file, you will use [JobAnalyzer](JobAnalyzer.md) with the `csv` option.
