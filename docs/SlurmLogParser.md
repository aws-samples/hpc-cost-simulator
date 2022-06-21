# Slurm Log Parser

The [SlurmLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/SlurmLogParser.py) script calls Slurm `sacct`, parses the job completion data, and generates a CSV file that can be used by [JobAnalyzer.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/JobAnalyzer.py).
It has 2 modes of operation, online and offline.

## Online

Online means the tool will call the `sacct` command directly, which requires the Linux machine running the tool to have `sacct` installed and configured as well as the permissions to access accounting records from all users.
When running in this mode, the output of `sacct` will be stored to a CSV file defined by the `--sacct-output-file` parameter.

## Offline

If running this tool on a Linux machine not connected to the Slurm head-node, you can first extract the `sacct` output to a CSV file and then run the tool offline to analyze it.
When running in this mode, the tool expects an existing file, defined by the `--sacct-input-file` argument, that contains the output of `saact`.

**Note**: You can't use both `--sacct-input-file` and `--sacct-output-file` together.

## Usage

```
usage: SlurmLogParser.py [-h] [--slurm-root SLURM_ROOT]
                         (--sacct-output-file SACCT_OUTPUT_FILE | --sacct-input-file SACCT_INPUT_FILE)
                         --output-csv OUTPUT_CSV [--debug]

Parse Slurm logs.

optional arguments:
  -h, --help            show this help message and exit
  --slurm-root SLURM_ROOT
                        Directory that contains the Slurm bin directory.
                        (default: None)
  --sacct-output-file SACCT_OUTPUT_FILE
                        File where the output of sacct will be written. Cannot
                        be used with --sacct-file. Required if --sacct-input-
                        file not set. (default: None)
  --sacct-input-file SACCT_INPUT_FILE
                        File with the output of sacct so can process sacct
                        output offline. Cannot be used with --sacct-output-
                        file. Required if --sacct-output-file not set. (default: None)

  --output-csv OUTPUT_CSV
                        CSV file with parsed job completion records (default: None)
  --debug, -d           Enable debug mode (default: False)
```

## Parsing the Job Completion Data

First you must source the setup script to make sure that all required packages are installed.

```
source setup.sh
```
This creates a python virtual environment and activates it.

The script calls `sacct` so your environment should be set up so that the Slurm scripts are in the path.
Otherwise you can pass the path to the scripts usingn the `--slurm-root` argument which should point to the
path that contains the Slurm `bin` directory.

The parsed data will be written in the `output` directory which will be created if it does not exist.

```
./SlurmLogParser.py \
    --sacct-output-file output/sacct-output.txt \
    --output-csv output/jobs.csv
```
