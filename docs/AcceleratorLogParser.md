# Altair Accelerator Log Parsing

The [AcceleratorLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/AcceleratorLogParser.py) script queries the Accelerator results database, parses the job completion data, and saves it into a CSV file.


## Usage

```
usage: AcceleratorLogParser.py [-h]
                               (--sql-output-file SQL_OUTPUT_FILE | --sql-input-file SQL_INPUT_FILE)
                               --output-csv OUTPUT_CSV [--debug]

Parse Altair Accelerator logs.

optional arguments:
  -h, --help            show this help message and exit
  --sql-output-file SQL_OUTPUT_FILE
                        File where the output of sql query will be written.
                        Cannot be used with --sql-input-file. Required if
                        --sql-input-file not set. Command to create file: nc
                        cmd vovsql_query -e "select id, submittime, starttime,
                        endtime, exitstatus, maxram, maxvm, cputime, susptime
                        from jobs" > SQL_OUTPUT_FILE (default: None)
  --sql-input-file SQL_INPUT_FILE
                        File with the output of sql query so can process it
                        offline. Cannot be used with --sql-output-file.
                        Required if --sql-output-file not set. (default: None)
  --output-csv OUTPUT_CSV
                        CSV file with parsed job completion records (default:
                        None)
  --debug, -d           Enable debug mode (default: False)
```

## Parsing the Job Completion Data

First you must source the setup script to make sure that all required packages are installed.

```
source setup.sh
```

This creates a python virtual environment and activates it.

The script calls `nc cmd vovsql_query` so your environment should be set up so that the nc scripts are in the path.
For example:

```
source /tools/altr/2019.01u7/common/etc/vovrc.sh
```

The parsed data will be written in the output directory which will be created if it does not exist.

```
./AcceleratorLogParser.py \
    --sql-output-file output/sql-output.txt \
    --output-csv output/jobs.csv
```
