# IBM Spectrum LSF

The [LSFLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/LSFLogParser.py) script parses the `lsb.acct*` files in the LSF `logfile` directory and writes the parsed data into a CSV file that can be read by [JobAnalyzer.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/JobAnalyzer.py).


## Video walkthrough
The fastest way to learn about using HPC Cost Simulator with IMB LSF is to watch this short walkthrough.

[![Video walkthrough - IBM LSF](https://img.youtube.com/vi/7rv6vUD_B_c/0.jpg)](http://www.youtube.com/watch?v=7rv6vUD_B_c "HPC Cost Simulator - IBM LSF")

## Parsing the accounting log files

First you must source the setup script to make sure that all required packages are installed.

```
source setup.sh
```

This creates a python virtual environment and activates it.

The script will parse all of the files that start with `lsb.acct*` in the logfile directory.
The parsed data will be written in the output directory which will be created if it does not exist.
The parsed job data will be written out in csv format.

```
./LSFLogParser.py \
    --logfile-dir <logfile-dir> \
    --output-csv jobs.csv
```

**Note:** The parser expects the file names to **start** with `lsb.acct`, do not rename the files.

## Full Syntax
```
usage: LSFLogParser.py [-h] --logfile-dir LOGFILE_DIR --output-csv OUTPUT_CSV
                       [--default-max-mem-gb DEFAULT_MAX_MEM_GB]
                       [--starttime STARTTIME] [--endtime ENDTIME] [--debug]

Parse LSF logs.

optional arguments:
  -h, --help            show this help message and exit
  --logfile-dir LOGFILE_DIR
                        LSF logfile directory (default: None)
  --output-csv OUTPUT_CSV
                        CSV file with parsed job completion records (default:
                        None)
  --default-max-mem-gb DEFAULT_MAX_MEM_GB
                        Default maximum memory for a job in GB. (default: 0.0)
  --starttime STARTTIME
                        Select jobs after the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --endtime ENDTIME     Select jobs before the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --debug, -d           Enable debug mode (default: False)
```

## What's next?

Once completed, you can run step #3 (cost simulation) by following the instructions in for the [JobAnalyzer](JobAnalyzer.md)
Since you already generated a CSV file, you will use [JobAnalyzer](JobAnalyzer.md) with the `csv` option.