# IBM Spectrum LSF Parser

The `LSFLogParser.py` script parses the `lsb.acct*` files in the LSF logfile directory and writes the parsed data into a CSV file that can be read by `JobAnalyzer.py`.

## Usage

```
usage: LSFLogParser.py [-h] --logfile-dir LOGFILE_DIR --output-csv OUTPUT_CSV
                       [--debug]

Parse LSF logs.

optional arguments:
  -h, --help            show this help message and exit
  --logfile-dir LOGFILE_DIR
                        LSF logfile directory (default: None)
  --output-csv OUTPUT_CSV
                        CSV file with parsed job completion records (default:
                        None)
  --debug, -d           Enable debug mode (default: False)
```

## Parsing the Log Files

First you must source the setup script to make sure that all required packages are installed.

```
source setup.sh
```

This creates a python virtual environment and activates it.

The script will parse all of the files that start with `lsb.acct*` in the logfile directory.
The parsed data will be written in the output directory which will be created if it does not exist.
The parsed job data can be written out in csv, yaml, or pickle format.
The recommended format is csv because it is relatively compact, human readable, and auditable to ensure it does not contain undesired information.

```
./LSFLogParser.py \
    --logfile-dir <logfile-dir> \
    --output-csv output/jobs.csv
```
