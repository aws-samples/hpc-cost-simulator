# Altair Accelerator

The [AcceleratorLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/AcceleratorLogParser.py) script queries the Accelerator results database, parses the job completion data, and saves it into a CSV file.

## Video walkthrough
The fasterest way to learn about using HPC Cost Simulator with Altair's Accelerator (NC) is to watch this short walkthrough.

{% include youtube.html id="pEsDqFWrabs" %}


## Collecting the data
To see data collection command syntax, run:
```
source setup.sh
./AcceleratorLogParser.py --show-data-collection-cmd
```
For example, as of this writing, the command is:
```
nc cmd vovsql_query -e "select jobs.id, jobs.submittime, jobs.starttime, jobs.endtime, resources.name, jobs.exitstatus, jobs.maxram, jobs.maxvm, jobs.cputime, jobs.susptime from jobs inner join resources on jobs.resourcesid=resources.id" > sql-output.txt
```

This command should run on a node connected to the Altair Accelerator head node, and with a user with permissions to see the jobs from all users.

**Note:** To be able to run the command, you first need to source the `vovrc.sh` script located in your `/common/etc` subfolder of your Altair install folder.

## Parsing the Job Completion Data

The parsed data will be written in the output directory which will be created if it does not exist.

```
./AcceleratorLogParser.py \
    --sql-output-file sql-output.txt \
    --output-csv output/jobs.csv
```

## Full Syntax
```
usage: AcceleratorLogParser.py [-h]
                               (--show-data-collection-cmd | --sql-output-file SQL_OUTPUT_FILE | --sql-input-file SQL_INPUT_FILE)
                               [--output-csv OUTPUT_CSV]
                               [--default-mem-gb DEFAULT_MEM_GB]
                               [--starttime STARTTIME] [--endtime ENDTIME]
                               [--debug]

Parse Altair Accelerator logs.

optional arguments:
  -h, --help            show this help message and exit
  --show-data-collection-cmd
                        Show the command to create the SQL_OUTPUT_FILE.
                        (default: False)
  --sql-output-file SQL_OUTPUT_FILE
                        File where the output of sql query will be written.
                        Cannot be used with --sql-input-file. Required if
                        --sql-input-file not set. (default: None)
  --sql-input-file SQL_INPUT_FILE
                        File with the output of sql query so can process it
                        offline. Cannot be used with --sql-output-file.
                        Required if --sql-output-file not set. (default: None)
  --output-csv OUTPUT_CSV
                        CSV file with parsed job completion records (default:
                        None)
  --default-mem-gb DEFAULT_MEM_GB
                        Default amount of memory (in GB) requested for jobs
                        that do not have a memory request. (default: 0.0)
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