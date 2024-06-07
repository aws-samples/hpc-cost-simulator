# JsonLogParser

The [JsonLogParser.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/JsonLogParser.py) script parses job information from a JSON file that contains an array of dicts that contain the required job information.
This allows you to run the Job Analyzer on jobs from an unsupported scheduler as long as you can export the job information in the required JSON format.
The parser also requires you to provide a schema mapping file that maps the field names in your json file to the field names used
by the job analyzer.

## JSON Jobs File Format

The expected JSON format looks like the following example.

```
[
    {
        "Job_Id": "1",
        "ncpus": "1",
        "mem_bytes": 13843545600,
        "node_count": 1,
        "ctime": "2024-06-28T01:00:00",
        "stime": "2024-06-28T01:00:00",
        "walltime": "2024-06-28T01:00:00",
        "etime": "2024-06-28T01:00:00",
        "queue": "regress",
        "project": "project1",
        "Exit_status": "0",
        "ru_mem_bytes": 7334404096
    },
    {
        "Job_Id": "2",
        "ncpus": "1",
        "mem_bytes": 13843545600,
        "node_count": 1,
        "ctime": "2024-06-28T01:00:00",
        "stime": "2024-06-28T01:00:00",
        "walltime": "2024-06-28T01:00:00",
        "etime": "2024-06-28T01:00:00",
        "queue": "regress",
        "project": "project1",
        "Exit_status": "0",
        "ru_mem_bytes": 7334404096
    }
]
```

The field names aren't fixed, but must be the same for each job record.

## JSON Schema Map File Format

The schema map is a JSON file that maps the standard field names used by the parser to field names of your JSON file.
It looks like the following and must contain the following required fields.
For numeric fields like max_mem_gb, you can specify the units and the number will be scaled.
For example if you log contains memory request in bytes, then you can specify that and the number will be converted to GB.

```
    "job_id": "Job_Id",
    "num_cores": "ncpus",
    "max_mem_gb": {"mem_bytes": {"units": "b"}},
    "num_hosts": "node_count",
    "submit_time": "ctime",
    "start_time": "stime",

    "run_time": "walltime",

    "eligible_time": "etime",
    "queue": "queue",
    "project": "project",

    "exit_status": "Exit_status",

    "ru_maxrss": "ru_mem_bytes"
```

## Parsing the accounting JSON log files

First you must source the setup script to make sure that all required packages are installed.

```
source setup.sh
```

This creates a python virtual environment and activates it.

The script will parse a JSON file that contains the job information.
The parsed data will be written in the output directory which will be created if it does not exist.
The parsed job data will be written out in csv format.

```
./JsonLogParser.py \
    --input-json <json-jobs-file> \
    --json-schema <json-schema-file>
    --output-csv jobs.csv
```

## Full Syntax
```
usage: JsonLogParser.py [-h] --input-json INPUT_JSON --json-schema-map
                        JSON_SCHEMA_MAP [--output-csv OUTPUT_CSV]
                        [--starttime STARTTIME] [--endtime ENDTIME]
                        [--disable-version-check] [--debug]

Parse JSON file with job results.

optional arguments:
  -h, --help            show this help message and exit
  --input-json INPUT_JSON
                        Json file with parsed job info. (default: None)
  --json-schema-map JSON_SCHEMA_MAP
                        Json file that maps input json field names to
                        SchedulerJobInfo field names. (default: None)
  --output-csv OUTPUT_CSV
                        CSV file where parsed jobs will be written. (default:
                        None)
  --starttime STARTTIME
                        Select jobs after the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --endtime ENDTIME     Select jobs before the specified time. Format YYYY-MM-
                        DDTHH:MM:SS (default: None)
  --disable-version-check
                        Disable git version check (default: False)
  --debug, -d           Enable debug mode (default: False)
```

## What's next?

Once completed, you can run step #3 (cost simulation) by following the instructions in for the [JobAnalyzer](JobAnalyzer.md)
Since you already generated a CSV file, you will use [JobAnalyzer](JobAnalyzer.md) with the `csv` option.
