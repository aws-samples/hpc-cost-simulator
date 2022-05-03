# Altair Accelerator Log Parsing

The `AcceleratorLogParser.py` script queries the Accelerator results database, parses the job completion datap, and saves it into a CSV file.


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

## Setup

## Notes

Set up paths for nc.

```
source /tools/altr/2019.01u7/common/etc/vovrc.sh
```

NC uses postgres.

Get a list of tables:

```
nc cmd vovsql_query -e "select * from pg_catalog.pg_tables where schemaname != 'pg_catalog' and schemaname != 'information_schema'"

 public metadata rtdamgr {} t f f f
 public loadinfo rtdamgr {} t f f f
 public users rtdamgr {} t f t f
 public hosts rtdamgr {} t f t f
 public jobclasses rtdamgr {} t f t f
 public projects rtdamgr {} t f t f
 public osgroups rtdamgr {} t f t f
 public fsgroups rtdamgr {} t f t f
 public tools rtdamgr {} t f t f
 public resources rtdamgr {} t f t f
 public grabbeds rtdamgr {} t f t f
 public statuses rtdamgr {} t f t f
 public jobs rtdamgr {} t f t f
 pg_temp_2 import_jobs_1916 rtdamgr {} t f f f
 pg_temp_2 import_jobs_normalized_1916 rtdamgr {} t f f f
```

Get list of columns in the table:

```
nc cmd vovsql_query -e "select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = 'jobs'"

 id integer
 loadinfo_id integer
 jobclassid integer
 projectid integer
 fsgroupid integer
 userid integer
 osgroupid integer
 toolid integer
 resourcesid integer
 grabbedid integer
 exehostid integer
 statusid integer
 spriority smallint
 submittime integer
 starttime integer
 endtime integer
 deadline integer
 exitstatus smallint
 maxram integer
 maxvm integer
 cputime real
 susptime integer
 failcode integer
 fstokens integer
```

Get columns we need for analysis:

```
nc cmd vovsql_query -e "select id, submittime, starttime, endtime, exitstatus, maxram, maxvm, cputime, susptime from jobs"
```

Get list of columns in the resources table:

```
nc cmd vovsql_query -e "select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = 'resources'"

 id integer
 name {character varying}
```

Get list of resources

```
nc cmd vovsql_query -e "select id, name from resources"
```

Get list of columns in the users table:

```
nc cmd vovsql_query -e "select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = 'users'"

 id integer
 name {character varying}
```

Get list of users in the users table:

```
nc cmd vovsql_query -e "select id, name from users"

 id integer
 name {character varying}
```
