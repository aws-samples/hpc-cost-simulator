# HPC Cost Simulator

This package contains scripts to parse scheduler logs and analyze them.
It analyzes scheduler accounting log files to simulate the cost of running the same jobs on AWS.
This tool can simulate the use of spot instances for short jobs and help guide the choice of savings plans by breaking the data to hourly consumption.
Initially supports LSF and Slurm. Altair Accelerator support to be added soon.

Currently supported schedulers are:

* IBM Spectrum LSF
* Slurm

Support is planned for the following:

* Altair Accelerator/RapidScaling

## Documentation

To see the docs run:

```
mkdocs serve
```

To view the code documentation:

```
pdoc -d google *.py
```

## Data Used

The parser parses out and save the minimum amount of data required to do the analysis.
The fields are documented in the SchedulerJobInfo.py module.
The intent is only to store timestamp information, the number of cores and amount of memory requested by each job,
and actual resource usage information, if available such as the user time, system time, and max memory used.

## Analysis

The `JobAnalyzer.py` script generates an hourly_stats.csv file that contain the estimated hourly EC2 spend.

```
Relative Hour,Total OnDemand Costs, Total Spot Costs (80% off)
0,0,0.00020266666666666661
1,0,0.013862400000000006
1,0,0
3,0,0.006399200000000001
4,0,0.009611466666666669
5,0,5.066666666666667e-06
```

It also generates a summary.csv that has summary counts of the jobs by the amount of memory requested and the duration of the jobs.

```
Note: All memory sizes are in GB, all times are in MINUTES (not hours)

MemorySize,0-1 Minutes,<--,<--,1-5 Minutes,<--,<--,5-20 Minutes,<--,<--,20-60 Minutes,<--,<--,60-240 Minutes,<--,<--,240-1440 Minutes,<--,<--,1440-1000000 Minutes,<--,<--,
,Job count,Total duration,Total wait time,Job count,Total duration,Total wait time,Job count,Total duration,Total wait time,Job count,Total duration,Total wait time,Job count,Total duration,Total wait time,Job count,Total duration,Total wait time,Job count,Total duration,Total wait time,
0-1GB,56,4.3500000000000005,0.0,19,55.16666666666666,0.0,5,38.38333333333333,0.0,0,0,0,0,0,0,0,0,0,0,0,0,
1-2GB,6,1.7166666666666668,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
2-4GB,1,0.4,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
4-8GB,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
8-16GB,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
16-32GB,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
32-64GB,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
64-128GB,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
128-256GB,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
256-512GB,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
512-1000000GB,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
