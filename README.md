# HPC Cost Simulator

This package contains scripts to parse scheduler logs and analyze them.
It analyzes scheduler accounting log files to simulate the cost of running the same jobs on AWS.
This tool can simulate the use of spot instances for short jobs and help guide the choice of savings plans by breaking the data to hourly consumption.
Initially supports LSF and Slurm. Altair Accelerator support to be added soon.

Currently supported schedulers are:

* Altair Accelerator/RapidScaling
* IBM Spectrum LSF
* Slurm

## Documentation

You can [view the docs](https://aws-samples.github.io/hpc-cost-simulator/) on Github pages.

If you are editing the docs in your repository and want to view them locally then you can use mkdocs to serve them.

To see the docs run:

```
mkdocs serve
```

To view the code documentation:

```
pdoc -d google *.py
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
