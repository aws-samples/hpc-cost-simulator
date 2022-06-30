
Reproduces [Bug #13: Issue where instances do not have spot pricing](https://github.com/aws-samples/hpc-cost-simulator/issues/13)

The hpc6a instance type doesn't have spot pricing.

Test a config in eu-west-1 that only has hpc6a and test that JobAnalyzer.py fails because no instance types selected.

Create a config in us-east-2 with short spot jobs and only hpc6a instance types.
The analyzer should use on-demand pricing when spot pricing doesn't exist.
