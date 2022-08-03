## Configuration 

All configuration parameters are stored in [config.yml](https://github.com/aws-samples/hpc-cost-simulator/blob/main/config.yml).
The schema of the configuration file is contained in [config_schema.yml](https://github.com/aws-samples/hpc-cost-simulator/blob/main/config_schema.yml)
and contains a list of all the options.
See comments within the files for details of each parameter's use.

Key configuration parameters that you may want to change include.

| Parameter | Default | Description |
|-----------|---------|-------------|
| instance_mapping: region_name | eu-west-1| The region where the AWS instances will run. Since instance availability and prices vary by region, this has direct impact on costs|
| instance_mapping: instance_prefix_list | [c6i, m5., r5., c5., z1d, x2i] | Instance types to be used during the analysis |
| consumption_model_mapping: maximum_minutes_for_spot | 60 | Threshold for using on-demand instances instead of spot instances. |
| consumption_model_mapping: ec2_savings_plan_duration | 3 | Duration of the savings plan. Valid values: [1, 3] |
| consumption_model_mapping: ec2_savings_plan_payment_option | 'All Upfront' | Payment option. Valid values: ['All Upfront', 'Partial Upfront', 'No Upfront'] |
| consumption_model_mapping: | 3 | Duration of the savings plan. Valid values: [1, 3] |
| consumption_model_mapping: | 'All Upfront' | Payment option. Valid values: ['All Upfront', 'Partial Upfront', 'No Upfront'] | 