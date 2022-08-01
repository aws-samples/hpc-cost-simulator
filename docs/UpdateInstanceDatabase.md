# Updating the Instance Type Information

While we update the HCS Instance Type Information with each new release, you can update it yourself to get updates instance types and prices for one region or all regions.
The Instance Type Information is stored in JSON format in  [instance_type_info.json](../instance_type_info.json)

**Note**: Running the script without specifying a version will collect the data for all instance types across all available regions, which may take a few minutes to complete.


## Prerequisites

To be able to update the EC2 Instance Type Information you need:
1. AWS CLI installed
2. AWS CLI Profile with the permissions shown below

### Required AWS permissions 

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GetEC2InstanceTypeInfo",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeInstanceTypes",
                "ec2:DescribeRegions",
                "ec2:DescribeSpotPriceHistory",
                "pricing:GetProducts"
            ],
            "Resource": "*"
        }
    ]
}
```


## Running the update

Once your source `setup.sh` to setup the Virtual Environment, you can run  [get_instance_type_info.py](https://github.com/aws-samples/hpc-cost-simulator/blob/main/EC2InstanceTypeInfoPkg/get_ec2_instance_info.py) to update the Instance Database:

```
source setup.sh
rm instance_type_info.json
./get_ec2_instance_info.py --input instance_type_info.json --region REGION_NAME
```

**Note:** If the script fails to complete for all regions, you can restart it without losing the data already collected by rerunning it with the `--input` parameter and providing the partial output from the previous run (`instance_type_info.json`)


### Usage:

'''
usage: get_ec2_instance_info.py [-h] [--region REGION] [--input INPUT]
                                [--output-csv OUTPUT_CSV] [--debug]

Get EC2 instance pricing info.

optional arguments:
  -h, --help            show this help message and exit
  --region REGION, -r REGION
                        AWS region(s) to get info for. (default: [])
  --input INPUT, -i INPUT
                        JSON input file. Reads existing info from previous
                        runs. Can speed up rerun if a region failed. (default:
                        None)
  --output-csv OUTPUT_CSV, -o OUTPUT_CSV
                        CSV output file. Default: instance_type_info.csv
                        (default: None)
  --debug, -d           Enable debug messages (default: False)
'''




