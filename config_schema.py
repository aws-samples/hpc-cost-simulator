"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import re
from schema import Schema, And, Use, Optional, Regex, SchemaError

config = {}

config_schema = Schema(
    {
        Optional('version', default=1): int,
        'instance_mapping': {
            Optional('hyperthreading', default=False): bool,
            'region_name': str,
            'range_minimum': int,
            'range_maximum': int,
            'ram_ranges_GB': [
                int
            ],
            'runtime_ranges_minutes': [
                int
            ],
            'instance_prefix_list': [
                str
            ],
        },
        'consumption_model_mapping': {
            'minimum_cpu_speed': Use(float),
            'maximum_minutes_for_spot': int,
            'ec2_savings_plan_duration': And(int, lambda n: n in [1, 3]),
            'ec2_savings_plan_payment_option': And(str, lambda s: s in ['All Upfront', 'Partial Upfront', 'No Upfront']),
            'compute_savings_plan_duration': And(Use(int), lambda n: n in [1, 3]),
            'compute_savings_plan_payment_option': And(str, lambda s: s in ['All Upfront', 'Partial Upfront', 'No Upfront']),
            Optional('job_file_batch_size', default=1000): int,
        },
        Optional('Jobs', default={'QueueRegExps': [], 'ProjectRegExps': []}): {
            Optional('QueueRegExps', default=[]): [
                str
            ],
            Optional('ProjectRegExps', default=[]): [
                str
            ]
        },
        Optional('ComputeClusterModel'): {
            Optional('BootTime', default='2:00'): str,
            Optional('IdleTime', default='4:00'): str,
            Optional('SchedulingFrequency', default="2:00"): str, # Time between scheduling jobs
            Optional('ShareInstances', default=False): bool, # Allow multiple jobs per instance
            Optional('ReservedInstances', default=[]): [
                {
                    'InstanceType': int,
                }
            ]
        }
    }
)

def check_schema(config_in):
    # Validate config against schema
    global config
    config = config_in
    validated_config = config_schema.validate(config)
    return validated_config
