'''
Utilities for dealing with memory sizes.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import logging
import re

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.propagate = False
logger.setLevel(logging.INFO)

MEM_KB = 1024
MEM_MB = MEM_KB*1024
MEM_GB = MEM_MB*1024
MEM_TB = MEM_GB*1024
MEM_PB = MEM_TB*1024

MEM_SUFFIX = {
    'K': MEM_KB,
    'M': MEM_MB,
    'G': MEM_GB,
    'T': MEM_TB,
    'P': MEM_PB,
}

def mem_string_to_float(string_value: str) -> float:
    logger.debug(f"mem_string_to_float({string_value})")
    if not string_value:
        raise ValueError("Empty string cannot be converted to float")
    match = re.match(r'(^[0-9.e-]+)([kmgtp])$', string_value, re.IGNORECASE)
    if match:
        value = float(match.group(1))
        suffix = match.group(2).upper()
        multiplier = MEM_SUFFIX[suffix]
        logger.debug(f"value={value}")
        logger.debug(f"suffix={suffix}")
        logger.debug(f"multiplier={multiplier}")
        value = value * multiplier
    else:
        value = float(string_value)
        logger.debug(f"value={value}")
    return value

def mem_string_to_int(string_value: str) -> int:
    logger.debug(f"mem_string_to_int({string_value})")
    if not string_value:
        raise ValueError("Empty string cannot be converted to int")
    return int(round(mem_string_to_float(string_value)))
