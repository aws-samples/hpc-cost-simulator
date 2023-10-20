#!/usr/bin/env python3

import argparse
import json
import logging
from sys import exit

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(levelname)s:%(asctime)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Compare instance_type_info.json files.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("file1", type=str, help="Original file")
    parser.add_argument("file2", type=str, help="New file")
    parser.add_argument("--debug", "-d", action='store_const', const=True, default=False, help="Enable debug messages")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    instance_type_and_family_info1 = json.loads(open(args.file1, 'r').read())
    instance_type_and_family_info2 = json.loads(open(args.file2, 'r').read())

    changes = {}
    regions1 = instance_type_and_family_info1.keys()
    regions2 = instance_type_and_family_info2.keys()
    deleted_regions = []
    for region1 in regions1:
        if region1 not in regions2:
            deleted_regions.append(region1)
    if deleted_regions:
        changes['Deleted regions'] = new_regions
    new_regions = []
    for region2 in regions2:
        if region2 not in regions1:
            new_regions.append(region2)
    if new_regions:
        changes['New regions'] = new_regions

    # Check for deleted/new instance families in existing regions
    for region in regions1:
        if region not in regions2:
            continue
        logger.debug(f"{region}:")

        deleted_instance_families = []
        for instance_family1 in instance_type_and_family_info1[region]['instance_families']:
            if instance_family1 not in instance_type_and_family_info2[region]['instance_families']:
                deleted_instance_families.append(instance_family1)
                logger.debug(f"    Deleted {instance_family1}")

        new_instance_families = []
        for instance_family2 in instance_type_and_family_info2[region]['instance_families']:
            if instance_family2 not in instance_type_and_family_info1[region]['instance_families']:
                new_instance_families.append(instance_family2)
                logger.debug(f"    Added {instance_family2}")

        deleted_instance_types = []
        for instance_type1 in instance_type_and_family_info1[region]['instance_types']:
            (instance_family1, instance_size1) = instance_type1.split('.')
            if instance_family1 in deleted_instance_families:
                continue
            if instance_type1 not in instance_type_and_family_info2[region]['instance_types']:
                deleted_instance_types.append(instance_type1)
                logger.debug(f"    Deleted {instance_type1}: {instance_family1} {instance_size1}")

        new_instance_types = []
        for instance_type2 in instance_type_and_family_info2[region]['instance_types']:
            (instance_family2, instance_size2) = instance_type2.split('.')
            if instance_family2 in new_instance_families:
                continue
            if instance_type2 not in instance_type_and_family_info1[region]['instance_types']:
                new_instance_types.append(instance_type2)
                logger.debug(f"    Added {instance_type2}: {instance_family2} {instance_size2}")

        if deleted_instance_families or new_instance_families or deleted_instance_types or new_instance_types:
            if region not in changes:
                changes[region] = {}

        if deleted_instance_families:
            changes[region]['Deleted instance families'] = deleted_instance_families

        if new_instance_families:
            changes[region]['New instance families'] = new_instance_families

        if deleted_instance_types:
            changes[region]['Deleted instance types'] = deleted_instance_types

        if new_instance_types:
            changes[region]['New instance types'] = new_instance_types

    print(f"Changes:\n{json.dumps(changes, indent=4)}")
