#!/usr/bin/env python3

import argparse
import logging

logger = logging.getLogger(__file__)
logger_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
logger_streamHandler = logging.StreamHandler()
logger_streamHandler.setFormatter(logger_formatter)
logger.addHandler(logger_streamHandler)
logger.setLevel(logging.INFO)

def main():
        parser = argparse.ArgumentParser(description="Analyze jobs")
        parser.add_argument("-n", required=True, type=int, help="Number of records to generate.")
        parser.add_argument("--output-file", required=True, type=str, help="Output file")
        parser.add_argument("--debug", '-d', action='store_const', const=True, default=False, help="Enable debug mode")
        args = parser.parse_args()

        if args.debug:
            logger.setLevel(logging.DEBUG)

        valid_record = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'

        with open(args.output_file, 'w') as fh:
            for idx in range(args.n):
                fh.write(f"{valid_record}\n")

if __name__ == '__main__':
    main()
