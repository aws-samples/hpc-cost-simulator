'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

import csv
import filecmp
from LSB_ACCT_FIELDS import LSB_ACCT_RECORD_FORMATS
from psutil import virtual_memory, swap_memory
from os import makedirs, path, remove, system
from os.path import dirname, realpath
from LSFLogParser import LSFLogParser
from MemoryUtils import MEM_GB, MEM_KB, MEM_MB
import psutil
import pytest
from SchedulerJobInfo import SchedulerJobInfo
import subprocess
from subprocess import CalledProcessError, check_output
from tempfile import NamedTemporaryFile
from test_CSVLogParser import order as last_order

order = last_order // 100 * 100 + 100
assert order == 300

class TestLSFLogParser:
    global order

    testdir = dirname(realpath(__file__))
    repodir = realpath(f"{testdir}/..")
    default_max_mem_gb = (100 * MEM_MB) / MEM_GB
    lsfLogParser = f"{repodir}/LSFLogParser.py"

    def cleanup_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    def parse_line_with_csv_reader(self, line: str) -> [str]:
        tmp_fh = NamedTemporaryFile(mode='w', delete=False)
        tmp_filename = tmp_fh.name
        tmp_fh.write(line)
        tmp_fh.close()
        tmp_fh = open(tmp_filename, 'r')
        csv_reader = csv.reader(tmp_fh, delimiter=' ')
        fields = next(csv_reader)
        tmp_fh.close()
        remove(tmp_filename)
        return fields

    order += 1
    @pytest.mark.order(order)
    def test_parse_record_fields(self):
        test_files_dir = 'test_files/LSFLogParser/bad-records'
        output_dir = 'output/LSFLogParser/bad-records'
        output_csv = path.join(output_dir, 'jobs.csv')
        parser = LSFLogParser(test_files_dir, output_csv, self.default_max_mem_gb)

        invalid_record_type_record = '"INVALID" "10.108" 1644826628 387 1501 33554450 2 1644826611 0 0 1644826612 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/tools" "" "/tools/output/100.txt" "" "1644826611.387" 02 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/tools/100m.py" 0.168266 0.044691 112800 0 -1 0 0 27278 4 0 936 32 -1 0 0 0 87 2 -1 "" "default" 0 2 "" "" 0 108544 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 6160 "" 1644826612 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 86016 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 16 "/tools" 0 "" 0.000000 0.00 0.00 0.00 0.00 2 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(invalid_record_type_record)
        with pytest.raises(ValueError) as excinfo:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        print(excinfo)
        print(excinfo.value)
        assert(str(excinfo.value).startswith('Invalid record type: INVALID'))

        invalid_record_type_record = '"INVALID "10.108" 1644826628 387 1501 33554450 2 1644826611 0 0 1644826612 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/tools" "" "/tools/output/100.txt" "" "1644826611.387" 02 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/tools/100m.py" 0.168266 0.044691 112800 0 -1 0 0 27278 4 0 936 32 -1 0 0 0 87 2 -1 "" "default" 0 2 "" "" 0 108544 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 6160 "" 1644826612 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 86016 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 16 "/tools" 0 "" 0.000000 0.00 0.00 0.00 0.00 2 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(invalid_record_type_record)
        with pytest.raises(ValueError) as excinfo:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        print(excinfo)
        print(excinfo.value)
        assert(str(excinfo.value).startswith('Invalid record type: INVALID '))

        record_type_missing_trailing_quote = '"JOB_FINISH "10.108" 1644826628 387 1501 33554450 2 1644826611 0 0 1644826612 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/tools" "" "/tools/output/100.txt" "" "1644826611.387" 02 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/tools/100m.py" 0.168266 0.044691 112800 0 -1 0 0 27278 4 0 936 32 -1 0 0 0 87 2 -1 "" "default" 0 2 "" "" 0 108544 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 6160 "" 1644826612 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 86016 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 16 "/tools" 0 "" 0.000000 0.00 0.00 0.00 0.00 2 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(record_type_missing_trailing_quote)
        with pytest.raises(ValueError) as excinfo:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        print(excinfo)
        print(excinfo.value)
        assert(str(excinfo.value).startswith('Invalid record type: JOB_FINISH '))

        bad_int = '"JOB_FINISH" "10.108" "not an int" 387 1501 33554450 2 1644826611 0 0 1644826612 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/tools" "" "/tools/output/100.txt" "" "1644826611.387" 02 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/tools/100m.py" 0.168266 0.044691 112800 0 -1 0 0 27278 4 0 936 32 -1 0 0 0 87 2 -1 "" "default" 0 2 "" "" 0 108544 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 6160 "" 1644826612 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 86016 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 16 "/tools" 0 "" 0.000000 0.00 0.00 0.00 0.00 2 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(bad_int)
        with pytest.raises(ValueError) as excinfo:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        print(excinfo)
        print(excinfo.value)
        assert(str(excinfo.value) == 'Event Time(%d)=not an int is not an int')

        bad_float = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997abc 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(bad_float)
        with pytest.raises(ValueError) as excinfo:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        print(excinfo)
        print(excinfo.value)
        assert(str(excinfo.value) == 'ru_utime(%f)=0.004997abc is not a float')

        missing_field_numAllocSlots = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00' # 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_numAllocSlots)
        with pytest.raises(ValueError) as excinfo:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        print(excinfo)
        print(excinfo.value)
        assert(str(excinfo.value) == 'Not enough fields to get value for numAllocSlots.')

        missing_field_allocSlots = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1' # "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_allocSlots)
        with pytest.raises(ValueError) as excinfo:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        print(excinfo)
        print(excinfo.value)
        assert(str(excinfo.value) == 'Not enough fields to get value for numAllocSlots.')

        missing_field_ineligiblePendTime = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal"' # -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_ineligiblePendTime)
        try:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise

        missing_field_indexRangeCnt = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1' # 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_indexRangeCnt)
        try:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise

        missing_field_requeueTime = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0' # 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_requeueTime)
        try:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise

        missing_field_numGPURusages = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0' # 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_numGPURusages)
        try:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise

        missing_field_storageInfoC = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0' # 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_storageInfoC)
        try:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise

        missing_field_numKVP = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0' # 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_numKVP)
        try:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise

        missing_field_KVP_key = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1' # "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_KVP_key)
        try:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise

        missing_field_KVP_value = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead"' # "0.00"'
        fields = self.parse_line_with_csv_reader(missing_field_KVP_value)
        try:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise

        extra_field = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00" "extra field"'
        fields = self.parse_line_with_csv_reader(extra_field)
        with pytest.raises(ValueError) as excinfo:
            parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        print(excinfo)
        print(excinfo.value)
        assert(str(excinfo.value) == "1 extra fields left over: 'extra field'")

        valid_record_type_record = '"JOB_FINISH" "10.108" 1644826549 386 1501 33554434 1 1644826545 0 0 1644826546 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/root" "" "" "" "1644826545.386" 0 1 "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/bin/sleep 3" 0.004997 0.006155 3748 0 -1 0 0 436 4 0 928 0 -1 0 0 0 19 2 -1 "" "default" 0 1 "" "" 0 2048 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644826546 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 1024 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 3 "/root" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(valid_record_type_record)
        record = parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        assert(record['record_type'] == 'JOB_FINISH')

        valid_record_type_record = '"JOB_FINISH" "10.108" 1644826628 387 1501 33554450 2 1644826611 0 0 1644826612 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/tools" "" "/tools/output/100.txt" "" "1644826611.387" 02 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" 64 100.0 "" "/tools/100m.py" 0.168266 0.044691 112800 0 -1 0 0 27278 4 0 936 32 -1 0 0 0 87 2 -1 "" "default" 0 2 "" "" 0 108544 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 6160 "" 1644826612 "" "" 5 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 86016 "select[type == any] order[r15s:pg] " "" -1 "" -1 0 "" 0 0 "" 16 "/tools" 0 "" 0.000000 0.00 0.00 0.00 0.00 2 "ip-10-30-66-253.eu-west-1.compute.internal" "ip-10-30-66-253.eu-west-1.compute.internal" -1 0 0 0 0 1 "schedulingOverhead" "0.00"'
        fields = self.parse_line_with_csv_reader(valid_record_type_record)
        record = parser._parse_record_fields(fields, LSB_ACCT_RECORD_FORMATS)
        assert(record['record_type'] == 'JOB_FINISH')

    order += 1
    @pytest.mark.order(order)
    def test_missing_args(self):
        self.cleanup_files()
        with pytest.raises(CalledProcessError) as excinfo:
            check_output([self.lsfLogParser], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert('the following arguments are required: --logfile-dir, --output-csv' in excinfo.value.output)
        assert(excinfo.value.returncode == 2)

    order += 1
    @pytest.mark.order(order)
    def test_missing_logfile_dir(self):
        self.cleanup_files()
        output_csv = 'jobs.csv'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output([self.lsfLogParser, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb)], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert('the following arguments are required: --logfile-dir' in excinfo.value.output)
        assert(excinfo.value.returncode == 2)

    order += 1
    @pytest.mark.order(order)
    def test_missing_output_csv(self):
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/acct'
        with pytest.raises(CalledProcessError) as excinfo:
            check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--default-max-mem-gb', str(self.default_max_mem_gb)], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert('the following arguments are required: --output-csv' in excinfo.value.output)
        assert(excinfo.value.returncode == 2)

    order += 1
    @pytest.mark.order(order)
    def test_main_acct_empty_output_dir(self):
        '''
        Reproduces https://gitlab.aws.dev/cartalla/schedulerloganalyzer/-/issues/3

        If the output-csv doesn't include a path then the output dir is empty and makedirs fails.
        '''
        self.cleanup_files()
        test_files_dir = path.join(self.repodir, 'test_files/LSFLogParser/acct')
        output_dir = 'output/LSFLogParser/acct'
        output_csv = 'jobs.csv'
        makedirs(output_dir)
        try:
            check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb), '-d'], cwd=output_dir, stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        expected_results_dir = 'test_files/LSFLogParser'
        assert(filecmp.cmp(path.join(output_dir, output_csv), path.join(expected_results_dir, 'exp_jobs.csv'), shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_not_enough_fields(self):
        '''
        Test for fields starting with storageInfoC missing.

        During testing with customers we saw that the fields were truncated starting with the storageInfoC field.
        Since this and following fields are not used just ignore the error and return the correctly parsed fields.
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/not-enough-fields'
        output_dir = 'output/LSFLogParser/not-enough-fields'
        output_csv = path.join(output_dir, 'jobs.csv')
        try:
            check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb)], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_hostFactor_is_not_a_float(self):
        '''
        Test for issue 9: Bad record: hostFactor(%f)=sj074 is not a float

        This turned out to be handling of numExHosts
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/issues/gl-9'
        output_dir = 'output/LSFLogParser/issues/gl-9'
        output_csv = path.join(output_dir, 'jobs.csv')
        try:
            check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb)], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_issue_gl_16(self):
        '''
        Test for issue gitlab 16
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/issues/gl-16'
        output_dir = 'output/LSFLogParser/issues/gl-16'
        output_csv = path.join(output_dir, 'jobs.csv')
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        try:
            output = check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb), '--debug'], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_issue_gl_19(self):
        '''
        Test for gitlab issue 19
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/issues/gl-19'
        output_dir = 'output/LSFLogParser/issues/gl-19'
        output_csv = path.join(output_dir, 'jobs.csv')
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        with pytest.raises(CalledProcessError) as excinfo:
            output = check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb)], stderr=subprocess.STDOUT, encoding='utf8')
            print(f"output:\n{output}")
        print(f"returncode: {excinfo.value.returncode}")
        print(f"output:\n{excinfo.value.output}")
        assert(excinfo.value.returncode == 1)
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))
        assert('Unsupported logfile format version 9.13.' in excinfo.value.output)

    order += 1
    @pytest.mark.order(order)
    def test_main_issue_gl_22(self):
        '''
        Test for gitlab issue 22
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/issues/gl-22'
        output_dir = 'output/LSFLogParser/issues/gl-22'
        output_csv = path.join(output_dir, 'jobs.csv')
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        try:
            output = check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb)], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_issue_gl_26(self):
        '''
        Test for gitlab issue 26
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/issues/gl-26'
        output_dir = 'output/LSFLogParser/issues/26'
        output_csv = path.join(output_dir, 'jobs.csv')
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        with pytest.raises(CalledProcessError) as excinfo:
            output = check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb)], stderr=subprocess.STDOUT, encoding='utf8')
            print(f"output:\n{output}")
        print(f"returncode: {excinfo.value.returncode}")
        print(f"output:\n{excinfo.value.output}")
        assert(excinfo.value.returncode == 1)
        assert('6 invalid records were found in 1 files' in excinfo.value.output)
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_issue_16(self):
        '''
        Test for github issue 16
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/issues/16'
        output_dir = 'output/LSFLogParser/issues/16'
        output_csv = path.join(output_dir, 'jobs.csv')
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        try:
            output = check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb), '-d'], stderr=subprocess.STDOUT, encoding='utf8')
            print(f"output:\n{output}")
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_issue_18(self):
        '''
        Test for github issue 18
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/issues/18'
        output_dir = 'output/LSFLogParser/issues/18'
        output_csv = path.join(output_dir, 'jobs.csv')
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        try:
            output = check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb), '-d'], stderr=subprocess.STDOUT, encoding='utf8')
            print(f"output:\n{output}")
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_issue_31_no_start_time(self):
        '''
        Test for github issue 31
        '''
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/issues/31'
        output_dir = 'output/LSFLogParser/issues/31'
        output_csv = path.join(output_dir, 'jobs.csv')
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        try:
            output = check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb), '-d'], stderr=subprocess.STDOUT, encoding='utf8')
            print(f"output:\n{output}")
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main_acct(self):
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser/acct'
        output_dir = 'output/LSFLogParser/acct'
        output_csv = path.join(output_dir, 'jobs.csv')
        try:
            check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb)], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        expected_results_dir = 'test_files/LSFLogParser'
        assert(filecmp.cmp(output_csv, path.join(expected_results_dir, 'exp_jobs.csv'), shallow=False))

    order += 1
    @pytest.mark.order(order)
    def test_main(self):
        self.cleanup_files()
        test_files_dir = 'test_files/LSFLogParser'
        output_dir = 'output/LSFLogParser'
        output_csv = path.join(output_dir, 'jobs.csv')
        try:
            check_output([self.lsfLogParser, '--logfile-dir', test_files_dir, '--output-csv', output_csv, '--default-max-mem-gb', str(self.default_max_mem_gb)], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"return code: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        test_files_dir = 'test_files/LSFLogParser'
        assert(filecmp.cmp(path.join(output_dir, 'jobs.csv'), path.join(test_files_dir, 'exp_jobs.csv'), shallow=False))

    def gen_lsb_acct(self, filename, number_of_tests):
        with open(filename, 'w') as lsb_acct_fh:
            job_id = 1
            for idx in range(number_of_tests):
                lsb_acct_fh.write(f'"JOB_FINISH" "10.108" 1644132419 {job_id} 1501 33554434 1 1644132138 0 0 1644132402 "simuser" "sender" "" "" "" "ip-10-30-14-225.eu-west-1.compute.internal" "/tools" "" "" "" "1644132138.385" 0 1 "ip-172-30-68-135.eu-west-1.compute.internal@lsf1" 64 1.0 "" "#!/usr/bin/python; import sys;import time; count = 600; megabyte = (0,) * (1024 * 1024 / 8); data = megabyte * count; for i in range(10):;    time.sleep(1)" 0.730425 0.136584 619912 0 -1 0 0 4340 4 0 928 16 -1 0 0 0 37 10 -1 "" "default" 0 1 "" "" 0 619520 0 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 5136 "" 1644132402 "" "" 6 1058 "1" 1110 "default" 1041 "jfincache" 1086 "-1 -1 -1 -1 100 -1 -1 -1 -1 -1 -1 -1 " 1032 "0" 1033 "0" 0 -1 4194304 463872 "select[(aws) && (type == any)] order[r15s:pg] " "" -1 "lsf1" 247 1644132349 "" 0 0 "" 17 "/tools" 0 "" 0.000000 0.00 0.00 0.00 0.00 1 "ip-172-30-68-135.eu-west-1.compute.internal@lsf1" -1 0 0 0 0 1 "schedulingOverhead" "0.00"\n')
                job_id += 1

    def stress_LSFLogParser(self, number_of_tests):
        self.cleanup_files()
        output_dir = 'output/LSFLogParser/scaling'
        lsb_acct_filename = path.join(output_dir, f'lsb.acct-{number_of_tests}')
        output_csv = path.join(output_dir, f'jobs-{number_of_tests}.csv')
        makedirs(output_dir)
        self.gen_lsb_acct(lsb_acct_filename, number_of_tests)
        try:
            output = check_output(['python', '-m', 'cProfile', '-s', 'cumtime', './LSFLogParser.py', '--default-max-mem-gb', '0', '--logfile-dir', output_dir, '--output-csv', output_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.output}")
            raise
        print(f"output:\n{output}")

    # order += 1
    # @pytest.mark.order(order)
    # def test_LSFLogParser_1k(self):
    #     ''''
    #     Analyze 1,000 jobs to see what memory utilization and run time does.

    #     real    0m7.325s
    #     user    0m4.279s
    #     sys     0m2.528s

    #     1365 jobs/s
    #     '''
    #     self.stress_LSFLogParser(1000)

    # order += 1
    # @pytest.mark.order(order)
    # def test_LSFLogParser_10k(self):
    #     ''''
    #     Analyze 1,000 jobs to see what memory utilization and run time does.
    #     '''
    #     self.stress_LSFLogParser(10000)
    #     assert(False)

    # order += 1
    # @pytest.mark.order(order)
    # def test_LSFLogParser_100k(self):
    #     ''''
    #     Analyze 1,000 jobs to see what memory utilization and run time does.

    #     Original: 1m9.059s
    #     Latest  : 0m42.337s
    #     '''
    #     self.stress_LSFLogParser(100000)
    #     assert(False)
