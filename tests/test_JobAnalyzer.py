'''
Test the JobAnalyzer.py module and script.

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''

from CSVLogParser import CSVLogParser
import filecmp
from JobAnalyzer import JobAnalyzer, JobCost
import json
import os
from os import environ, getenv, listdir, path, system
from os.path import abspath, dirname
import pytest
from SchedulerJobInfo import SchedulerJobInfo
import subprocess
from subprocess import CalledProcessError, check_output
import unittest

@pytest.mark.order(7, after=['tests/test_SortJobs.py::TestSortJobs::test_random'])
class TestJobAnalyzer(unittest.TestCase):

    def __init__(self, name):
        super().__init__(name)
        self._restore_instance_type_info()

    REPO_DIR = abspath(f"{dirname(__file__)}/..")

    CONFIG_FILENAME = path.join(REPO_DIR, 'test_files/JobAnalyzer/config.yml')

    OUTPUT_DIR = path.join(REPO_DIR, 'output/JobAnalyzer')

    INSTANCE_NAME_PATTERN = '\w\d\w*\.\d{0,2}\w+'

    INPUT_CSV = path.join(REPO_DIR, 'test_files/LSFLogParser/exp_jobs.csv')

    csv_parser = CSVLogParser(INPUT_CSV, None)

    region = 'eu-west-1'

    _jobAnalyzer = None

    def get_jobAnalyzer(self):
        if self._jobAnalyzer:
            return self._jobAnalyzer
        self._jobAnalyzer = JobAnalyzer(self.csv_parser, self.CONFIG_FILENAME, self.OUTPUT_DIR)
        return self._jobAnalyzer

    def _remove_instance_type_info(self):
        system(f"rm -f {dirname(__file__)+'/../instance_type_info.json'}")

    def _restore_instance_type_info(self):
        system(f"git restore {dirname(__file__)+'/../instance_type_info.json'}")

    def cleanup_output_files(self):
        system(f"rm -rf {dirname(__file__)+'/../output'}")

    def _remove_credentials(self):
        self.AWS_ACCESS_KEY_ID = getenv('AWS_ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = getenv('AWS_SECRET_ACCESS_KEY')
        self.AWS_SESSION_TOKEN = getenv('AWS_SESSION_TOKEN')
        if self.AWS_ACCESS_KEY_ID:
            del environ['AWS_ACCESS_KEY_ID']
        if self.AWS_SECRET_ACCESS_KEY:
            del environ['AWS_SECRET_ACCESS_KEY']
        if self.AWS_SESSION_TOKEN:
            del environ['AWS_SESSION_TOKEN']

    def _restore_credentials(self):
        if self.AWS_ACCESS_KEY_ID:
            environ['AWS_ACCESS_KEY_ID'] = self.AWS_ACCESS_KEY_ID
        if self.AWS_SECRET_ACCESS_KEY:
            environ['AWS_SECRET_ACCESS_KEY'] = self.AWS_SECRET_ACCESS_KEY
        if self.AWS_SESSION_TOKEN:
            environ['AWS_SESSION_TOKEN'] = self.AWS_SESSION_TOKEN

    def _get_hourly_files(self, dir):
        '''
        Gets the hourly output files for the current job

        Args:
            dir (str): output directory
        Returns:
            [str]: Sorted list of output filenames
        '''
        all_files = listdir(dir)
        output_files = []
        prefix = path.basename("hourly-")
        for file in all_files:
            if file.startswith(prefix) and file[-4:] == ".csv":
                output_file = file
                output_files.append(output_file)
        output_files.sort()
        return output_files

    def test_get_ranges(self):
        self.assertEqual(self.get_jobAnalyzer().get_ranges([1,2,3,4,5]),['0-1','1-2','2-3','3-4','4-5','5-'+str(self.get_jobAnalyzer().range_max)])
        self.assertEqual(self.get_jobAnalyzer().get_ranges([50]),['0-50','50-'+str(self.get_jobAnalyzer().range_max)])

    @pytest.mark.order(after='test_get_ranges')
    def test_read_configuration(self):
        # Test bad filename
        with pytest.raises(FileNotFoundError) as excinfo:
            config = JobAnalyzer.read_configuration(self.CONFIG_FILENAME + 'INVALID')
        print(excinfo.value)

        config = JobAnalyzer.read_configuration(self.CONFIG_FILENAME)
        key_dict = {'version':'',
            'instance_mapping':'',
            'consumption_model_mapping':''
        }

        self.assertEqual(key_dict.keys(), config.keys())
        self.assertGreaterEqual(len(config["instance_mapping"]["ram_ranges_GB"]), 2)
        self.assertGreaterEqual(len(config["instance_mapping"]["runtime_ranges_minutes"]), 2)
        self.assertTrue(type(config["instance_mapping"]["instance_prefix_list"]) == list)   # makes sures edits don't change to a string

    @pytest.mark.order(after='test_read_configuration')
    def test_select_range(self):
        jobAnalyzer = self.get_jobAnalyzer()

        self.assertEqual(jobAnalyzer.select_range(0,[1,5,10,20]),str(jobAnalyzer.range_min)+'-1')
        self.assertEqual(jobAnalyzer.select_range(5,[1,5,10,20]),'1-5')
        self.assertEqual(jobAnalyzer.select_range(14,[1,5,10,20]),'10-20')
        self.assertEqual(jobAnalyzer.select_range(25,[1,5,10,20]),'20-'+str(jobAnalyzer.range_max))

    @pytest.mark.order(after='test_select_range')
    def test_add_job_to_hourly_bucket(self):
        self.cleanup_output_files()
        jobAnalyzer = self.get_jobAnalyzer()
        jobAnalyzer._clear_job_stats

        # Make sure that not jobs in hourly buckets
        self.assertEqual(jobAnalyzer.jobs_by_hours,{})

        # Create a dummy job
        wait_time = int(11.666666666666666 * 60)
        start_time = 1643903745
        submit_time = start_time - wait_time
        run_time = 45 * 60
        finish_time = start_time + run_time
        job_dict = {'job_id': 107, 'tasks': 1, 'memory_GB': 65.0, 'instance_family': 'r5', 'instance_type': 'r5.4xlarge', 'instance_hourly_cost': 1.128, 'instance_count': 1}
        job = SchedulerJobInfo(job_dict['job_id'], num_cores=1, max_mem_gb=job_dict['memory_GB'], num_hosts=job_dict['tasks'], submit_time=submit_time, start_time=start_time, finish_time=finish_time, wait_time=wait_time)
        job_cost_data = JobCost(job, run_time/60<= 60, job_dict['instance_family'], job_dict['instance_type'], job_dict['instance_hourly_cost'])

        # Expected contents of hourly csv file
        job_log = '2022-02-03T15:55:45,107,1,45.0,65.0,11.6667,r5.4xlarge,r5,True,1.128,0.846\n'

        batch_size = int(jobAnalyzer.config['consumption_model_mapping']['job_file_batch_size'])
        for i in range(1,batch_size):
            jobAnalyzer._add_job_to_hourly_bucket(job_cost_data)
            count = 0
            for j in jobAnalyzer.jobs_by_hours:
                count += len(jobAnalyzer.jobs_by_hours[j])
            self.assertEqual(i,count)
        jobAnalyzer._add_job_to_hourly_bucket(job_cost_data)
        self.assertEqual(jobAnalyzer.jobs_by_hours, {})
        with open(path.join(jobAnalyzer._output_dir, 'hourly-1643900400.csv'), 'r') as job_log_file:
            lines = job_log_file.readlines()
        self.assertEqual(len(lines), batch_size+1)
        for i in range(1,batch_size):
            self.assertEqual(lines[i], job_log)

    @pytest.mark.order(after='test_log_job_to_file')
    def test_missing_parser(self):
        self.cleanup_output_files()
        with pytest.raises(CalledProcessError) as excinfo:
            output = check_output(['./JobAnalyzer.py', '--output-dir', 'output'], stderr=subprocess.STDOUT, encoding='utf8')
        print(excinfo.value)
        print(excinfo.value.output)
        assert('The following arguments are required: parser' in excinfo.value.output)
        assert(excinfo.value.returncode == 2)

    @pytest.mark.order(after='test_missing_parser')
    def test_csv_bad_credentials(self):
        self.cleanup_output_files()
        self.maxDiff = None
        self._remove_credentials()
        self._remove_instance_type_info()

        try:
            output = check_output(['./JobAnalyzer.py', '--output-dir', 'output/JobAnalyzer/lsf', 'csv', '--input-csv', 'test_files/LSFLogParser/exp_jobs.csv'], stderr=subprocess.STDOUT, encoding='utf8', env=environ)
            print(output)
            assert(False)
        except CalledProcessError as e:
            print(e.output)
            assert('Unable to locate credentials.' in e.output)
            assert('Configure your AWS CLI credentials.' in e.output)

        self._restore_credentials()
        self._remove_instance_type_info()
        self._restore_instance_type_info()

    @pytest.mark.order(after='test_csv_bad_credentials')
    def test_add_job_to_collector(self):
        '''
            * Tests the empty job_data_collector
            * Adds jobs to job_data_Collector and verifies the results
        '''
        # Test the empty dict
        jobAnalyzer = self.get_jobAnalyzer()
        jobAnalyzer._clear_job_stats()
        jobs = jobAnalyzer.job_data_collector

        config = jobAnalyzer.config
        self.assertEqual(len(jobs.keys()),len(config["instance_mapping"]["ram_ranges_GB"])+1)
        for key in jobs:
            self.assertEqual(len(jobs[key]), len(config["instance_mapping"]["runtime_ranges_minutes"])+1)
            for value in jobs[key]:
                self.assertEqual(jobs[key][value]['number_of_jobs'], 0)
                self.assertEqual(jobs[key][value]['total_duration_minutes'], 0)
                self.assertEqual(jobs[key][value]['total_wait_minutes'], 0)

        # Fill collected with jobs and test results
        job = SchedulerJobInfo(job_id=1, num_cores=1, max_mem_gb=0.8, num_hosts=1, submit_time=0, start_time=0, finish_time=3*60, wait_time=2*60)
        jobAnalyzer._add_job_to_collector(job)
        try:
            self.assertEqual(jobs['0-1']['1-5']['number_of_jobs'], 1)
            self.assertEqual(jobs['0-1']['1-5']['total_duration_minutes'], 3)
            self.assertEqual(jobs['0-1']['1-5']['total_wait_minutes'], 2)
        except:
            print(json.dumps(jobs, indent=4))
            raise

        job_dict = {'job_id':2, 'tasks': 3, 'memory_GB': 0.5, 'wait_time_minutes': 2.5, 'runtime_minutes': 4.5, 'instance_count': 1}
        job = SchedulerJobInfo(job_dict['job_id'], num_cores=1, max_mem_gb=job_dict['memory_GB'], num_hosts=job_dict['tasks'], submit_time=0, start_time=int(job_dict['wait_time_minutes']*60), finish_time=int(job_dict['wait_time_minutes']*60) + int(job_dict['runtime_minutes']*60), wait_time=int(job_dict['wait_time_minutes']*60))
        jobAnalyzer._add_job_to_collector(job)
        try:
            self.assertEqual(jobs['0-1']['1-5']['number_of_jobs'], 4)
            self.assertAlmostEqual(jobs['0-1']['1-5']['total_duration_minutes'], 16.5, 1)
            self.assertAlmostEqual(jobs['0-1']['1-5']['total_wait_minutes'], 9.5, 1)
        except:
            print(json.dumps(jobs, indent=4))
            raise

        job_dict = {'job_id':3, 'tasks': 3, 'memory_GB': 2, 'wait_time_minutes': 2.1, 'runtime_minutes': 4.1, 'instance_count': 1}
        job = SchedulerJobInfo(job_dict['job_id'], num_cores=1, max_mem_gb=job_dict['memory_GB'], num_hosts=job_dict['tasks'], submit_time=0, start_time=int(job_dict['wait_time_minutes']*60), finish_time=int(job_dict['wait_time_minutes']*60 + job_dict['runtime_minutes']*60), wait_time=int(job_dict['wait_time_minutes']*60))
        print(f"{job.job_id} run_time={job.run_time}")
        jobAnalyzer._add_job_to_collector(job)
        try:
            self.assertEqual(jobs['1-2']['1-5']['number_of_jobs'], 3)
            self.assertAlmostEqual(jobs['1-2']['1-5']['total_duration_minutes'], 12.3, 1)
            self.assertAlmostEqual(jobs['1-2']['1-5']['total_wait_minutes'], 6.3, 1)
        except:
            #print(json.dumps(jobs, indent=4))
            raise

        job_dict = {'job_id':4, 'tasks': 5, 'memory_GB': 1.8, 'wait_time_minutes': 0.3, 'runtime_minutes': 3.2, 'instance_count': 1}
        job = SchedulerJobInfo(job_dict['job_id'], num_cores=1, max_mem_gb=job_dict['memory_GB'], num_hosts=job_dict['tasks'], submit_time=0, start_time=int(job_dict['wait_time_minutes']*60), finish_time=int(job_dict['wait_time_minutes']*60) + int(job_dict['runtime_minutes']*60), wait_time=int(job_dict['wait_time_minutes']*60))
        jobAnalyzer._add_job_to_collector(job)
        try:
            self.assertEqual(jobs['1-2']['1-5']['number_of_jobs'], 8)
            self.assertAlmostEqual(jobs['1-2']['1-5']['total_duration_minutes'], 28.3, 1)
            self.assertAlmostEqual(jobs['1-2']['1-5']['total_wait_minutes'], 7.8, 1)
        except:
            print(json.dumps(jobs, indent=4))
            raise

    @pytest.mark.order(after='test_add_job_to_collector')
    def test_get_lowest_priced_instance(self):
        jobAnalyzer = self.get_jobAnalyzer()

        (t1, price1) = jobAnalyzer.get_lowest_priced_instance(['c5.large'], False)
        (t2, price2) = jobAnalyzer.get_lowest_priced_instance(['c5.xlarge'], False)
        (t3, price3) = jobAnalyzer.get_lowest_priced_instance(['c5.2xlarge'], False)

        self.assertAlmostEqual(price2/price1,2,0)
        self.assertAlmostEqual(price3/price2,2,0)

        (instance_type, price) = jobAnalyzer.get_lowest_priced_instance(['c5.2xlarge', 'c5.large', 'c5.xlarge'], False)
        assert(instance_type, 'c5.large')
        assert(price == price1)

    def check_get_instance_by_spec(self, min_mem_gb, min_cores, min_freq, exp_num_instance_types):
        jobAnalyzer = self.get_jobAnalyzer()

        instance_type_info = jobAnalyzer.instance_type_info.instance_type_info[self.region]

        instance_types = jobAnalyzer.get_instance_by_spec(min_mem_gb, min_cores, min_freq)
        for instance_type in instance_types:
            mem_gb = instance_type_info[instance_type]['MemoryInMiB'] / 1024
            cores = instance_type_info[instance_type]['CoreCount']
            freq = instance_type_info[instance_type]['SustainedClockSpeedInGhz']
            print(f"instance_type: {instance_type:15} mem_gb={mem_gb:6} cores={cores:3} freq={freq}")
            assert(mem_gb >= min_mem_gb)
            assert(cores >= min_cores)
            assert(freq >= min_freq)
        assert(len(instance_types) == exp_num_instance_types)

    @pytest.mark.order(after='test_get_lowest_priced_instance')
    def test_get_instance_by_spec(self):
        jobAnalyzer = self.get_jobAnalyzer()

        if not jobAnalyzer.instance_type_info:
            jobAnalyzer.get_instance_type_info()

        min_mem_gb = 1537
        min_cores = 17
        min_freq = 4.5
        exp_num_instance_types = 0
        self.check_get_instance_by_spec(min_mem_gb, min_cores, min_freq, exp_num_instance_types)

        min_mem_gb = 1536
        min_cores = 17
        min_freq = 4.5
        exp_num_instance_types = 1
        self.check_get_instance_by_spec(min_mem_gb, min_cores, min_freq, exp_num_instance_types)

        min_mem_gb = 1535
        min_cores = 17
        min_freq = 4.5
        exp_num_instance_types = 1
        self.check_get_instance_by_spec(min_mem_gb, min_cores, min_freq, exp_num_instance_types)

        min_mem_gb = 768
        min_cores = 16
        min_freq = 4.5
        exp_num_instance_types = 2
        self.check_get_instance_by_spec(min_mem_gb, min_cores, min_freq, exp_num_instance_types)

        min_mem_gb = 768
        min_cores = 1
        min_freq = 3
        exp_num_instance_types = 11
        self.check_get_instance_by_spec(min_mem_gb, min_cores, min_freq, exp_num_instance_types)

        min_mem_gb = 32
        min_cores = 24
        min_freq = 2
        exp_num_instance_types = 21
        self.check_get_instance_by_spec(min_mem_gb, min_cores, min_freq, exp_num_instance_types)

        min_mem_gb = 1
        min_cores = 47
        min_freq = 1
        exp_num_instance_types = 9
        self.check_get_instance_by_spec(min_mem_gb, min_cores, min_freq, exp_num_instance_types)

        # all instance types in the allowlist
        min_mem_gb = 1
        min_cores = 1
        min_freq = 1
        exp_num_instance_types = 54
        self.check_get_instance_by_spec(min_mem_gb, min_cores, min_freq, exp_num_instance_types)

    @pytest.mark.order(after='test_get_instance_by_spec')
    def test_get_instance_by_pricing(self):
        jobAnalyzer = self.get_jobAnalyzer()

        if not jobAnalyzer.instance_type_info:
            jobAnalyzer.get_instance_type_info()

        self.assertEqual(len(jobAnalyzer.instance_types), 54)
        counter = {}
        for instance_type in jobAnalyzer.instance_types:
            instance_family = instance_type.split('.')[0]
            counter[instance_family] = counter.get(instance_family, 0) + 1
        assert(len(counter.keys()) == 8)
        self.assertEqual(counter['c5'], 8)
        self.assertEqual(counter['r5'], 8)
        self.assertEqual(counter['m5'],8)
        self.assertEqual(counter['c6i'],9)
        self.assertEqual(counter['z1d'],6)
        self.assertEqual(counter['x2iezn'], 5)
        self.assertEqual(counter['x2idn'], 3)
        self.assertEqual(counter['x2iedn'], 7)

        (instance_type1, price1) = jobAnalyzer.get_lowest_priced_instance(['c5.large', 'c6i.large'], False)
        self.assertEqual(instance_type1,'c6i.large')
        self.assertGreater(price1, 0.0001)

        (instance_type2, price2) = jobAnalyzer.get_lowest_priced_instance(['c6i.8xlarge', 'r5.8xlarge'], False)
        self.assertTrue(instance_type2, 'c6i.8xlarge')
        self.assertGreater(price2, 0.9)

    @pytest.mark.order(after='test_get_instance_by_pricing')
    def test_multi_hour_jobs(self):
        '''
        Test JobAnalyzer when jobs are longer than an hour.
        '''
        # Remove credentials to ensure instance_type_info.json is used.
        self._remove_credentials()

        self.cleanup_output_files()
        jobs_csv = 'test_files/JobAnalyzer/multi-hour/jobs.csv'
        output_dir = 'output/JobAnalyzer/multi-hour'
        # Put this in a try block so that can print the output if an unexpected exception occurs.
        try:
            check_output(['./JobAnalyzer.py', '--output-dir', output_dir, 'csv', '--input-csv', jobs_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.stdout}")
            raise
        csv_files_dir = output_dir
        csv_files = [
            'hourly-1647784800.csv',
            'hourly_stats.csv',
            'summary.csv'
            ]
        for csv_file in csv_files:
            assert(filecmp.cmp(path.join(csv_files_dir, csv_file), path.join(output_dir, csv_file), shallow=False))

        self._restore_credentials()

    @pytest.mark.order(after='test_multi_hour_jobs')
    def test_from_accelerator(self):
        '''
        Test JobAnalyzer when parsing jobs from Accelerator logs.
        '''
        try:
            result = subprocess.run(["nc", "-h"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='UTF-8') # nosec
        except FileNotFoundError as e:
            print(f"Cannot find nc command.")
            return
        except CalledProcessError as e:
            print(f"'nc -h' failed.")
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.stdout}")
            return
        output = result.stdout
        if result.returncode != 2 or 'Altair Engineering.' not in output:
            print(f"Unexpected result from 'nc -h'\nreturncode: expected 2, actual {result.returncode}\noutput:\n{output}")
            if 'Usage: nc' in output:
                print(f"'nc -h' called ncat (netcat), not Altair nc.")
            print(f"'nc -h' failed.")
            return

        self._remove_credentials()

        self.cleanup_output_files()
        test_files_dir = 'test_files/AcceleratorLogParser'
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        output_dir = 'output/JobAnalyzer/accelerator'
        output_csv = path.join(output_dir, 'jobs.csv')
        # Put this in a try block so that can print the output if an unexpected exception occurs.
        try:
            check_output(['./JobAnalyzer.py', '--output-csv', output_csv, '--output-dir', output_dir, 'accelerator', '--logfile-dir', test_files_dir], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.stdout}")
            raise
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

        csv_files = [
            'hourly_stats.csv',
            'summary.csv'
            ]
        for csv_file in csv_files:
            assert(path.exists(csv_file))

        self._restore_credentials()

    @pytest.mark.order(after='test_from_accelerator')
    def test_from_accelerator_sql_file(self):
        '''
        Test JobAnalyzer when parsing jobs from Accelerator sql output.
        '''
        self._remove_credentials()

        self.cleanup_output_files()

        test_files_dir = 'test_files/AcceleratorLogParser'
        sql_input_file = path.join(test_files_dir, 'sql_output.txt')
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        output_dir = 'output/JobAnalyzer/accelerator'
        output_csv = path.join(output_dir, 'jobs.csv')
        exp_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        # Put this in a try block so that can print the output if an unexpected exception occurs.
        try:
            output = check_output(['./JobAnalyzer.py', '--output-csv', output_csv, '--output-dir', output_dir, 'accelerator', '--sql-input-file', sql_input_file], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.stdout}")
            raise
        print(f"output:\n{output}")
        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

        csv_files = [
            'hourly_stats.csv',
            'summary.csv'
            ]
        exp_csv_files_dir = 'test_files/JobAnalyzer/accelerator'
        exp_csv_files = self._get_hourly_files(exp_csv_files_dir) + csv_files
        act_csv_files = self._get_hourly_files(output_dir) + csv_files
        print(f"exp_csv_files: {exp_csv_files}")
        print(f"act_csv_files: {act_csv_files}")
        for exp_csv_file in exp_csv_files:
            assert(exp_csv_file in act_csv_files)
        for act_csv_file in exp_csv_files:
            assert(act_csv_file in exp_csv_files)
        for csv_file in exp_csv_files:
            assert(filecmp.cmp(path.join(exp_csv_files_dir, csv_file), path.join(output_dir, csv_file), shallow=False))

        self._restore_credentials()

    @pytest.mark.order(after='test_from_accelerator')
    def test_from_accelerator_csv(self):
        self._remove_credentials()

        self.cleanup_output_files()
        test_files_dir = 'test_files/AcceleratorLogParser'
        input_csv = path.join(test_files_dir, 'exp_jobs.csv')
        output_dir = 'output/JobAnalyzer/accelerator'
        output_csv = path.join(output_dir, 'jobs.csv')
        expected_output_csv = input_csv
        try:
            output = check_output(['./JobAnalyzer.py', '--output-csv', output_csv, '--output-dir', output_dir, 'csv', '--input-csv', input_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.stdout}")
            raise
        print(f"output:\n{output}")

        assert(filecmp.cmp(output_csv, expected_output_csv, shallow=False))

        exp_csv_files_dir = 'test_files/JobAnalyzer/accelerator'
        exp_csv_files = self._get_hourly_files(exp_csv_files_dir)
        act_csv_files = self._get_hourly_files(output_dir)
        for exp_csv_file in exp_csv_files:
            assert(exp_csv_file in act_csv_files)
        for act_csv_file in exp_csv_files:
            assert(act_csv_file in exp_csv_files)
        csv_files = exp_csv_files + [
            'hourly_stats.csv',
            'summary.csv'
            ]
        for csv_file in csv_files:
            assert(filecmp.cmp(path.join(exp_csv_files_dir, csv_file), path.join(output_dir, csv_file), shallow=False))

        self._restore_credentials()

    @pytest.mark.order(after='test_from_accelerator_csv')
    def test_from_lsf(self):
        '''
        Test JobAnalyzer when parsing jobs from LSF logs.
        '''
        self._remove_credentials()

        self.cleanup_output_files()
        test_files_dir = 'test_files/LSFLogParser'
        expected_output_csv = path.join(test_files_dir, 'exp_jobs.csv')
        output_dir = 'output/JobAnalyzer/lsf'
        output_csv = path.join(output_dir, 'jobs.csv')
        # Put this in a try block so that can print the output if an unexpected exception occurs.
        try:
            check_output(['./JobAnalyzer.py', '--output-csv', output_csv, '--output-dir', output_dir, 'lsf', '--logfile-dir', test_files_dir], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(e.output)
            raise

        assert(filecmp.cmp(expected_output_csv, output_csv, shallow=False))

        exp_csv_files_dir = 'test_files/JobAnalyzer/lsf'
        exp_csv_files = self._get_hourly_files(exp_csv_files_dir)
        act_csv_files = self._get_hourly_files(output_dir)
        for exp_csv_file in exp_csv_files:
            assert(exp_csv_file in act_csv_files)
        for act_csv_file in exp_csv_files:
            assert(act_csv_file in exp_csv_files)
        csv_files = exp_csv_files + [
            'hourly_stats.csv',
            'summary.csv'
            ]
        for csv_file in csv_files:
            assert(filecmp.cmp(path.join(exp_csv_files_dir, csv_file), path.join(output_dir, csv_file), shallow=False))

        self._restore_credentials()

    @pytest.mark.order(after='test_from_lsf')
    def test_from_lsf_csv(self):
        self._remove_credentials()

        self.cleanup_output_files()
        test_files_dir = 'test_files/LSFLogParser'
        input_csv = path.join(test_files_dir, 'exp_jobs.csv')
        output_dir = 'output/JobAnalyzer/lsf'
        output_csv = path.join(output_dir, 'jobs.csv')
        expected_output_csv = input_csv
        try:
            output = check_output(['./JobAnalyzer.py', '--output-csv', output_csv, '--output-dir', output_dir, 'csv', '--input-csv', input_csv], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(e.output)
            raise
        print(f"output:\n{output}")

        assert(filecmp.cmp(expected_output_csv, output_csv, shallow=False))

        exp_csv_files_dir = 'test_files/JobAnalyzer/lsf'
        exp_csv_files = self._get_hourly_files(exp_csv_files_dir)
        act_csv_files = self._get_hourly_files(output_dir)
        for exp_csv_file in exp_csv_files:
            assert(exp_csv_file in act_csv_files)
        for act_csv_file in exp_csv_files:
            assert(act_csv_file in exp_csv_files)
        csv_files = exp_csv_files + [
            'hourly_stats.csv',
            'summary.csv'
            ]
        for csv_file in csv_files:
            assert(filecmp.cmp(path.join(exp_csv_files_dir, csv_file), path.join(output_dir, csv_file), shallow=False))

        self._restore_credentials()

    @pytest.mark.order(after='test_from_lsf_csv')
    def test_from_slurm_sacct_file(self):
        self._remove_credentials()

        self.cleanup_output_files()
        sacct_input_file = 'test_files/SlurmLogParser/sacct-output.txt'
        output_dir = 'output/JobAnalyzer/slurm'
        # Put this in a try block so that can print the output if an unexpected exception occurs.
        try:
            output = check_output(['./JobAnalyzer.py', '--output-dir', output_dir, 'slurm', '--sacct-input-file', sacct_input_file], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(e.output)
            raise
        print(f"output:\n{output}")

        exp_csv_files_dir = 'test_files/JobAnalyzer/slurm'
        exp_csv_files = self._get_hourly_files(exp_csv_files_dir)
        act_csv_files = self._get_hourly_files(output_dir)
        for exp_csv_file in exp_csv_files:
            assert(exp_csv_file in act_csv_files)
        for act_csv_file in exp_csv_files:
            assert(act_csv_file in exp_csv_files)
        csv_files = exp_csv_files + [
            'hourly_stats.csv',
            'summary.csv'
            ]
        for csv_file in csv_files:
            assert(filecmp.cmp(path.join(exp_csv_files_dir, csv_file), path.join(output_dir, csv_file), shallow=False))

        self._restore_credentials()

    @pytest.mark.order(after='test_from_slurm_sacct_file')
    def test_from_slurm(self):
        # Only run this test if sacct is in the path so can run tests on instances without slurm
        try:
            check_output(["squeue"]) # nosec
        except (CalledProcessError, FileNotFoundError) as e:
            print(f"Slurm is not installed or available in the path.")
            return

        self._remove_credentials()

        output = check_output(['./JobAnalyzer.py', '--output-dir', 'output/JobAnalyzer/slurm', 'slurm'], stderr=subprocess.STDOUT, encoding='utf8')

        self.cleanup_output_files()
        output_dir = 'output/JobAnalyzer/slurm'
        # Put this in a try block so that can print the output if an unexpected exception occurs.
        try:
            output = check_output(['./JobAnalyzer.py', '--output-dir', output_dir, 'slurm'], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(e.output)
            raise

        self._restore_credentials()

    @pytest.mark.order(after='test_from_slurm')
    def test_get_instances(self):
        jobAnalyzer = self.get_jobAnalyzer()

        self.cleanup_output_files()
        self._remove_instance_type_info()

        jobAnalyzer.get_instance_type_info()

        self.assertEqual(len(jobAnalyzer.instance_types), 54)
        counter = {}
        for instance_type in jobAnalyzer.instance_types:
            instance_family = instance_type.split('.')[0]
            counter[instance_family] = counter.get(instance_family, 0) + 1
        self.assertEqual(counter['c5'], 8)
        self.assertEqual(counter['r5'], 8)
        self.assertEqual(counter['m5'],8)
        self.assertEqual(counter['c6i'],9)
        self.assertEqual(counter['z1d'],6)
        self.assertEqual(counter['x2iezn'],5)

        jobAnalyzer.get_instance_type_info()

        self.assertEqual(len(jobAnalyzer.instance_types), 54)
        counter = {}
        for instance_type in jobAnalyzer.instance_types:
            instance_family = instance_type.split('.')[0]
            counter[instance_family] = counter.get(instance_family, 0) + 1
        self.assertEqual(counter['c5'], 8)
        self.assertEqual(counter['r5'], 8)
        self.assertEqual(counter['m5'],8)
        self.assertEqual(counter['c6i'],9)
        self.assertEqual(counter['z1d'],6)
        self.assertEqual(counter['x2iezn'],5)

        self._remove_instance_type_info()
        self._restore_instance_type_info()

    @pytest.mark.order(after='test_get_instances')
    def test_get_instance_type_info_region(self):
        self.cleanup_output_files()
        self._remove_instance_type_info()
        try:
            output = check_output(['./get_ec2_instance_info.py', '--region', self.region, '--input', 'instance_type_info.json'], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(e.output)
            raise
        print(f"output:\n{output}")
        assert(path.exists(path.join(self.REPO_DIR, 'instance_type_info.json')))

        self._remove_instance_type_info()
        self._restore_instance_type_info()

    @pytest.mark.order(after='test_get_instance_type_info_region')
    def test_get_instance_type_info(self):
        '''
        Generate instance_type_info.json to make sure it is up to date

        Generate for all AWS regions.
        '''
        self._remove_instance_type_info()
        try:
            check_output(['./get_ec2_instance_info.py', '--input', 'instance_type_info.json'], stderr=subprocess.STDOUT, encoding='utf8')
        except CalledProcessError as e:
            print(f"returncode: {e.returncode}")
            print(f"output:\n{e.stdout}")
            raise
