'''
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
'''
import pytest
import SchedulerJobInfo

order = 0

class TestSchedulerJobInfo:
    global order
    order += 1
    assert order == 1
    @pytest.mark.order(order)
    def test_fix_datetime(self):
        global order
        assert order == 7
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime(None)  == (None, None))
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime(-1)  == (None, None))
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime(0)[0]  == '1970-01-01T00:00:00')
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime(1644826549)[0]  == '2022-02-14T08:15:49')
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime(1643910038)[0]  == '2022-02-03T17:40:38')
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime('')  == (None, None))
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime('-1')  == (None, None))
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime('0')[0]  == '1970-01-01T00:00:00')
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime('1644826549')[0]  == '2022-02-14T08:15:49')
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime('1970-01-01T00:00:00')[0]  == '1970-01-01T00:00:00')
        assert(SchedulerJobInfo.SchedulerJobInfo.fix_datetime('2022-02-14T08:15:49')[0]  == '2022-02-14T08:15:49')
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.fix_datetime('1970-01')

    order += 1
    @pytest.mark.order(order)
    def test_fix_duration(self):
        fix_duration = SchedulerJobInfo.SchedulerJobInfo.fix_duration

        duration_string = fix_duration(None)
        assert(duration_string == (None, None))

        # Integer/float values
        assert(fix_duration(None) == (None, None))
        assert(fix_duration(-1) == (None, None))
        assert(fix_duration(0)[0] == '0:00:00')
        assert(fix_duration(0.0123)[0] == '0:00:00.012300')
        assert(fix_duration(1)[0] == '0:00:01')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.MINUTE_SECONDS-1)[0] == '0:00:59')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.MINUTE_SECONDS)[0] == '0:01:00') # 1 minute
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.MINUTE_SECONDS+1)[0] == '0:01:01')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.HOUR_SECONDS-1)[0] == '0:59:59')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.HOUR_SECONDS)[0] == '1:00:00') # 1 hour
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.HOUR_SECONDS+1)[0] == '1:00:01')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.DAY_SECONDS-1)[0] == '23:59:59')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.DAY_SECONDS)[0] == '01-0:00:00') # 1 day
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.DAY_SECONDS+1)[0] == '01-0:00:01')

        # String values
        assert(fix_duration('') == (None, None))
        assert(fix_duration('0')[0] == '0:00:00')
        assert(fix_duration('0.1230456')[0] == '0:00:00.123046')
        assert(fix_duration(str(SchedulerJobInfo.SchedulerJobInfo.DAY_SECONDS))[0] == '01-0:00:00')
        assert(fix_duration('0:0')[0] == '0:00:00')
        assert(fix_duration('0:00')[0] == '0:00:00')
        assert(fix_duration('00:00')[0] == '0:00:00')
        assert(fix_duration('0:00:00')[0] == '0:00:00')
        assert(fix_duration('00:00:00')[0] == '0:00:00')
        assert(fix_duration('0-00:00:00')[0] == '0:00:00')
        assert(fix_duration('00-00:00:00')[0] == '0:00:00')
        assert(fix_duration('1-1:1:1')[0] == '01-1:01:01')
        assert(fix_duration('01-01:01:01')[0] == '01-1:01:01')
        assert(fix_duration('00:59')[0] == '0:00:59')
        assert(fix_duration('01:00')[0] == '0:01:00') # 1 minute
        assert(fix_duration('01:01')[0] == '0:01:01')
        assert(fix_duration('59:59')[0] == '0:59:59')
        assert(fix_duration('01:00:00')[0] == '1:00:00') # hour
        assert(fix_duration('01:00:01')[0] == '1:00:01')
        assert(fix_duration('23:59:59')[0] == '23:59:59')
        assert(fix_duration('01-00:00:00')[0] == '01-0:00:00')
        assert(fix_duration('01-00:00:01')[0] == '01-0:00:01')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.MINUTE_SECONDS)[0] == '0:01:00') # 1 minute
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.MINUTE_SECONDS+1)[0] == '0:01:01')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.HOUR_SECONDS-1)[0] == '0:59:59')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.HOUR_SECONDS)[0] == '1:00:00') # 1 hour
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.HOUR_SECONDS+1)[0] == '1:00:01')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.DAY_SECONDS-1)[0] == '23:59:59')
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.DAY_SECONDS)[0] == '01-0:00:00') # 1 day
        assert(fix_duration(SchedulerJobInfo.SchedulerJobInfo.DAY_SECONDS+1)[0] == '01-0:00:01')

        with pytest.raises(ValueError):
            fix_duration([])
        with pytest.raises(ValueError):
            fix_duration('abc')
        with pytest.raises(ValueError):
            fix_duration('0-0')

    # timestamp_to_datetime tested by fix_datetime

    order += 1
    @pytest.mark.order(order)
    def test_str_to_datetime(self):
        # str_to_datetime used by constructor to compare different times to get durations
        assert(SchedulerJobInfo.SchedulerJobInfo.datetime_to_str(SchedulerJobInfo.SchedulerJobInfo.str_to_datetime('1970-01-01T00:00:00')) == '1970-01-01T00:00:00')

        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_datetime(None)
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_datetime(-1)
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_datetime(0)
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_datetime('1970-01')

    order += 1
    @pytest.mark.order(order)
    def test_datetime_to_str(self):
        # Valid cases tested by test_str_to_datetime
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.datetime_to_str(None)
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.datetime_to_str(-1)
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.datetime_to_str(0)
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.datetime_to_str('')

    order += 1
    @pytest.mark.order(order)
    def test_str_to_timedelta(self):
        # This function is already tested by test_fix_duration which converts string to timedelta and back again to fix formatting.
        # Test invalid types and values
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_timedelta([])
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_timedelta(-1)
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_timedelta(0)
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_timedelta(1)
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_timedelta('abc')
        with pytest.raises(ValueError):
            SchedulerJobInfo.SchedulerJobInfo.str_to_timedelta('0-0')

    order += 1
    @pytest.mark.order(order)
    def test_timedelta_to_string(self):
        # This function is already tested by test_fix_duration which converts string to timedelta and back again to fix formatting.
        # Test invalid types and values
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.timedelta_to_string([])
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.timedelta_to_string(-1)
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.timedelta_to_string(0)
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.timedelta_to_string(1)
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.timedelta_to_string('abc')
        with pytest.raises(AttributeError):
            SchedulerJobInfo.SchedulerJobInfo.timedelta_to_string('0-0')

    order += 1
    assert order == 7
    @pytest.mark.order(order)
    def test_init(self):
        with pytest.raises(TypeError):
            job = SchedulerJobInfo.SchedulerJobInfo()

        # LSFLogParser use case
        job = SchedulerJobInfo.SchedulerJobInfo(
            job_id = '101',
            resource_request = '',
            num_cores = 1,
            max_mem_gb = 1,
            num_hosts = 1,

            submit_time = '1970-01-01T00:00:00',
            ineligible_pend_time = 0,
            requeue_time = 0,
            wait_time = 3,
            start_time = '1970-01-01T00:02:36',
            run_time = 3002,
            finish_time = '1970-01-01T00:07:38',

            exit_status = 0,
            ru_inblock = 1000,
            ru_majflt = 23,
            ru_maxrss = 1000000000,
            ru_minflt = 0,
            ru_msgrcv = 0,
            ru_msgsnd = 0,
            ru_nswap = 0,
            ru_oublock = 0,
            ru_stime = 0.0,
            ru_utime = 0.0
        )

        # SlurmLogParser use case
        job = SchedulerJobInfo.SchedulerJobInfo(
            job_id = '101',
            resource_request = '',
            num_cores = 1,
            max_mem_gb = 1,
            num_hosts = 1,

            submit_time = '1970-01-01T00:00:00',
            eligible_time = '1970-01-01T00:00:00',
            start_time = '1970-01-01T00:02:36',
            run_time = 3002,
            finish_time = '1970-01-01T00:07:38',

            exit_status = 0,
            ru_inblock = 1000,
            ru_majflt = 23,
            ru_maxrss = 1000000000,
            # ru_minflt = 0,
            # ru_msgrcv = 0,
            # ru_msgsnd = 0,
            # ru_nswap = 0,
            ru_oublock = 0,
            ru_stime = 0.0,
            ru_utime = 0.0
        )
