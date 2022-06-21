
LSB_ACCT_RECORD_FORMATS = {
    'JOB_FINISH': {
        # A job has finished.
        # If LSF_HPC_EXTENSIONS="SHORT_EVENTFILE" is specified in lsf.conf, older daemons and commands (pre-LSF Version 6.0) cannot recognize the lsb.acct file format.
        # The fields in order of occurrence are:
        'fields': [
            ('Version Number', '%s'), # Version number of the log file format
            ('Event Time', '%d'), # Time the event was logged (in seconds since the epoch)
            ('jobId', '%d'), # ID for the job
            ('userId', '%d'), # UNIX user ID of the submitter
            ('options', '%d'), # Bit flags for job processing
            ('numProcessors', '%d'), # Number of processors initially requested for execution
            ('submitTime', '%d'), # Job submission time
            ('beginTime', '%d'), # Job start time – the job should be started at or after this time
            ('termTime', '%d'), # Job termination deadline – the job should be terminated by this time
            ('startTime', '%d'), # Job dispatch time – time job was dispatched for execution
            ('userName', '%s'), # User name of the submitter
            ('queue', '%s'), # Name of the job queue to which the job was submitted
            ('resReq', '%s'), # Resource requirement specified by the user
            ('dependCond', '%s'), # Job dependency condition specified by the user
            ('preExecCmd', '%s'), # Pre-execution command specified by the user
            ('fromHost', '%s'), # Submission host name
            ('cwd', '%s'), # Current working directory (up to 4094 characters for UNIX or 512 characters for Windows), or the current working directory specified by bsub -cwd if that command was used.
            ('inFile', '%s'), # Input file name (up to 4094 characters for UNIX or 512 characters for Windows)
            ('outFile', '%s'), # output file name (up to 4094 characters for UNIX or 512 characters for Windows)
            ('errFile', '%s'), # Error output file name (up to 4094 characters for UNIX or 512 characters for Windows)
            ('jobFile', '%s'), # Job script file name

            # This is weird. If numAskedHosts!=0 then there will be a list of askedHosts and no numExHosts field.
            # If numAskedHosts==0 then the numExHosts field is set and there is a list of execHosts
            # It appears that you will either have askedHosts or execHosts but not both.

            ('numAskedHosts', '%d'), # Number of host names to which job dispatching will be limited
            #('askedHosts', '%s'), # List of host names to which job dispatching will be limited (%s for each); nothing is logged to the record for this value if the last field value is 0. If there is more than one host name, then each additional host name will be returned in its own field

            ('numExHosts', '%d'), # Number of processors used for execution
                # If LSF_HPC_EXTENSIONS="SHORT_EVENTFILE" is specified in lsf.conf, the value of this field is the number of hosts listed in the execHosts field.
                # Logged value reflects the allocation at job finish time.
            #('execHosts', '%s'), # List of execution host names (%s for each); nothing is logged to the record for this value if the last field value is 0.

                # If LSF_HPC_EXTENSIONS="SHORT_EVENTFILE" is specified in lsf.conf, the value of this field is logged in a shortened format.
                # The logged value reflects the allocation at job finish time.
            ('jStatus', '%d'), # Job status. The number 32 represents EXIT, 64 represents DONE
            ('hostFactor', '%f'), # CPU factor of the first execution host.
            ('jobName', '%s'), # Job name (up to 4094 characters).
            ('command', '%s'), # Complete batch job command specified by the user (up to 4094 characters for UNIX or 512 characters for Windows).

            # lsfRusage
            # The following fields contain resource usage information for the job (see getrusage(2)). If the value of some field is unavailable (due to job exit or the difference among the operating systems), -1 will be logged. Times are measured in seconds, and sizes are measured in KB.
            ('ru_utime', '%f'), # User time used
            ('ru_stime', '%f'), # System time used
            ('ru_maxrss', '%f'), # Maximum shared text size
            ('ru_ixrss', '%f'), # Integral of the shared text size over time (in KB seconds)
            ('ru_ismrss', '%f'), # Integral of the shared memory size over time (valid only on Ultrix)
            ('ru_idrss', '%f'), # Integral of the unshared data size over time
            ('ru_isrss', '%f'), # Integral of the unshared stack size over time
            ('ru_minflt', '%f'), # Number of page reclaims
            ('ru_majflt', '%f'), # Number of page faults
            ('ru_nswap', '%f'), # Number of times the process was swapped out
            ('ru_inblock', '%f'), # Number of block input operations
            ('ru_oublock', '%f'), # Number of block output operations
            ('ru_ioch', '%f'), # Number of characters read and written (valid only on HP-UX)
            ('ru_msgsnd', '%f'), # Number of System V IPC messages sent
            ('ru_msgrcv', '%f'), # Number of messages received
            ('ru_nsignals', '%f'), # Number of signals received
            ('ru_nvcsw', '%f'), # Number of voluntary context switches
            ('ru_nivcsw', '%f'), # Number of involuntary context switches
            ('ru_exutime', '%f'), # Exact user time used (valid only on ConvexOS)

            ('mailUser', '%s'), # Name of the user to whom job related mail was sent
            ('projectName', '%s'), # LSF project name (up to 59 characters)
            ('exitStatus', '%d'), # UNIX exit status of the job
            ('maxNumProcessors', '%d'), # Maximum number of processors specified for the job
            ('loginShell', '%s'), # Login shell used for the job
            ('timeEvent', '%s'), # Time event string for the job - IBM Spectrum LSF Process Manager only
            ('idx', '%d'), # Job array index
            ('maxRMem', '%d'), # Maximum resident memory usage in KB of all processes in the job
            ('maxRSwap', '%d'), # Maximum virtual memory usage in KB of all processes in the job
            ('inFileSpool', '%s'), # Spool input file (up to 4094 characters for UNIX or 512 characters for Windows)
            ('commandSpool', '%s'), # Spool command file (up to 4094 characters for UNIX or 512 characters for Windows)
            ('rsvId', '%s'), # Advance reservation ID for a user group name less than 120 characters long; for example, "user2#0"
                # If the advance reservation user group name is longer than 120 characters, the rsvId field output appears last.
            ('sla', '%s'), # SLA service class name under which the job runs
            ('exceptMask', '%d'), # Job exception handling
                # Values:
                # J_EXCEPT_OVERRUN 0x02
                # J_EXCEPT_UNDERUN 0x04
                # J_EXCEPT_IDLE 0x80
            ('additionalInfo', '%s'), # Placement information of HPC jobs
            ('exitInfo', '%d'), # Job termination reason, mapped to corresponding termination keyword displayed by bacct.
            ('warningAction', '%s'), # Job warning action
            ('warningTimePeriod', '%d'), # Job warning time period in seconds
            ('chargedSAAP', '%s'), # SAAP charged to a job
            ('licenseProject', '%s'), # IBM Spectrum LSF License Scheduler project name
            ('app', '%s'), # Application profile name
            ('postExecCmd', '%s'), # Post-execution command to run on the execution host after the job finishes
            ('runtimeEstimation', '%d'), # Estimated run time for the job, calculated as the CPU factor of the submission host multiplied by the runtime estimate (in seconds).
            ('jobGroupName', '%s'), # Job group name
            ('requeueEvalues', '%s'), # Requeue exit value
            ('options2', '%d'), # Bit flags for job processing
            ('resizeNotifyCmd', '%s'), # Resize notification command to be invoked on the first execution host upon a resize request.
            ('lastResizeTime', '%d'), # Last resize time. The latest wall clock time when a job allocation is changed.
            ('rsvId', '%s'), # Advance reservation ID for a user group name more than 120 characters long.
                # If the advance reservation user group name is longer than 120 characters, the rsvId field output appears last.
            ('jobDescription', '%s'), # Job description (up to 4094 characters).

            # submitEXT
            # Submission extension field, reserved for internal use.
            ('Num', '%d'), # Number of elements (key-value pairs) in the structure.
            #('key', '%s'), # Reserved for internal use.
            #('value', '%s'), # Reserved for internal use.

            ('numHostRusage', '%d'), # The number of host-based resource usage entries (hostRusage) that follow. Enabled by default.

            # hostRusage
            # The following fields contain host-based resource usage information for the job. To disable reporting of hostRusage set LSF_HPC_EXTENSIONS=NO_HOST_RUSAGE in lsf.conf.
            #('hostname', '%s'), # Name of the host.
            #('mem', '%d'), # Total resident memory usage of all processes in the job running on this host.
            #('swap', '%d'), # The total virtual memory usage of all processes in the job running on this host.
            #('utime', '%d'), # User time used on this host.
            #('stime', '%d'), # System time used on this host.

            ('options3', '%d'), # Bit flags for job processing
            ('runLimit', '%d'), # Job submission runtime limit
            ('avgMem', '%d'), # Job average memory usage
            ('effectiveResReq', '%s'), # The runtime resource requirements used for the job.
            ('srcCluster', '%s'), # The name of the submission cluster
            ('srcJobId', '%d'), # The submission cluster job ID
            ('dstCluster', '%s'), # The name of the execution cluster
            ('dstJobId', '%d'), # The execution cluster job ID
            ('forwardTime', '%d'), # The job forward time.
            ('flow_id', '%d'), # Internal usage.
            ('acJobWaitTime', '%d'), # Reserved for internal use.
            ('totalProvisionTime', '%d'), # Reserved for internal use.
            ('outdir', '%s'), # The output directory.
            ('runTime', '%d'), # Time in seconds that the job has been in the run state. runTime includes the totalProvisionTime.
            ('subcwd', '%s'), # The submission current working directory.
            ('num_network', '%d'), # The number of allocated networks.

            # networkAlloc
            # List of network allocation information. Nothing is logged to the record for this value if the last field value is 0.
            #('networkID', '%s'), # Allocated networkID array.
            #('num_window', '%d'), # Array of allocated numbers of windows.

            ('affinity', '%s'), # Affinity allocation information.
            ('serial_job_energy', '%f'), # Serial job energy data.
            ('cpi', '%f'), # Cycles per instruction.
            ('gips', '%f'), # Giga-instructions per second.
            ('gbs', '%f'), # Gigabytes per second.
            ('gflops', '%f'), # Giga floating point operations per second.

            ('numAllocSlots', '%d'), # Number of allocated slots.
            #('allocSlots', '%s'), # List of execution host names where the slots are allocated.

            ('ineligiblePendTime', '%d'), # Time in seconds that the job has been in the ineligible pending state.

            ('indexRangeCnt', '%d'), # The number of element ranges indicating successful signals
            #('indexRangeStart1', '%d'), # The start of the first index range.
            #('indexRangeEnd1', '%d'), # The end of the first index range.
            #('indexRangeStep1', '%d'), # The step of the first index range.
            #('indexRangeStartN', '%d'), # The start of the last index range.
            #('indexRangeEndN', '%d'), # The end of the last index range.
            #('indexRangeStepN', '%d'), # The step of the last index range.

            ('requeueTime', '%d'), # The job's requeue time.

            ('numGPURusages', '%d'), # The number of host-based GPU rusage records.
            # gRusage
            # List of host-based GPU rusage. Nothing is logged to the record for this value if the last field value is 0.
            #('hostname', '%s'), # Current host name.
            #('numKVP', '%d'), # Number of elements (key-value pairs) in the structure.
            #('key', '%s'), # Reserved for future use.
            #('value', '%s'), # Reserved for future use.

            ('storageInfoC', '%d'), # The number of storage staging information.
            #('storageInfoV', '%d'), # List of storage staging information. Nothing is logged to the record for this value if the last field value is 0.

            # finishKVP
            ('numKVP', '%d'), # Number of elements (key-value pairs) in the structure.
            #('key', '%s'), # Reserved for future use.
            #('value', '%s'), # Reserved for future use.

            #('schedulingOverhead', '%f'), # The scheduler overhead for a job, in milliseconds. This is the total time that is taken by the scheduler to dispatch the job and the time that is taken by the scheduler to reallocate resources to a new job.
        ]
    },
    'EVENT_ADRSV_FINISH': None,
    'JOB_RESIZE': None,
}

MINIMAL_LSB_ACCT_FIELDS = [
    'Event Time',
    'beginTime',
    'effectiveResReq',
    'exitInfo',
    'exitStatus',
    'forwardTime',
    'hostFactor',
    'idx',
    'ineligiblePendTime',
    'jStatus',
    'jobId',
    'maxNumProcessors',
    'maxRMem',
    'maxRSwap',
    'numAllocSlots',
    'numProcessors',
    'record_type',
    'requeueTime',
    'resReq',
    'ru_inblock',
    'ru_majflt',
    'ru_maxrss',
    'ru_minflt',
    'ru_msgrcv',
    'ru_msgsnd',
    'ru_nswap',
    'ru_oublock',
    'ru_stime',
    'ru_utime',
    'runLimit',
    'runTime',
    'startTime',
    'submitTime',
    'termTime',
    'totalProvisionTime',
]
