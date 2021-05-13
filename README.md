# hpc-exporter

This prometheus exporter connects via ssh to the frontend of a given HPC infrastructure and queries the scheduler in order to collect and expose the metrics of all the jobs the user can see. It also gathers and exposes data on the HPC partitions/queues.

## Metrics collected for supported schedulers
### **PBS Professional**
#### Job metrics

Labels of all the job metrics: `{job_id, job_name, job_user, job_queue}` : 
- `pbs_job_state`: Job state numeric code. See **State codes** below for details.
- `pbs_job_state`: Exit status numeric code
- `pbs_job_priority`: Job current priority
- `pbs_job_walltime_used`: Walltime consumed in seconds
- `pbs_job_walltime_max`: Maximum wall time in seconds
- `pbs_job_walltime_remaining`: Remaining wall time in seconds
- `pbs_job_cpu_time`: Consumed CPU time in seconds
- `pbs_job_cpu_n`: Number of threads requested by this job
- `pbs_job_memory_physical`: Physical memory consumed in `bytes`
- `pbs_job_memory_virtual`: Virtual memory consumed in `bytes`
- `pbs_job_time_queued`: Time spent in queue by the job in seconds
- `pbs_job_exit_status`: Job exit status. -50 if not completed. Check [here](https://www.nas.nasa.gov/hecc/support/kb/pbs-exit-codes_185.html) for the meaning of each code
#### Queue metrics

Labels of all the queue metrics: `{queue_name, queue_type}` : 
- `pbs_queue_enabled`: 1 if the queue is enabled, 0 if it is disabled
- `pbs_queue_started`: 1 if the queue is started, 0 if it is stopped
- `pbs_queue_jobs_max`: Maximum number of jobs that can be run in the queue (-1 if inf)
- `pbs_queue_jobs_queued`: Number of jobs with queued status in the queue
- `pbs_queue_jobs_running`: Number of jobs with running status in the queue
- `pbs_queue_jobs_held`: Number of jobs with held status in the queue
- `pbs_queue_jobs_waiting`: Number of jobs with waiting status in the queue
- `pbs_queue_jobs_transit`: Number of jobs with transit status in the queue
- `pbs_queue_jobs_exiting`: Number of jobs with exiting status in the queue
- `pbs_queue_jobs_complete`: Number of jobs with complete status in the queue

### **SLURM**

#### Job metrics

Labels of all the job metrics: `{job_id, job_name, job_user, job_partition}` : 
- `slurm_job_state`: Job state numeric code. **State codes** below for details.
- `slurm_job_walltime_used`: Walltime consumed in seconds
- `slurm_job_cpu_n`: Number of CPUs assigned to this job
- `slurm_job_memory_physical_max`: Maximum physical memory in `bytes` that can be allocated to this job
- `slurm_job_memory_virtual_max`: Maximum virtual memory in `bytes` that can be allocated to this job
- `slurm_job_queued`: Time spent in queue by the job in seconds
#### Partition metrics

Labels of all the partition metrics: `{queue_name}` : 
- `slurm_partition_availability`: Availability status code of the partition. See **State codes** below for details.
- `slurm_partition_cores_total`: Number of cores in this partition 
- `slurm_partition_cores_alloc`: Number of cores allocated to a job in this partition 
- `slurm_partition_cores_idle`: Number of idle cores in this partition 
## Usage

### Standalone
1. Download the code
2. Enter the folder and build it with go
```
go build
```
3. Run the exporter
```
hpc_exporter -host <HOST> -listen-address <PORT> -scheduler <SCHED> [-sacct-history <HISTORY>] -ssh-user <USER> -ssh-auth-method <AUTH> [-ssh-password <PASS> | -ssh-known-hosts <PATH> [-ssh-priv-key-file <PATH> | -ssh-priv-key <KEY>]]  -log-level=<LOGLEVEL> -scrape-interval <INTERVAL>
```
- `<HOST>`: HPC frontend address. `localhost` as default, not supported.
- `<PORT>`: Port the metrics will be exposed on for prometheus. `:9110` as default.   
- `<SCHED>`: Scheduler used in the HPC. `pbs` as default, installed in sodalite-fe.hlrs.de
- `<HISTORY>`: If the scheduler is Slurm, the jobs that will be reported will be the ones that have been submitted in the last `<HISTORY>` days. Default 5.
- `<USER>`: SSH user to connect to the `<HOST>` frontend
- `<AUTH>`: SSH authentication method used on the HPC frontend. See **Authentication methods** below for details
- `<LOGLEVEL>`: Logging level. `error` as default, `warn`, `info` and `debug` also supported
- `<INTERVAL>`: Minimum amount of time in seconds between each query to the HPC frontend. Default 300.

### Dockerization
Check the [README.md](docker/README.md) file in [`docker`](docker) folder for instructions about Docker deployment of HPC exporters

### Authentication methods
Authentication methods supported are:
- `password`: Password SSH authentication. 
    Expects the password to be set with `-ssh-password <PASS>`.
- `keypair-file`: Public-private SSH authentication. 
    Expects the paths to known hosts (`-ssh-known-hosts <PATH>`) and private key (`-ssh-priv-key-file <PATH>`) files of the user.
- `keypair`: Public-private SSH authentication. 
    Expects the path to known hosts (`-ssh-known-hosts <PATH>`) and the private key (`-ssh-priv-key <KEY>`) of the user in plain text (not stored in a file), including header and footer such as `-----BEGIN RSA PRIVATE KEY-----\n` and `\n-----BEGIN RSA PRIVATE KEY-----`.

## State codes

The state of each job and partition is reported as a numerical value. The correspondence between the state and the code is as follows. For more information about what each state means, consult [Slurm](https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES) and [PBS](http://docs.adaptivecomputing.com/torque/4-1-3/Content/topics/commands/qstat.htm) official documentation.

### **PBS job status codes**
CODE|SHORT|LONG
:--|:--- |:---|
 0|`C`|`COMPLETED`
 1|`E`|`EXITING`
 2|`R`|`RUNNING`
 3|`Q`|`QUEUED`
 4|`W`|`WAITING`
 5|`H`|`HELD`
 6|`T`|`TRANSIT`
 7|`S`|`SUSPENDED`

### **SLURM job status codes**
CODE|SHORT|LONG
:--|:--- |:---|
0  |`CD` |`COMPLETED`
1  |`CG` |`COMPLETING`
2  |`SO` |`STAGE_OUT`
3  |`R`  |`RUNNING`
4  |`CF` |`CONFIGURING`
5  |`PD` |`PENDING`
6  |`RQ` |`REQUEUED`
7  |`RF` |`REQUEUE_FED`
8  |`RS` |`RESIZING`
9  |`RD` |`RESV_DEL_HOLD`
10 |`RH` |`REQUEUE_HOLD`
11 |`SI` |`SIGNALING`
12 |`S`  |`SUSPENDED`
13 |`ST` |`STOPPED`
14 |`PR` |`PREEMPTED`
15 |`RV` |`REVOKED`
16 |`SE` |`SPECIAL_EXIT`
17 |`DL` |`DEADLINE`
18 |`TO` |`TIMEOUT`
19 |`OOM`|`OUT_OF_MEMORY`
20 |`BF` |`BOOT_FAIL`
21 |`NF` |`NODE_FAIL`
22 |`F`  |`FAILED`
23 |`CA` |`CANCELLED`

### **SLURM partition status codes**

CODE|STATE
:--|:--- 
 0|`UP`
 1|`DOWN`
 2|`DRAIN`
 3|`INACT`
