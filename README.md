# hpc-exporter

This exporter connects via ssh to the frontend of a given HPC infrastructure and queries the scheduler in order to collect metrics of user jobs and expose them in Prometheus format.

## Metrics collected for supported schedulers
### PBS Professional
- `pbs_qstat_u_jobstate{exit_status,job_id,job_name,job_state,username}`: Job state numeric code
- `pbs_qstat_u_jobstate{exit_status,job_id,job_name,job_state,username}`: Exit status numeric code
- `pbs_qstat_u_totalruntime{comp_time,exit_status,job_id,job_name,job_state,start_time,username}`: `total_runtime` in seconds
- `pbs_qstat_u_consumedcputime{comp_time,exit_status,job_id,job_name,job_state,start_time,username}`: `resources_used.cput` in seconds
- `pbs_qstat_u_consumedwalltime{comp_time,exit_status,job_id,job_name,job_state,start_time,username}`: `resources_used.walltime` in seconds
- `pbs_qstat_u_consumedpmem{exit_status,job_id,job_name,job_state,units,username}`: `resources_used.mem` in `units`
- `pbs_qstat_u_consumedvmem{exit_status,job_id,job_name,job_state,units,username}`: `resources_used.vmem` in `units`

### SLURM
- `slurm_jobstate{exit_status_full,job_id,job_name,job_state,username}`: Job state numeric code
- `slurm_jobexitstatus1{exit_status_full,job_id,job_name,job_state,username}`: LHS of X:X exit status code
- `slurm_jobexitstatus2{exit_status_full,job_id,job_name,job_state,username}`: RHS of X:X exit status code
- `slurm_jobwalltime{exit_status_full,job_id,job_name,job_state,username,wall_time}`: Job elapsed wall time in seconds

## Usage
1. Download the code
2. Enter the folder and build it with go
```
go build
```
3. Run the exporter
```
hpc_exporter -host <HOST> -listen-address <PORT> -scheduler <SCHED> -ssh-user <USER> -ssh-auth-method <AUTH>
[-ssh-password <PASS> | -ssh-known-hosts <PATH> -ssh-private-key <PATH>]  -log.level=<LOGLEVEL> [-target-job-ids <JOBLIST>]
```
- `<HOST>`: `localhost` as default, not supported 
- `<PORT>`: `:9100` as default, any free port upper to   
- `<SCHED>`: `pbs` as default, installed in sodalite-fe.hlrs.de
- `<USER>`: SSH user to connect to the `<HOST>` frontend
- `<AUTH>`: see **Authentication methods** below for details
- `<LOGLEVEL>`: `error` as default, `info` and `debug` also supported
- `<JOBLIST>`: see **Targeting specific user jobs** for details

### Authentication methods
Authentication methods supported are `password` and `keypair`.
- The `password` method needs the clear password to be set with `-ssh-password <PASS>`.
- The `keypair` expects the paths to known hosts (`-ssh-known-hosts <PATH>`) and private key (`-ssh-private-key <PATH>`) files of the user.

### Targeting specific user jobs
The parameter `-target-job-ids <JOBLIST>` expects a comma-separated list of specific user jobs to be monitored. **This parameter is mandatory for SLURM schedulers**. For PBS schedulers can be omitted to monitor all the jobs launched by the user.
