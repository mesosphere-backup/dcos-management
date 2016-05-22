DC/OS Management Subcommands
==========================
Control mesos master via DC/OS CLI

# Subcommands
## maintenance
`maintenance` subcommands allow you to list maintenance status, schedule downtime, mark host as DOWN and make transition from DRAINING or DOWN to UP. You still have to make transition from DRAINING to DOWN.

To understand how mesos deal with maintenance of agents, please read [mesos documentation](http://mesos.apache.org/documentation/latest/maintenance/).

Also, instead of nanoseconds, `maintenance` subcommands expect seconds. This may change in a future release.

By default, maintenance window (i.e. `--duration`) is set to one hour.

### Examples
#### Announce reverse offers from now, for 2 hours 
```sh
$ dcos management maintenance schedule --start=$(date "+%s") --duration=7200 mesos-agent00.local mesos-agent01.local mesos-agent02.local mesos-agent03.local mesos-agent05.local
Schedules updated
$ dcos management maintenance list
HOST                 STATE     START       DURATION  
mesos-agent00.local  DRAINING  1463862657      7200  
mesos-agent01.local  DRAINING  1463862657      7200  
mesos-agent02.local  DRAINING  1463862657      7200  
mesos-agent03.local  DRAINING  1463862657      7200  
mesos-agent05.local  DRAINING  1463862657      7200 
```
#### Cancel maintenance for agent00 and agent05 
```
$ dcos management maintenance schedule --flush mesos-agent00.local mesos-agent05.local
Flushing specified host(s)
Schedules updated
$ dcos management maintenance list
HOST                 STATE     START       DURATION  
mesos-agent01.local  DRAINING  1463862657      7200  
mesos-agent02.local  DRAINING  1463862657      7200  
mesos-agent03.local  DRAINING  1463862657      7200  
```
#### Mark agent01 as down
```sh
$ dcos management maintenance down mesos-agent01.local
mesos-agent01.local is now DOWN
$ dcos management maintenance list
HOST                 STATE     START       DURATION  
mesos-agent01.local  DOWN      1463862657      7200  
mesos-agent02.local  DRAINING  1463862657      7200  
mesos-agent03.local  DRAINING  1463862657      7200  

```
#### Reactivate agent01 and agent02
```sh
$ dcos management maintenance up mesos-agent01.local mesos-agent02.local
mesos-agent02.local is now not down --  flushing schedule
Flushing specified host(s)
Schedules updated
submitted hosts are now UP
$ dcos management maintenance list
HOST                 STATE     START       DURATION  
mesos-agent03.local  DRAINING  1463862657      7200  

```
#### Cancel remaining scheduled maintenance
```sh
$ dcos management maintenance schedule --flush 
Scheduled maintenances flushed
$ dcos management maintenance list
$ 
```
#### Emergency shutdown of agent00
```sh
$ dcos management maintenance down mesos-agent00.local 
Schedules updated
mesos-agent00.local is now DOWN
$ dcos management maintenance list
HOST                 STATE  START       DURATION  
mesos-agent00.local  DOWN   1463863101      3600  

```
### Limitations
- flushing all maintenance schedules silently fails if at least one host is DOWN.
- marking an host as DOWN is a sequential operation
- only works wiht IP or hostname

### ToDo
- fix flush when some host are down
- more doc

### Known bugs
- up and down operation are broken with IP
