DC/OS Management Subcommands
==========================
Control mesos master via DC/OS CLI

# Subcommands
## maintenance
`maintenance` subcommands allow you to list maintenance status, schedule downtime, mark host as DOWN and make transition from DRAINING or DOWN to UP. 

You still have to make transitions yourself.

To understand how mesos deal with maintenance of agents, please read [mesos documentation](http://mesos.apache.org/documentation/latest/maintenance/).

Also, instead of nanoseconds, `maintenance` subcommands expect seconds. This may change in a future release.

By default, maintenance window (i.e. `--duration`) is set to one hour.

`<hostname>` parameter accepts several type of values:
* a hostname
* an IPv4 address
* a slave ID
* a raw machine_id json: '{"hostname" : "mesos-agent08", "ip": "192.168.99.40"}'. It can be useful to blacklist non provisionned agents.

### Examples
#### Announce a maintenance operation 2 hours from now
```sh
$ dcos management maintenance schedule add --start=$(date "+%s") --duration=7200 dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S5 192.168.100.28
Schedules updated
$ dcos management maintenance list
HOST            IP              ID                                        STATE     START       DURATION
mesos-agent01  192.168.100.27  dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S5   DRAINING  1463955032      7200
mesos-agent01  192.168.100.28  dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S11  DRAINING  1463955032      7200

```
#### Cancel maintenance for agent01
```
$ dcos management maintenance schedule flush 192.168.100.27
Flushing specified host(s)
Schedules updated
$ dcos management maintenance list
HOST            IP              ID                                        STATE     START       DURATION
mesos-agent02  192.168.100.28  dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S11  DRAINING  1463955032      7200
```
#### Mark agent02 as down
```sh
$ dcos management maintenance down  192.168.100.28
submitted hosts are now DOWN
$ dcos management maintenance list
HOST            IP              ID  STATE  START       DURATION
mesos-agent02  192.168.100.28      DOWN   1463955032      7200
$ dcos node
mesos-agent01   192.168.100.27  dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S5
mesos-agent03   192.168.100.30  dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S8
mesos-agent00   192.168.100.8   dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S4

```

Note:
- with the same command, you can force the transition from UP to DOWN
- mesos-agent02 is now out of pool
#### Reactivate agent02
```sh
$ dcos management maintenance up '{"hostname": "mesos-agent02", "ip": "192.168.100.28"}'
submitted hosts are now UP
$ dcos node
   HOSTNAME           IP                           ID
mesos-agent01  192.168.100.27  dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S5
mesos-agent02  192.168.100.28  dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S12
mesos-agent03  192.168.100.30  dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S8
mesos-agent00  192.168.100.8   dd85d3f3-ecc5-4f9d-b851-dc88086834ff-S4

```
### Limitations
- flushing all maintenance schedules silently fails if at least one host is DOWN.
- quite slow
### ToDo
- fix flush when some host are down
- more doc

