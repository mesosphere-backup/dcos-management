DCOS Management Subcommand
==========================


## Subcommands
### maintenance
You still have to mark your node down.
#### Example
```sh
$ dcos management maintenance --list
$ dcos management maintenance --start $(date "+%s00000000")  --duration 36000000000000 mesos04.local mesos05.local
$ dcos management maintenance --list
HOST           STATE
mesos04.local  DRAINING
mesos05.local  DRAINING
$ dcos management maintenance --down mesos05.local
$ dcos management maintenance --list
HOST           STATE
mesos05.local  DOWN
mesos04.local  DRAINING
$ dcos management maintenance --up mesos05.local
$ dcos management maintenance --list  
HOST           STATE
mesos04.local  DRAINING

```
#### Limitations
- you can set up only one maintenance window at a time
- you can't make a transition from DRAINING to UP directly. You must mark it as DOWN first

