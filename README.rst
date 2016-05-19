DCOS Management Subcommand
==========================

## This project is not affiliated with mesosphere

## Subcommand
### maintenance
#### example
```sh
$ dcos management maintenance --list
{}
$ dcos management maintenance --start 1463699601668461056  mesos04.local mesos05.local
$ dcos management maintenance --list
{u'draining_machines': [{u'id': {u'hostname': u'mesos05.local'}}, {u'id': {u'hostname': u'mesos04.local'}}]}
$ dcos management maintenance --now mesos04.local mesos05.local
$ dcos management maintenance --list
{u'down_machines': [{u'hostname': u'mesos05.local'}, {u'hostname': u'mesos04.local'}]}
$ dcos management maintenance --up mesos04.local mesos05.local
$ dcos management maintenance --list
{}
```
