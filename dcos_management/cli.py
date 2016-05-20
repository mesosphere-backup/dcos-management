"""DCOS Management stuff

Usage:
    dcos management --info
    dcos management maintenance (--list | --down | --start=<date> | --up ) [--duration=<duration>] [<hostname>...]

Options:
    --help           Show this screen
    --version        Show version
"""
# check IP (crappy)
import socket
import json
import time

import docopt
from dcos import cmds, emitting, http, util, mesos
from dcos.errors import DCOSException
from dcos_management import constants
from dcos_management import tables
from dcoscli.util import decorate_docopt_usage

emitter = emitting.FlatEmitter()
logger = util.get_logger(__name__)


def main():
    try:
        return _main()
    except DCOSException as e:
        emitter.publish(e)
        return 1

@decorate_docopt_usage
def _main():
    util.configure_process_from_environ()

    args = docopt.docopt(
        __doc__,
        version='dcos-management version {}'.format(constants.version))

    http.silence_requests_warnings()

    return cmds.execute(_cmds(), args)


def _cmds():
    """
    :returns: All of the supported commands
    :rtype: [Command]
    """

    return [
        cmds.Command(
            hierarchy=['management', '--info'],
            arg_keys=[],
            function=_info),

        cmds.Command(
            hierarchy=['management','maintenance'],
            arg_keys=['--list','--down', '--start', '--up', '--duration', '<hostname>'],
            function=_maintenance),
    ]

def _info():
    emitter.publish(__doc__.split('\n')[0])
    return 0

def _maintenance(list, down, start, up, duration, hostname):
    dcos_client = mesos.DCOSClient()
    maintenance_status = []
    try:
        url = dcos_client.master_url('maintenance/status')
        req = http.get(url).json()
        if 'draining_machines' in req:
            machined_ids = req['draining_machines']
            for i in range(len(machined_ids)):
                if "ip" in machined_ids[i]['id']:
                    host = machined_ids[i]['id']['ip']
                else:
                    host = machined_ids[i]['id']['hostname']
                maintenance_status.append({'hostname': host, 'state': "DRAINING"})
        if 'down_machines' in req:
            machined_ids = req['down_machines']
            for m in machined_ids:
                if "ip" in m:
                    host = m["ip"]
                else:
                    host = m["hostname"]
                maintenance_status.append({'hostname': host, 'state': 'DOWN'})
    except:
        raise

    if list:
        table = tables.maintenance_table(maintenance_status)
        output = str(table)
        if output:
            emitter.publish(output)
        return 0

    if not hostname:
        return 42

    machine_ids = []
    for s in hostname:
        key = ""
        try:
            socket.inet_aton(s)
            key = "ip"
        except:
            key = "hostname"
        item = dict()
        item[key] = s
        machine_ids.append(item)

    if up:
        url = dcos_client.master_url('machine/up')
        try:
            http.post(url, data=None, json=machine_ids)
            return 0
        except:
            raise
    found = None
    for h in hostname:
        for j in range(len(maintenance_status)):
            if h == maintenance_status[j]['hostname']:
                found = 1

    if not duration:
        duration = 3600000000000

    url = dcos_client.master_url('maintenance/schedule')
    jsonSchedData = dict()

    if not found:
        jsonSchedData['windows'] = []
        starttime = long((time.time()) * 1000000000 if not start else start)
        unavailibitiyObj= dict()
        unavailibitiyObj['machine_ids'] = machine_ids
        unavailibitiyObj['unavailability']= {
        "start" : { 'nanoseconds': starttime },
        'duration' : { 'nanoseconds': long(duration) }
        }
        jsonSchedData['windows'].append(unavailibitiyObj)
        try:
            url = dcos_client.master_url('maintenance/schedule')
            http.post(url, data=None, json=jsonSchedData)
        except:
            raise

    if down:
        try:
            url = dcos_client.master_url('machine/down')
            http.post(url, data=None, json=machine_ids)
        except:
            raise
