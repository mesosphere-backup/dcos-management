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

def get_maintenance_status(dcos_client, hostnames):
    maintenance_status = []
    try:
        url = dcos_client.master_url('maintenance/status')
        req = http.get(url).json()
        if 'draining_machines' in req:
            machine_ids = req['draining_machines']
            for i in range(len(machine_ids)):
                if "ip" in machine_ids[i]['id']:
                    host = machine_ids[i]['id']['ip']
                else:
                    host = machine_ids[i]['id']['hostname']
                maintenance_status.append({'hostname': host, 'state': "DRAINING"})
        if 'down_machines' in req:
            machine_ids = req['down_machines']
            for m in machine_ids:
                if "ip" in m:
                    host = m["ip"]
                else:
                    host = m["hostname"]
                maintenance_status.append({'hostname': host, 'state': 'DOWN'})
    except:
        raise
    found = dict()
    for h in hostnames:
        for j in range(len(maintenance_status)):
            if h == maintenance_status[j]['hostname']:
                found['h'] = 1
    return maintenance_status, found

def get_machine_ids(hostnames):
    machine_ids = []
    for s in hostnames:
        key = ""
        try:
            socket.inet_aton(s)
            key = "ip"
        except:
            key = "hostname"
        item = dict()
        item[key] = s
        machine_ids.append(item)
    return machine_ids

def _maintenance(list, down, start, up, duration, hostnames):
    dcos_client = mesos.DCOSClient()
    maintenance_status, found = get_maintenance_status(dcos_client, hostnames)

    if list:
        table = tables.maintenance_table(maintenance_status)
        output = str(table)
        if output:
            emitter.publish(output)
        return 0

    if not hostnames:
        emitter.publish("You must define at least one hostname")
        return 1

    machine_ids = get_machine_ids(hostnames)

    if up:
        url = dcos_client.master_url('machine/up')
        for h in hostnames:
            isDown = 0
            for j in range(len(maintenance_status)):
                if h == maintenance_status[j]['hostname'] and maintenance_status[j]['state'] == 'DOWN':
                    isDown = 1
                    try:
                        http.post(url, data=None, json=[maintenance_status[j]])
                        emitter.publish(h + " is now UP")
                    except:
                        raise
            if isDown == 0 :
                emitter.publish(h + " is now not down --  ignoring")
        return 0

    if not duration:
        duration = 3600000000000

    url = dcos_client.master_url('maintenance/schedule')
    current_scheduled = http.get(url).json()
    if "windows" not in current_scheduled:
        current_scheduled = None
    jsonSchedData = dict()

    if len(found) == 0:
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
            if current_scheduled:
                jsonSchedData['windows'].append(current_scheduled['windows'])
            http.post(url, data=None, json=jsonSchedData)
        except:
            raise
    if down:
        maintenance_status, found = get_maintenance_status(dcos_client, hostnames)
        url = dcos_client.master_url('machine/down')
        for h in hostnames:
            for j in range(len(maintenance_status)):
                if h == maintenance_status[j]['hostname'] and maintenance_status[j]['state'] == 'DRAINING':
                    try:
                        http.post(url, data=None, json=[maintenance_status[j]])
                        emitter.publish(h + " is now DOWN")
                    except:
                        raise
