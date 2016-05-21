import socket
import json
import time

from dcos import emitting, http, util, mesos
from dcos_management import tables
from dcos.errors import DCOSException

stons = 1000000000
DEFAULT_DURATION = 3600

emitter = emitting.FlatEmitter()
logger = util.get_logger(__name__)

def get_maintenance_status(dcos_client, hostnames=[]):
    """
    :param dcos_client: DCOSClient
    :type dcos_client: DCOSClient
    :params hostnames: list of hostnames
    :type: list
    :returns: list of dict of maintenance status, list found in maintenance status
    :rtype: list,list
    """
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
    except DCOSException as e:
        logger.exception(e)
    except Exception:
        raise DCOSException("Unable to fetch maintenance status from mesos master")

    found = dict()
    for h in hostnames:
        for j in range(len(maintenance_status)):
            if h == maintenance_status[j]['hostname']:
                found[h] = 1
    return maintenance_status, found

def get_schedules(dcos_client):
    """
    :param dcos_client: DCOSClient
    :type dcos_client: DCOSClient
    :returns: an object of maintenance schedule
    :rtype: dict of array
    """
    try:
        url = dcos_client.master_url('maintenance/schedule')
        current_scheduled = http.get(url).json()
        if "windows" not in current_scheduled:
            return None
        return current_scheduled
    except DCOSException as e:
        logger.exception(e)
    except Exception:
        raise DCOSException("Unable to fetch scheduled maintenance windows from mesos master")

def get_machine_ids(hostnames):
    """
    :param hostnames:
    :type hostnames: array
    :retuns:
    :rtype:
    """
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

def get_machine_ids_dict(hostnames):
    """
    :param hostnames:
    :type hostnames: array
    :retuns:
    :rtype: dict()
    """
    machine_ids = dict()

    for s in hostnames:
        machine_ids[s] = 1

    return machine_ids

def list(json_):
    dcos_client = mesos.DCOSClient()
    full_maintenance_status = []

    maintenance_status, _ = get_maintenance_status(dcos_client)
    current_scheduled = get_schedules(dcos_client)
    # # XXX: fix me python 3.x
    # machine_ids = get_machine_ids_dict(maintenance_status[0].keys())

    for e in maintenance_status:
        e["start"] = "None"
        e["duration"] = None
        if current_scheduled:
            for i in range(len(current_scheduled['windows'])):
                sched = current_scheduled['windows'][i]
                for j in range(len(sched['machine_ids'])):
                    if 'ip' in sched['machine_ids'][j]:
                        t = sched['machine_ids'][j]['ip']
                    else:
                        t = sched['machine_ids'][j]['hostname']
                    if t == e["hostname"]:
                        e["start"] = long(sched['unavailability']['start']['nanoseconds']) / stons
                        e["duration"] = long(sched['unavailability']['duration']['nanoseconds']) / stons
        full_maintenance_status.append(e)


    emitting.publish_table(emitter, full_maintenance_status, tables.maintenance_table, json_)

def up(hostnames):
    """
    :param hostnames:
    :type hostnames: array
    :retuns:
    :rtype:
    """
    if not hostnames:
        raise DCOSException("You must define at least one hostname")

    dcos_client = mesos.DCOSClient()
    maintenance_status, found = get_maintenance_status(dcos_client, hostnames)
    if len(maintenance_status) == 0:
        emitter.publish("No maintenance schedule found on mesos master")
        return 0

    to_up = []
    for h in hostnames:
        toSkip = mayBeUp = isDown = isDraining = 0
        for j in range(len(maintenance_status)):
            if h != maintenance_status[j]['hostname']:
                toSkip = 1
                continue
            if maintenance_status[j]['state'] == 'DOWN':
                to_up.append(maintenance_status[j])
            elif maintenance_status[j]['state'] == 'DRAINING':
                emitter.publish(h + " is now not down --  flushing schedule")
                schedule_maintenance(None, True, None, [h])
            else:
                emitter.publish(h + " has no schedule for maintenance")
    try:
        url = dcos_client.master_url('machine/up')
        http.post(url, data=None, json=to_up)
        emitter.publish("submitted hosts are now UP")
    except DCOSException as e:
        logger.exception(e)
    except Exception:
        raise DCOSException("Can't complete operation on mesos master")
    return 0

def down(hostnames):
    """
    :param hostnames:
    :type hostnames: array
    :retuns:
    :rtype:
    """
    dcos_client = mesos.DCOSClient()
    maintenance_status, found = get_maintenance_status(dcos_client, hostnames)
    if not hostnames:
        raise DCOSException("You must define at least one hostname")

    maintenance_status, found = get_maintenance_status(dcos_client, hostnames)
    url = dcos_client.master_url('machine/down')
    for h in hostnames:
        if h not in found:
            schedule_maintenance(None,False,None,[h])
            # XXX: should we refresh ?
            maintenance_status, found = get_maintenance_status(dcos_client, hostnames)
        for j in range(len(maintenance_status)):
            if h == maintenance_status[j]['hostname'] and maintenance_status[j]['state'] == 'DRAINING':
                try:
                    http.post(url, data=None, json=[maintenance_status[j]])
                    emitter.publish(h + " is now DOWN")
                except DCOSException as e:
                    logger.exception(e)
                except Exception:
                    raise DCOSException("Can't complete operation on mesos master")
            if h == maintenance_status[j]['hostname'] and maintenance_status[j]['state'] == 'DOWN':
                emitter.publish(h + " is already down --  ignoring")
    return 0


def schedule_maintenance(start, flush, duration, hostnames):
    """
    """
    dcos_client = mesos.DCOSClient()
    maintenance_status, found = get_maintenance_status(dcos_client, hostnames)
    if flush and len(hostnames) == 0:
        if len(maintenance_status) == 0:
            emitter.publish("No maintenance schedule found on mesos master")
            return 0
        try:
            url = dcos_client.master_url('maintenance/schedule')
            http.post(url, data=None, json={})
            emitter.publish("Scheduled maintenances flushed")
            return 0
        except DCOSException as e:
            logger.exception(e)
            emitter.publish("e")
        except Exception:
            raise DCOSException("Can't complete operation on mesos master")

    if not hostnames:
        emitter.publish("You must define at least one hostname")
        return 1

    machine_ids = get_machine_ids(hostnames)

    if not duration:
        duration = long( DEFAULT_DURATION * stons)
    else:
        duration = long(int(duration) * stons)

    if not start:
        start = long(time.time() * stons )
    else:
        start = long(int(start) * stons)

    jsonSchedData = dict()
    cur_shed = get_schedules(dcos_client)
    if not cur_shed:
        jsonSchedData['windows'] = []
    else:
        jsonSchedData = cur_shed
        # remove conflicting entries
        to_popi = []
        for i in range(len(jsonSchedData['windows'])):
            sched = jsonSchedData['windows'][i]
            to_popj = []
            for j in range(len(sched['machine_ids'])):
                if 'ip' in sched['machine_ids'][j]:
                    t = sched['machine_ids'][j]['ip']
                else:
                    t = sched['machine_ids'][j]['hostname']
                for h in hostnames:
                    if h == t:
                        to_popj.append(j)
                        break
            # remove
            offset = 0
            for jj in to_popj:
                sched['machine_ids'].pop(jj - offset)
                offset += 1
            if len(sched['machine_ids']) == 0:
                to_popi.append(i)
        offset = 0
        for ii in to_popi:
            jsonSchedData['windows'].pop(ii - offset)
            offset += 1
    if not flush:
        unavailibitiyObj= dict()
        unavailibitiyObj['machine_ids'] = machine_ids
        unavailibitiyObj['unavailability']= {
        "start" : { 'nanoseconds': start },
        'duration' : { 'nanoseconds': duration }
        }
        jsonSchedData['windows'].append(unavailibitiyObj)
    else:
        emitter.publish("Flushing specified host(s)")
    try:
        url = dcos_client.master_url('maintenance/schedule')
        http.post(url, data=None, json=jsonSchedData)
        emitter.publish("Schedules updated")
    except DCOSException as e:
        logger.exception(e)
    except Exception:
        raise DCOSException("Can't complete operation on mesos master")
