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

# XXX: first match win
def find_machine_ids(slaves, host):
    for i in range(len(slaves)):
        s = slaves[i]
        if host in [s["hostname"], s["ip"], s["id"]]:
            return {"hostname": s["hostname"], "ip": s["ip"]}
    return None

def compare_machine_ids( machine_id_a, machine_id_b):
    if machine_id_a["hostname"] == machine_id_b["hostname"] and machine_id_a["ip"] == machine_id_b["ip"]:
       return True
    return False

def find_matching_slave(slaves, machine_id):
    for i in range(len(slaves)):
        if compare_machine_ids(slaves[i], machine_id):
            return slaves[i]
    return None

def get_maintenance_status(dcos_client, slaves=[]):
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
        # XXX: to refactor
        if 'draining_machines' in req:
            machine_ids = req['draining_machines']
            for m in machine_ids:
                machine_id = m['id']
                slave = find_matching_slave(slaves,machine_id)
                if not slave:
                    machine_id["id"] = ""
                else:
                    machine_id = slave
                machine_id["state"] = "DRAINING"
                maintenance_status.append(machine_id)
        if 'down_machines' in req:
            machine_ids = req['down_machines']
            for m in machine_ids:
                machine_id = m
                slave = find_matching_slave(slaves,machine_id)
                if not slave:
                    machine_id["id"] = ""
                else:
                    machine_id = slave
                machine_id["state"] = "DOWN"
                maintenance_status.append(machine_id)
        return maintenance_status
    except DCOSException as e:
        logger.exception(e)
    except Exception as (e):
        raise DCOSException("Unable to fetch maintenance status from mesos master: " + str(e))

def get_scheduled(dcos_client):
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


class Maintenance():
    """ Maintenance """
    def __init__(self,hosts=[]):
        self.dcos_client = mesos.DCOSClient()
        self.hosts = dict()
        self.slaves = []
        self.machine_ids = []
        self.maintenance_status = None
        self.scheduled = None
        self.get_slaves()
        self.get_scheduled()
        if len(hosts) != 0:
            for h in hosts:
                try:
                     machine_id = json.loads(h)
                     if len(machine_id) == 2 and "ip" in machine_id and "hostname" in machine_id:
                        self.machine_ids.append(machine_id)
                     else:
                        emitter.publish("malformed unmanaged host entry: " + str(h))
                except ValueError as e:
                    machine_id = find_machine_ids(self.slaves, h)
                    if machine_id:
                        self.machine_ids.append(machine_id)

    def get_slaves(self):
        _slaves = self.dcos_client.get_state_summary()['slaves']
        for i in range(len(_slaves)):
            s = _slaves[i]
            self.slaves.append(
                {"hostname": s["hostname"],
                "ip": mesos.parse_pid(s['pid'])[1], "id": s["id"]
                }
            )
    def get_maintenance_status(self, force=False):
        if not self.maintenance_status or force:
            self.maintenance_status = get_maintenance_status(self.dcos_client,
                                                                slaves=self.slaves)

    def get_scheduled(self, force=False):
        if not self.scheduled or force:
            self.scheduled = get_scheduled(self.dcos_client)

    def list(self, json_):
        self.get_maintenance_status()
        full_maintenance_status = []
        for e in self.maintenance_status:
            e["start"] = "None"
            e["duration"] = None
            if self.scheduled:
                for i in range(len(self.scheduled['windows'])):
                    sched = self.scheduled['windows'][i]
                    for j in range(len(sched['machine_ids'])):
                        if compare_machine_ids(sched['machine_ids'][j],e):
                            e["start"] = long(sched['unavailability']['start']['nanoseconds']) / stons
                            e["duration"] = long(sched['unavailability']['duration']['nanoseconds']) / stons
                            full_maintenance_status.append(e)
        emitting.publish_table(emitter, full_maintenance_status, tables.maintenance_table, json_)

    def flush_all(self):
        if not self.scheduled:
            emitter.publish("No maintenance schedule found on mesos master")
            return 0
        try:
            url = self.dcos_client.master_url('maintenance/schedule')
            http.post(url, data=None, json={})
            emitter.publish("Scheduled maintenances flushed")
            return 0
        except DCOSException as e:
            logger.exception(e)
            emitter.publish("e")
        except Exception:
            raise DCOSException("Can't complete operation on mesos master")

    def flush(self, machine_ids=[]):
        if len(machine_ids) == 0:
            machine_ids = self.machine_ids
        if not self.scheduled:
            emitter.publish("No maintenance schedule found on mesos master")
            return 0
        # remove conflicting entries
        to_popi = []
        for i in range(len(self.scheduled['windows'])):
            sched = self.scheduled['windows'][i]
            to_popj = []
            for j in range(len(sched['machine_ids'])):
                s = sched['machine_ids'][j]
                for h in machine_ids:
                    if compare_machine_ids(s, h):
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
            self.scheduled['windows'].pop(ii - offset)
            offset += 1
        emitter.publish("Flushing specified host(s)")
        try:
            url = self.dcos_client.master_url('maintenance/schedule')
            http.post(url, data=None, json=self.scheduled)
            emitter.publish("Schedules updated")
        except DCOSException as e:
            logger.exception(e)
        except Exception:
            raise DCOSException("Can't complete operation on mesos master")

    def schedule_maintenance(self,start,duration):
        if not duration:
            duration = long( DEFAULT_DURATION * stons)
        else:
            duration = long(int(duration) * stons)

        if not start:
            start = long(time.time() * stons )
        else:
            start = long(int(start) * stons)

        if not self.scheduled:
            self.scheduled = dict()
            self.scheduled['windows'] = []
        unavailibitiyObj= dict()
        unavailibitiyObj['machine_ids'] = self.machine_ids
        unavailibitiyObj['unavailability']= {
        "start" : { 'nanoseconds': start },
        'duration' : { 'nanoseconds': duration }
        }
        self.scheduled['windows'].append(unavailibitiyObj)

        try:
            url = self.dcos_client.master_url('maintenance/schedule')
            http.post(url, data=None, json=self.scheduled)
            emitter.publish("Schedules updated")
        except DCOSException as e:
            logger.exception(e)
        except Exception:
            raise DCOSException("Can't complete operation on mesos master")

    def up(self):
        to_up = []
        to_flush = []
        self.get_maintenance_status()
        for m in range(len(self.machine_ids)):
            for s in range(len(self.maintenance_status)):
                if compare_machine_ids(self.machine_ids[m],self.maintenance_status[s]):
                    add = {"hostname": self.maintenance_status[s]["hostname"], "ip": self.maintenance_status[s]["ip"]}
                    if self.maintenance_status[s]['state'] == "DOWN":
                        to_up.append(add)
                    if self.maintenance_status[s]['state'] == "DRAINING":
                        to_flush.append(add)
        if len(to_flush) > 0:
            self.flush(machine_ids=to_flush)
        try:
            url = self.dcos_client.master_url('machine/up')
            http.post(url, data=None, json=to_up)
            emitter.publish("submitted hosts are now UP")
        except DCOSException as e:
            logger.exception(e)
        except Exception:
            raise DCOSException("Can't complete operation on mesos master")
        return 0

    def down(self):
        to_down = []
        self.get_maintenance_status()
        for m in range(len(self.machine_ids)):
            for s in range(len(self.maintenance_status)):
                if compare_machine_ids(self.machine_ids[m],self.maintenance_status[s]):
                    add = {"hostname": self.maintenance_status[s]["hostname"], "ip": self.maintenance_status[s]["ip"]}
                    if self.maintenance_status[s]['state'] == "DRAINING":
                        to_down.append(add)
        try:
            url = self.dcos_client.master_url('machine/down')
            http.post(url, data=None, json=to_down)
            emitter.publish("submitted hosts are now DOWN")
        except DCOSException as e:
            logger.exception(e)
        except Exception:
            raise DCOSException("Can't complete operation on mesos master")
        return 0

def list(json_):
    m = Maintenance()
    m.list(json_)

def up(hosts):
    m = Maintenance(hosts=hosts)
    m.up()

def down(hosts):
    m = Maintenance(hosts=hosts)
    m.down()

def flush_schedule(hosts):
    m = Maintenance(hosts=hosts)
    if len(hosts) == 0:
        m.flush_all()
    else:
        m.flush()


def schedule_maintenance(start, duration, hosts):
    m = Maintenance(hosts=hosts)
    m.schedule_maintenance(start, duration)
