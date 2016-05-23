import socket
import json
import time

import sys

from dcos import emitting, http, util, mesos
from dcos_management import tables
from dcos.errors import DCOSException

stons = 1000000000
DEFAULT_DURATION = 3600

emitter = emitting.FlatEmitter()
logger = util.get_logger(__name__)

# First match win
# FIXME: be more pedantic on returns
def find_machine_id(agents, host):
    """
    :param agents: Array of mesos agents
    :type: list
    :param host: Host to find. Can be ip, hostname of agent ID
    :type: string
    :returns: a machine_id-like
    :rtype: dict
    """
    for i in range(len(agents)):
        s = agents[i]
        # down agent
        if 'state' in s:
            if s['state'] == "DOWN":
                s['id'] = ""
        if host in [s['hostname'], s['ip'], s['id']]:
            return {"hostname": s['hostname'], "ip": s['ip']}
    return None

def compare_machine_ids( machine_id_a, machine_id_b):
    """
    :params machine_id_a:
    """
    if machine_id_a['hostname'] == machine_id_b['hostname'] and machine_id_a['ip'] == machine_id_b['ip']:
       return True
    return False

def find_matching_agent(agents, machine_id):
    for i in range(len(agents)):
        if compare_machine_ids(agents[i], machine_id):
            return agents[i]
    return None

def get_maintenance_status(dcos_client, agents=[]):
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
                agent = find_matching_agent(agents,machine_id)
                if not agent:
                    machine_id['id'] = ""
                else:
                    machine_id = agent
                machine_id['state'] = "DRAINING"
                maintenance_status.append(machine_id)
        if 'down_machines' in req:
            machine_ids = req['down_machines']
            for m in machine_ids:
                machine_id = m
                agent = find_matching_agent(agents,machine_id)
                if not agent:
                    machine_id['id'] = ""
                else:
                    machine_id = agent
                machine_id['state'] = "DOWN"
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

def get_machine_ids(hosts, agents=[], maintenance_status=[]):
    machine_ids = []
    md = agents + maintenance_status
    for h in hosts:
        try:
             machine_id = json.loads(h)
             if len(machine_id) == 2 and "ip" in machine_id and "hostname" in machine_id:
                machine_ids.append(machine_id)
             else:
                emitter.publish("malformed unmanaged host entry: " + str(h))
        except ValueError as e:
            machine_id = find_machine_id(md, h)
            if machine_id:
                machine_ids.append(machine_id)
    return machine_ids

def filter_agents(machine_ids=[], maintenance_status=[]):
    not_scheduled = []
    down = []
    draining = []
    for m in range(len(machine_ids)):
        scheduled = False
        for s in range(len(maintenance_status)):
            if compare_machine_ids(machine_ids[m],maintenance_status[s]):
                if maintenance_status[s]['state'] == "DOWN":
                    down.append(machine_ids[m])
                if maintenance_status[s]['state'] == "DRAINING":
                    draining.append(machine_ids[m])
                scheduled = True
                break
        if not scheduled:
            not_scheduled.append(machine_ids[m])
    return not_scheduled, down, draining


class Maintenance():
    """ Maintenance """
    """ init is slow """
    def __init__(self,hosts=[]):
        self.dcos_client = mesos.DCOSClient()
        self.agents = []
        self.scheduled = None
        self.maintenance_status = None
        self.machine_ids = []

        self.get_agents()
        self.get_scheduled()
        self.get_maintenance_status()
        self.get_machines_ids(hosts)

    def get_agents(self):
        _agents = self.dcos_client.get_state_summary()['slaves']
        for i in range(len(_agents)):
            s = _agents[i]
            self.agents.append(
                {"hostname": s['hostname'],
                "ip": mesos.parse_pid(s['pid'])[1], "id": s['id']
                }
            )
    def get_all_agents(self):
        md = self.agents + self.maintenance_status
        agents = []
        for i in md:
            if i not in agents:
                agents.append({"hostname": i["hostname"], "ip": i["ip"]})
        return agents

    def get_scheduled(self, force=False):
        if not self.scheduled or force:
            self.scheduled = get_scheduled(self.dcos_client)

    def get_maintenance_status(self, force=False):
        if not self.maintenance_status or force:
            self.maintenance_status = get_maintenance_status(self.dcos_client,
                                                                agents=self.agents)

    def get_machines_ids(self,hosts):
        self.machine_ids = get_machine_ids(hosts, agents=self.agents, maintenance_status=self.maintenance_status)


    def list(self, json_):
        full_maintenance_status = []
        for e in self.maintenance_status:
            e['start'] = "None"
            e['duration'] = None
            if self.scheduled:
                for i in range(len(self.scheduled['windows'])):
                    sched = self.scheduled['windows'][i]
                    for j in range(len(sched['machine_ids'])):
                        if compare_machine_ids(sched['machine_ids'][j],e):
                            e['start'] = long(sched['unavailability']['start']['nanoseconds']) / stons
                            e['duration'] = long(sched['unavailability']['duration']['nanoseconds']) / stons
                            full_maintenance_status.append(e)

        emitting.publish_table(emitter, full_maintenance_status, tables.maintenance_table, json_)

    def flush_all(self):
        not_scheduled, down, draining = filter_agents(self.get_all_agents(),self.maintenance_status)
        self.flush(draining)

    def flush(self, m_ids=[]):
        if len(m_ids) == 0:
            m_ids = self.machine_ids
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
                for h in m_ids:
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

    def schedule_maintenance(self,start,duration, m_ids = []):
        if len(m_ids) == 0:
            m_ids = self.machine_ids
        up, down, draining = filter_agents(m_ids, self.maintenance_status)

        machine_ids = up + draining
        if len(machine_ids) == 0:
            emitter.publish("Agents are already DOWN")

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
            "start" : { "nanoseconds": start },
            "duration" : { "nanoseconds": duration }
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

    def up_all(self):
        not_scheduled, down, draining = filter_agents(self.get_all_agents(),self.maintenance_status)
        self.up(down)
        self.flush(draining)

    def up(self,m_ids=[]):
        if len(m_ids) == 0:
            m_ids = self.machine_ids
        not_scheduled, down, draining = filter_agents(m_ids, self.maintenance_status)
        if len(draining) > 0:
            self.flush(machine_ids=draining)

        try:
            url = self.dcos_client.master_url('machine/up')
            http.post(url, data=None, json=down)
            emitter.publish("submitted hosts are now UP")
        except DCOSException as e:
            logger.exception(e)
        except Exception:
            raise DCOSException("Can't complete operation on mesos master")
        return 0

    def down(self, m_ids=[]):
        if len(m_ids) == 0:
            m_ids = self.machine_ids
        not_scheduled, down, draining = filter_agents(m_ids, self.maintenance_status)

        self.schedule_maintenance(None,None, not_scheduled)

        to_down = not_scheduled + draining

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

def up(hosts, all):
    m = Maintenance(hosts=hosts)
    if all:
        m.up_all()
    else:
        if len(hosts) == 0:
            emitter.publish("You must defined at least one host")
            return 0
        m.up()

def down(hosts):
    m = Maintenance(hosts=hosts)
    m.down()

def flush_schedule(hosts, all):
    m = Maintenance(hosts=hosts)
    if all:
        m.flush_all()
    else:
        if len(hosts) == 0:
            emitter.publish("You must defined at least one host")
            return 0
        m.flush()

def schedule_maintenance(start, duration, hosts):
    m = Maintenance(hosts=hosts)
    m.schedule_maintenance(start, duration)
