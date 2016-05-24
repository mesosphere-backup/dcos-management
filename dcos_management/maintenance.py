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

# First match win
# FIXME: be more pedantic on returns
def find_machine_id(agents, host):
    """
    :param agents: Array of mesos agents properties (machine_id + additional infos)
    :type: list of dict
    :param host: Host to find. Can be ip, hostname of agent ID
    :type: string
    :returns: a machine_id
    :rtype: dict
    """
    for agent in agents:
        # down agent
        if 'state' in agent:
            if agent['state'] == "DOWN":
                agent['id'] = ""
        if host in [agent['hostname'], agent['ip'], agent['id']]:
            return {"hostname": agent['hostname'], "ip": agent['ip']}
    return None

def compare_machine_ids( machine_id_a, machine_id_b):
    """
    :param machine_id_a: machine_id
    :type: dict
    :param machine_id_a: machine_id
    :type: dict
    :return: true if both machine_id match, else False
    :rtype: boolean
    """
    return machine_id_a['hostname'] == machine_id_b['hostname'] and machine_id_a['ip'] == machine_id_b['ip']

def find_matching_agent(agents, machine_id):
    """
    :param agents: a list machine_id (dict)
    :type: array of dict
    :param machine_id: a machine_id dict
    :type: dict
    :return: machine_id if found
    :rtype: dict
    """
    for agent in agents:
        if compare_machine_ids(agent, machine_id):
            return agent
    return None

def lookup_and_tag(machine_ids, state, agents, attribute=None):
    """
    :param machine_ids: An object containing extended machine ids dicts
    :type: list of dict
    :param state: State arbitrally associated with machine_ids
    :type: string
    :param agents: A list of extended machine ids dict from agents list
    :type: list of dict
    :param attribute: key to locate machine_ids inside `machines_ids`
    :type: string
    :return: tagged agents
    :rtype: array of dict

    """
    _machine_ids = []
    for m in machine_ids:
        if attribute:
            machine_id = m[attribute]
        else:
            machine_id = m
        agent = find_matching_agent(agents,machine_id)
        if not agent:
            machine_id['id'] = ""
        else:
            machine_id = agent
        machine_id['state'] = state
        _machine_ids.append(machine_id)
    return _machine_ids

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
            maintenance_status.extend(lookup_and_tag(req['draining_machines'], "DRAINING", agents, "id"))
        if 'down_machines' in req:
            maintenance_status.extend(lookup_and_tag(req['down_machines'], "DOWN", agents, None))
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
    """
    :param host:
    :type:
    :param agents:
    :type:
    :param maintenance_status: list of dict of maintenance status,
    :type:
    :returns: list of  machine_ids
    :rtype: list of machine_id, dict
    """
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
    for machine_id in machine_ids:
        scheduled = False
        for ms in maintenance_status:
            if compare_machine_ids(machine_id,ms):
                if ms['state'] == "DOWN":
                    down.append(machine_id)
                if ms['state'] == "DRAINING":
                    draining.append(machine_id)
                scheduled = True
                break
        if not scheduled:
            not_scheduled.append(machine_id)
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
        self.full_maintenance_status = []

        self.get_agents()
        self.get_scheduled()
        self.get_maintenance_status()
        self.get_machines_ids(hosts)
        self.get_full_maintenance_status()

    def get_agents(self):
        _agents = self.dcos_client.get_state_summary()['slaves']
        for agent in agents:
            self.agents.append(
                {"hostname": agent['hostname'],
                "ip": mesos.parse_pid(agent['pid'])[1], "id": agent['id']
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


    def get_full_maintenance_status(self):
        for host in self.maintenance_status:
            host['start'] = "None"
            host['duration'] = None
            if self.scheduled:
                for schedule in self.scheduled['windows']:
                    for scheduled_host in schedule['machine_ids']:
                        if compare_machine_ids(scheduled_host,host):
                            host['start'] = long(schedule['unavailability']['start']['nanoseconds']) / stons
                            host['duration'] = long(schedule['unavailability']['duration']['nanoseconds']) / stons
                            host["expired"] = False if (time.time() < (host['start'] + host['duration'])) else True
                            self.full_maintenance_status.append(host)

    def list(self, json_):
        emitting.publish_table(emitter, self.full_maintenance_status, tables.maintenance_table, json_)

    def flush_all(self):
        not_scheduled, down, draining = filter_agents(self.get_all_agents(),self.full_maintenance_status)
        self.flush(draining)

    # WIP
    #   def flush_expired(self):
    #     not_scheduled, down, draining = filter_agents(self.get_all_agents(),self.full_maintenance_status, True)
    #     self.flush(draining)

    def flush(self, m_ids=[]):
        if len(m_ids) == 0:
            m_ids = self.machine_ids
        if not self.scheduled:
            emitter.publish("No maintenance schedule found on mesos master")
            return 0
        # remove conflicting entries
        to_popi = []
        for sched in self.scheduled['windows']:
            to_popj = []
            for s in sched['machine_ids']:
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
        up, down, draining = filter_agents(m_ids, self.full_maintenance_status)

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
        not_scheduled, down, draining = filter_agents(self.get_all_agents(),self.full_maintenance_status)
        self.up(down)
        self.flush(draining)

    def up(self,m_ids=[]):
        if len(m_ids) == 0:
            m_ids = self.machine_ids
        not_scheduled, down, draining = filter_agents(m_ids, self.full_maintenance_status)
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
        not_scheduled, down, draining = filter_agents(m_ids, self.full_maintenance_status)

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
    # WIP
    # elif expired:
    #     m.flush_expired()
    else:
        if len(hosts) == 0:
            emitter.publish("You must defined at least one host")
            return 0
        m.flush()

def schedule_maintenance(start, duration, hosts):
    m = Maintenance(hosts=hosts)
    m.schedule_maintenance(start, duration)
