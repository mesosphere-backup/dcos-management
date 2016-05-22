"""DCOS Management stuff

Usage:
    dcos management --info
    dcos management maintenance list [--json]
    dcos management maintenance up <hostname>...
    dcos management maintenance down <hostname>...
    dcos management maintenance schedule add [--start=<date>] [--duration=<duration>] [<hostname>...]
    dcos management maintenance schedule flush  [<hostname>...]

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
from dcos_management import constants, tables, maintenance
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
            hierarchy=['management','maintenance', 'list'],
            arg_keys=['--json'],
            function=maintenance.list),

        cmds.Command(
            hierarchy=['management','maintenance', 'up'],
            arg_keys=['<hostname>'],
            function=maintenance.up),

        cmds.Command(
            hierarchy=['management','maintenance', 'down'],
            arg_keys=['<hostname>'],
            function=maintenance.down),

        cmds.Command(
            hierarchy=['management','maintenance', 'schedule', 'flush'],
            arg_keys=['<hostname>'],
            function=maintenance.flush_schedule),

        cmds.Command(
            hierarchy=['management','maintenance', 'schedule', 'add'],
            arg_keys=['--start', '--duration', '<hostname>'],
            function=maintenance.schedule_maintenance),

    ]

def _info():
    emitter.publish(__doc__.split('\n')[0])
    return 0
