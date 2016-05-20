"""DCOS Management stuff

Usage:
    dcos management --info
    dcos management maintenance (--list | --down | --start=<date> | --up ) [--duration=<duration>] [<hostname>...]
    dcos management reservation (--add | --remove) --resource-string=<resources> --principal=<principal>

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
            hierarchy=['management','maintenance'],
            arg_keys=['--list','--down', '--start', '--up', '--duration', '<hostname>'],
            function=maintenance._maintenance),
    ]

def _info():
    emitter.publish(__doc__.split('\n')[0])
    return 0
