import copy
import datetime
import posixpath
from collections import OrderedDict

import prettytable
from dcos import mesos, util
from dcoscli import tables as dcoscli_tables

def maintenance_table(maintenance):
    """Returns a PrettyTable representation of the provided mesos tasks.
    :rtype: PrettyTable
    """

    fields = OrderedDict([
        ("HOST", lambda t: t["hostname"]),
        ("IP", lambda t: t["ip"]),
        ("ID", lambda t: t["id"]),
        ("STATE", lambda t: t["state"]),
        ("START", lambda t: t["start"]),
        ("DURATION", lambda t: t["duration"]),

    ])

    tb = dcoscli_tables.table(fields, maintenance, sortby="STATE")
    tb.align["HOST"] = "l"
    tb.align["IP"] = "l"
    tb.align["ID"] = "l"
    tb.align["STATE"] = "l"
    tb.align["START"] = "l"
    tb.align["DURATION"] = "r"
    return tb
