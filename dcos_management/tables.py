import copy
import datetime
import posixpath
from collections import OrderedDict

import prettytable
from dcos import mesos, util


def maintenance_table(maintenance):
    """Returns a PrettyTable representation of the provided mesos tasks.
    :rtype: PrettyTable
    """

    fields = OrderedDict([
        ("HOST", lambda t: t["hostname"]),
        ("STATE", lambda t: t["state"]),

    ])

    tb = table(fields, maintenance, sortby="STATE")
    tb.align["HOST"] = "l"
    tb.align["STATE"] = "l"

    return tb

# XXX: fix.
# Copy'n'Paste from dcos-cli/cli/dcoscli/tables.py
def table(fields, objs, **kwargs):
    """Returns a PrettyTable.  `fields` represents the header schema of
    the table.  `objs` represents the objects to be rendered into
    rows.
    :param fields: An OrderedDict, where each element represents a
                   column.  The key is the column header, and the
                   value is the function that transforms an element of
                   `objs` into a value for that column.
    :type fields: OrderdDict(str, function)
    :param objs: objects to render into rows
    :type objs: [object]
    :param **kwargs: kwargs to pass to `prettytable.PrettyTable`
    :type **kwargs: dict
    :rtype: PrettyTable
    """

    tb = prettytable.PrettyTable(
        [k.upper() for k in fields.keys()],
        border=False,
        hrules=prettytable.NONE,
        vrules=prettytable.NONE,
        left_padding_width=0,
        right_padding_width=1,
        **kwargs
    )

    # Set these explicitly due to a bug in prettytable where
    # '0' values are not honored.
    tb._left_padding_width = 0
    tb._right_padding_width = 2

    for obj in objs:
        row = [fn(obj) for fn in fields.values()]
        tb.add_row(row)

    return tb
