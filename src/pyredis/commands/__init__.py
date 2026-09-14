"""The command table.

Each module below owns one family of commands and exposes a ``commands``
:class:`~pyredis.commands.base.CommandSet`. Composing them here - rather than
having each module register itself into a shared global - means the table is
built once, explicitly, and importing a command module on its own has no side
effects.
"""

from itertools import chain

from pyredis.commands import (
    connection_cmds,
    generic,
    lists,
    pubsub_cmds,
    replication_cmds,
    server_cmds,
    streams,
    strings,
    transactions,
    zsets,
)
from pyredis.commands.base import CommandSet, CommandSpec, CommandTable

#: Every command this server implements, keyed by name.
COMMAND_TABLE = CommandTable(
    chain(
        connection_cmds.commands,
        server_cmds.commands,
        generic.commands,
        strings.commands,
        lists.commands,
        streams.commands,
        zsets.commands,
        pubsub_cmds.commands,
        transactions.commands,
        replication_cmds.commands,
    )
)

__all__ = ["COMMAND_TABLE", "CommandSet", "CommandSpec", "CommandTable"]
