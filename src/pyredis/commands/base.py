"""Command registration and lookup.

Each command module builds its own :class:`CommandSet` with the
:meth:`CommandSet.add` decorator, and ``pyredis.commands.__init__`` composes
them into one immutable :class:`CommandTable`. Nothing registers itself into a
process-wide mutable registry, so importing a command module has no side
effects beyond defining functions.

A handler is an ordinary function taking ``(connection, args)`` and returning a
:data:`~pyredis.protocol.Reply`. ``args`` includes the command name at index 0,
matching Redis's own convention so that arity numbers can be copied straight
from the Redis command table. Handlers that must wait for something -
``BLPOP``, ``XREAD BLOCK``, ``WAIT`` - are declared ``async``; the dispatcher
awaits those and calls the rest directly, so the common path allocates no
coroutine.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, TypeVar

from pyredis.protocol import Reply

if TYPE_CHECKING:  # pragma: no cover - import cycle broken for type checking only
    from pyredis.connection import Connection

__all__ = ["CommandSet", "CommandSpec", "CommandTable", "Handler"]

Handler = Callable[["Connection", list[bytes]], "Reply | Awaitable[Reply]"]

#: Bound to Handler so the decorator returns the *same* callable type it was
#: given, which keeps handlers directly callable from one another.
HandlerT = TypeVar("HandlerT", bound=Handler)

#: ``arity`` follows the Redis convention: a positive number is the exact
#: argument count including the command name, a negative number is the minimum.
ARITY_VARIADIC: Final = -1


@dataclass(frozen=True, slots=True)
class CommandSpec:
    """Everything the dispatcher needs to know about one command."""

    name: str
    handler: Handler
    arity: int
    #: Mutates the keyspace, so it is propagated to replicas and rejected on a
    #: read-only replica when it arrives from a normal client.
    is_write: bool = False
    #: Permitted while the connection is in subscribe mode (RESP2 restricts
    #: that mode to a handful of commands).
    allowed_in_subscribe_mode: bool = False
    #: Queued rather than executed inside ``MULTI``. False for the transaction
    #: control commands themselves, which must run immediately.
    queued_in_multi: bool = True
    #: May appear inside a transaction at all. False for commands that write
    #: their own output (``SUBSCRIBE``, ``PSYNC``): their bytes would land
    #: inside the EXEC reply array and corrupt it.
    allowed_in_multi: bool = True
    #: True when the handler is a coroutine function and must be awaited.
    is_async: bool = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "is_async", inspect.iscoroutinefunction(self.handler))

    def accepts_arity(self, argc: int) -> bool:
        """Whether ``argc`` arguments (command name included) is acceptable."""
        if self.arity >= 0:
            return argc == self.arity
        return argc >= -self.arity


class CommandSet:
    """A module-local collection of command specs, built with a decorator."""

    __slots__ = ("_specs",)

    def __init__(self) -> None:
        self._specs: list[CommandSpec] = []

    def add(
        self,
        name: str,
        *,
        arity: int,
        is_write: bool = False,
        allowed_in_subscribe_mode: bool = False,
        queued_in_multi: bool = True,
        allowed_in_multi: bool = True,
    ) -> Callable[[HandlerT], HandlerT]:
        """Register the decorated function as the handler for ``name``.

        The function is returned unchanged so it stays directly callable, which
        keeps handlers easy to exercise in isolation.
        """

        def decorator(handler: HandlerT) -> HandlerT:
            self._specs.append(
                CommandSpec(
                    name=name.lower(),
                    handler=handler,
                    arity=arity,
                    is_write=is_write,
                    allowed_in_subscribe_mode=allowed_in_subscribe_mode,
                    queued_in_multi=queued_in_multi,
                    allowed_in_multi=allowed_in_multi,
                )
            )
            return handler

        return decorator

    def __iter__(self) -> Iterator[CommandSpec]:
        return iter(self._specs)


class CommandTable:
    """Immutable name -> spec lookup, built once at import time."""

    __slots__ = ("_by_name",)

    def __init__(self, specs: Iterable[CommandSpec]) -> None:
        by_name: dict[bytes, CommandSpec] = {}
        for spec in specs:
            key = spec.name.encode()
            if key in by_name:
                raise AssertionError(f"duplicate command registration: {spec.name}")
            by_name[key] = spec
        self._by_name = by_name

    def lookup(self, name: bytes) -> CommandSpec | None:
        """Find a command by name. Command names are case-insensitive."""
        return self._by_name.get(name.lower())

    def names(self) -> list[str]:
        return sorted(spec.name for spec in self._by_name.values())

    def __len__(self) -> int:
        return len(self._by_name)

    def __iter__(self) -> Iterator[CommandSpec]:
        return iter(self._by_name.values())
