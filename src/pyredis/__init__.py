"""pyredis: a Redis-compatible in-memory key-value server written from scratch.

The public entry point is :func:`pyredis.__main__.main`; everything else is
internal and free to change.
"""

__version__ = "1.0.0"

#: Version reported as ``redis_version`` by ``INFO``.
#:
#: Client libraries gate behaviour on that field, so reporting nothing (or
#: something unparseable) breaks them. This server implements a *subset* of the
#: named version's commands - ``INFO`` also reports ``pyredis_version``, which
#: is the authoritative version of this implementation. The same approach is
#: taken by other Redis-compatible servers. See ``docs/LIMITATIONS.md``.
COMPATIBLE_REDIS_VERSION = "7.0.0"

__all__ = ["COMPATIBLE_REDIS_VERSION", "__version__"]
