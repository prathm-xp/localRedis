"""Master/replica replication: shared state, and the replica-side link."""

from pyredis.replication.state import LinkStatus, ReplicaHandle, ReplicationState

__all__ = ["LinkStatus", "ReplicaHandle", "ReplicationState"]
