"""
Enumeration of common lifecycle changes.

"""


class LifecycleChange(set):
    """
    A collection of CRUD lifecycle transitions that may be announced via pubsub.

    Note that `Changed` is strongly discouraged; use immutable resources instead.

    """
    Changed = "changed"
    Created = "created"
    Deleted = "deleted"

    def __init__(self, graph):
        super().__init__([
            LifecycleChange.Changed,
            LifecycleChange.Created,
            LifecycleChange.Deleted,
        ])

    def matches(self, media_type):
        parts = media_type.split(".")
        return any(
            item in parts
            for item in self
        )
