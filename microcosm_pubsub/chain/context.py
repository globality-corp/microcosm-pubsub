from collections import MutableMapping
from itertools import chain


CONTEXT = "context"


class SafeContext(MutableMapping):
    """
    Dictionary that raises on overwriting keys.

    """
    def __init__(self, *args, **kwargs):
        self.store = dict(*args, **kwargs)

    def __getitem__(self, key):
        # enable context self-references
        if key == CONTEXT:
            return self
        return self.store[key]

    def __setitem__(self, key, value):
        # do not allow overwrite of context self-references
        if key == CONTEXT:
            raise ValueError("May not reassign 'context' key")

        if key in self.store:
            raise ValueError(f"Key '{key}' already set")

        self.store[key] = value

    def assign(self, key, value):
        # do not allow overwrite of context self-references
        if key == CONTEXT:
            raise ValueError("May not reassign 'context' key")

        # Skip validation
        self.store[key] = value
        return self

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __getattr__(self, key):
        return self[key]

    def local(self, *args, **kwargs):
        """
        Create a locally scoped child context.

        """
        return ScopedSafeContext(self, *args, **kwargs)


class ScopedSafeContext(SafeContext):
    """
    SafeContext that delegates reads to a parent context.

    """
    def __init__(self, parent, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parent = parent

    def __getitem__(self, key):
        # enable context self-references
        if key == CONTEXT:
            return self

        try:
            return self.store[key]
        except KeyError:
            return self.parent[key]

    def __setitem__(self, key, value):
        if key in self.parent:
            raise ValueError(f"Key '{key}' already set")
        super().__setitem__(key, value)

    def __iter__(self):
        return chain(iter(self.store), iter(self.parent))

    def __len__(self):
        return len(self.store) + len(self.parent)
