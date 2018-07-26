from collections import MutableMapping


class SafeContext(MutableMapping):
    """
    Dictionary that raises on overwriting keys.

    """

    def __init__(self, *args, **kwargs):
        self.store = dict(*args, **kwargs)

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        if key in self.store:
            raise ValueError(f"Key '{key}' already set")
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __getattr__(self, key):
        return self[key]
