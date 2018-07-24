EXTRACTS = "_extracts"
BINDS = "_binds"


def extracts(*extract):
    def decorate(func):
        setattr(func, EXTRACTS, extract)
        return func
    return decorate


def binds(**binds):
    def decorate(func):
        setattr(func, BINDS, binds)
        return func
    return decorate
