class ItemNotCached(KeyError): pass
class DuplicateKeyOnAdd(KeyError): pass
class NoItemsCached(Exception): pass
class ItemExpired(ItemNotCached): pass
