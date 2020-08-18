

class LinkedEntry:

    def __init__(self, key, item, prior=None, next=None):
        self.key = key
        self.item = item
        self.prior = prior
        self.next = next


    def __repr__(self):
        if self.prior is None:
            prior = 'None'
        else:
            prior = "'%s'" % (self.prior.key)

        if self.next is None:
            next = 'None'
        else:
            next = "'%s'" % (self.next.key)

        s = "LinkedEntry('%s', prior=%s, next=%s)" % (self.key, prior, next)


    def __str__(self):
        return self.key


class LRULinkedList:
    '''Linked list to keep track of LRU.  Newest at front of list'''

    def __init__(self):
        self.__head = None
        self.__tail = None
        self.__keys = dict()


    def add_new(self, key, item):
        '''
        Add item to the front

        :param key: Key identifying cached item
        :param item: CachedItem
        '''
        if key in self.__keys:
            raise KeyError("Key already in list")

        entry = LinkedEntry(key, item, next=self.__head)
        self.__head = entry
        if self.__tail is None:
            self.__tail = entry

        if entry.next is not None:
            entry.next.prior = entry

        self.__keys[key] = entry


    def oldest_key(self):
        '''Return the key of the oldest item'''
        if self.__tail is not None:
            return self.__tail.key


    def pop_oldest(self):
        '''
        Return oldest item off list

        :return key, CachedItem
        '''
        if self.__tail is not None:
            entry = self.__tail
            self.__tail = entry.prior
            del self.__keys[entry.key]
            return entry.key, entry.item


    def remove(self, key):
        '''
        Remove an item from the list

        :param key: Key of item to remove
        '''
        if key in self.__keys:
            entry = self.__keys[key]
            self.__remove_entry_from_linked_list(entry)
            del self.__keys[key]


    def __remove_entry_from_linked_list(self, entry):
        if entry.prior is not None:
            entry.prior.next = entry.next
        if entry.next is not None:
            entry.next.prior = entry.prior
        if self.__head == entry:
            self.__head = entry.next
        if self.__tail == entry:
            self.__tail = entry.prior
        entry.next = None
        entry.prior = None


    def mark_used(self, key):
        '''Mark an item as just used'''
        entry = self.__keys[key]

        # Remove item from LL
        self.__remove_entry_from_linked_list(entry)

        # Put item on front
        entry.next = self.__head
        self.__head = entry


    def _entries(self):
        entry = self.__head
        while entry is not None:
            # Save next incade this gets removed
            next = entry.next
            yield entry
            entry = next


    def keys(self):
        for entry in self._entries():
            yield entry.key


    def items(self):
        for entry in self._entries():
            yield entry.key, entry.item


    def __getitem__(self, key):
        return self.__keys[key].item


    def __len__(self):
        return len(self.__keys)


    def __contains__(self, key):
        return key in self.__keys
