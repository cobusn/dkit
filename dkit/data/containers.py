import collections
import bisect
import pickle
import shelve
from _pickle import Pickler, Unpickler, dumps, loads
from io import BytesIO

import collections.abc
import collections_extended as ce

"""
Container data structures
"""


__all__ = [
    "AttrDict",
    "DictionaryEmulator",
    "FastFlexShelve",
    "FlexBSDDBShelve",
    "FlexShelve",
    "ListEmulator",
    "OrderedSet",
    "RangeCounter",
    "ReusableStack",
    "SortedCollection",
]


class RangeCounter(object):
    """can be used to count items in ranges"""

    def __init__(self, *the_iterable):
        last = None
        lst = []
        for i in the_iterable:
            lst.append((last, i, 0))
            last = i
        lst.append((last, None, 0))
        print(lst)
        self.store = ce.RangeMap(lst)

    def increment(self, value):
        self.store[value] = self.store[value] + 1

    def update(self, the_iterable):
        for i in the_iterable:
            self.store[i] += 1

    def __getitem__(self, key):
        return self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __str__(self):
        return str(self.store)


class AttrDict(dict):
    """
    Access class attributes as dictionary values and vice-versa

    refer to:

        https://stackoverflow.com/questions/4984647/accessing-dict-keys-like-an-attribute

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


class DictionaryEmulator(collections.abc.MutableMapping):
    """
    Base class for emulating a dict without inheriting from dict
    """
    def __init__(self, *args, **kwargs):
        self.store = {}
        self.update(dict(*args, **kwargs))

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __keytransform__(self, key):
        return key

    def as_dict(self):
        """for use by map_db classes"""
        return self.store


class SizedMap(collections.OrderedDict):
    """maintain a dictionary of size n

    Older entries are discarded on a fifo
    basis

    https://stackoverflow.com/questions/2437617/how-to-limit-the-size-of-a-dictionary

    """
    def __init__(self, n, callback=None, *args, **kwargs):
        self.n = n
        self.callback = callback if callback else self.on_discard
        super().__init__(*args, **kwargs)

    def _check_size_limit(self):
        """maintain size n"""
        while len(self) > self.n:
            self.callback(
                *self.popitem(last=False)
            )

    def on_discard(self, key, last):
        """callback called each time a value is discarded

        parameter:
            - key: discarded key
            - value: discarded value
        """
        pass

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._check_size_limit()


class ListEmulator(collections.abc.MutableSequence):
    """
    simple list emulator implementation
    """

    def __init__(self, *items):
        self.inner_list = list()
        self.inner_list.extend(items)

    def __len__(self):
        return len(self.inner_list)

    def __getitem__(self, index):
        return self.inner_list[index]

    def __delitem__(self, index):
        del self.inner_list[index]

    def insert(self, index, value):
        self.inner_list.insert(index, value)

    def __setitem__(self, index, value):
        self.inner_list[index] = value


class ReusableStack(object):
    """
    simulted LIFO stack with reset()

    Stack simulated with a list that is designed to preserve the
    data and can be reset. Useful for re-using the same data.

    Data must be supplied with the constructor and cannot be
    modified once the class is instantiated.
    """
    def __init__(self, iterable=[]):
        self._data = list(iterable)
        self.index = 0
        self.reset()

    def pop(self, items=1):
        """
        pop an item from end of stack
        """
        if self.index <= 0:
            raise IndexError("Stack is empty")
        self.index -= 1
        return self._data[self.index]

    def __len__(self):
        """
        return length of stack
        """
        return len(self._data)

    def reset(self):
        """
        reset position back to the end of the stack
        """
        self.index = len(self._data)


class SortedCollection(object):
    '''
    Sequence sorted by a key function.

    It supports key functions like those use in sorted(), min(), and max().
    The result of the key function call is saved so that keys can be searched
    efficiently.

    Instead of returning an insertion-point which can be hard to interpret, the
    five find-methods return a specific item in the sequence. They can scan for
    exact matches, the last item less-than-or-equal to a key, or the first item
    greater-than-or-equal to a key.

    Once found, an item's ordinal position can be located with the index() method.
    New items can be added with the insert() and insert_right() methods.
    Old items can be deleted with the remove() method.

    The usual sequence methods are provided to support indexing, slicing,
    length lookup, clearing, copying, forward and reverse iteration, contains
    checking, item counts, item removal, and a nice looking repr.

    Finding and indexing are O(log n) operations while iteration and insertion
    are O(n).  The initial sort is O(n log n).

    The key function is stored in the 'key' attibute for easy introspection or
    so that you can assign a new key function (triggering an automatic re-sort).

    In short, the class was designed to handle all of the common use cases for
    bisect but with a simpler API and support for key functions.

    >>> from pprint import pprint
    >>> from operator import itemgetter

    >>> s = SortedCollection(key=itemgetter(2))
    >>> for record in [
    ...         ('roger', 'young', 30),
    ...         ('angela', 'jones', 28),
    ...         ('bill', 'smith', 22),
    ...         ('david', 'thomas', 32)]:
    ...     s.insert(record)

    >>> pprint(list(s))         # show records sorted by age
    [('bill', 'smith', 22),
     ('angela', 'jones', 28),
     ('roger', 'young', 30),
     ('david', 'thomas', 32)]

    >>> s.find_le(29)           # find oldest person aged 29 or younger
    ('angela', 'jones', 28)
    >>> s.find_lt(28)           # find oldest person under 28
    ('bill', 'smith', 22)
    >>> s.find_gt(28)           # find youngest person over 28
    ('roger', 'young', 30)

    >>> r = s.find_ge(32)       # find youngest person aged 32 or older
    >>> s.index(r)              # get the index of their record
    3
    >>> s[3]                    # fetch the record at that index
    ('david', 'thomas', 32)

    >>> s.key = itemgetter(0)   # now sort by first name
    >>> pprint(list(s))
    [('angela', 'jones', 28),
     ('bill', 'smith', 22),
     ('david', 'thomas', 32),
     ('roger', 'young', 30)]

    '''
    def __init__(self, iterable=(), key=None):
        self._given_key = key
        key = (lambda x: x) if key is None else key
        decorated = sorted((key(item), item) for item in iterable)
        self._keys = [k for k, item in decorated]
        self._items = [item for k, item in decorated]
        self._key = key

    def _getkey(self):
        return self._key

    def _setkey(self, key):
        if key is not self._key:
            self.__init__(self._items, key=key)

    def _delkey(self):
        self._setkey(None)

    key = property(_getkey, _setkey, _delkey, 'key function')

    def reset(self):
        """ reset the container """
        self.__init__([], self._key)

    def copy(self):
        """ return a copy of the object """
        return self.__class__(self, self._key)

    def __len__(self):
        return len(self._items)

    @property
    def min_value(self):
        return self._items[0]

    @property
    def max_value(self):
        return self._items[-1]

    def __getitem__(self, i):
        return self._items[i]

    def __iter__(self):
        return iter(self._items)

    def __reversed__(self):
        return reversed(self._items)

    def __repr__(self):
        return '%s(%r, key=%s)' % (
            self.__class__.__name__,
            self._items,
            getattr(self._given_key, '__name__', repr(self._given_key))
        )

    def __reduce__(self):
        return self.__class__, (self._items, self._given_key)

    def __contains__(self, item):
        k = self._key(item)
        i = bisect.bisect_left(self._keys, k)
        j = bisect.bisect_right(self._keys, k)
        return item in self._items[i:j]

    def index(self, item):
        """Find the position of an item.  Raise ValueError if not found."""
        k = self._key(item)
        i = bisect.bisect_left(self._keys, k)
        j = bisect.bisect_right(self._keys, k)
        return self._items[i:j].index(item) + i

    def count(self, item):
        """Return number of occurrences of item"""
        k = self._key(item)
        i = bisect.bisect_left(self._keys, k)
        j = bisect.bisect_right(self._keys, k)
        return self._items[i:j].count(item)

    def insert(self, item):
        """Insert a new item.  If equal keys are found, add to the left"""
        k = self._key(item)
        i = bisect.bisect_left(self._keys, k)
        self._keys.insert(i, k)
        self._items.insert(i, item)

    def insert_right(self, item):
        """Insert a new item.  If equal keys are found, add to the right"""
        k = self._key(item)
        i = bisect.bisect_right(self._keys, k)
        self._keys.insert(i, k)
        self._items.insert(i, item)

    def remove(self, item):
        """Remove first occurence of item.  Raise ValueError if not found"""
        i = self.index(item)
        del self._keys[i]
        del self._items[i]

    def find(self, k):
        """Return first item with a key == k.  Raise ValueError if not found."""
        i = bisect.bisect_left(self._keys, k)
        if i != len(self) and self._keys[i] == k:
            return self._items[i]
        raise ValueError('No item found with key equal to: %r' % (k,))

    def index_le(self, k):
        "return index of last item with a key <= k. Raise ValueError if not found"
        return bisect.bisect_right(self._keys, k)

    def index_lt(self, k):
        'Return index last item with a key < k.  Raise ValueError if not found.'
        i = bisect.bisect_left(self._keys, k)
        if i:
            return i-1
        raise ValueError('No item found with key below: %r' % (k,))

    def find_le(self, k):
        'Return last item with a key <= k.  Raise ValueError if not found.'
        i = bisect.bisect_right(self._keys, k)
        if i:
            return self._items[i-1]
        raise ValueError('No item found with key at or below: %r' % (k,))

    def find_lt(self, k):
        'Return last item with a key < k.  Raise ValueError if not found.'
        i = bisect.bisect_left(self._keys, k)
        if i:
            return self._items[i-1]
        raise ValueError('No item found with key below: %r' % (k,))

    def find_ge(self, k):
        'Return first item with a key >= equal to k.  Raise ValueError if not found'
        i = bisect.bisect_left(self._keys, k)
        if i != len(self):
            return self._items[i]
        raise ValueError('No item found with key at or above: %r' % (k,))

    def find_gt(self, k):
        'Return first item with a key > k.  Raise ValueError if not found'
        i = bisect.bisect_right(self._keys, k)
        if i != len(self):
            return self._items[i]
        raise ValueError('No item found with key above: %r' % (k,))


class _ClosedDict(collections.abc.MutableMapping):
    'Marker for a closed dict.  Access attempts raise a ValueError.'

    def closed(self, *args):
        raise ValueError('invalid operation on closed shelf')
    __iter__ = __len__ = __getitem__ = __setitem__ = __delitem__ = keys = closed

    def __repr__(self):
        return '<Closed Dictionary>'


class _Shelve(collections.abc.MutableMapping):
    """Base class for shelf implementations.

    This is initialized with a dictionary-like object.
    See the module's __doc__ string for an overview of the interface.
    """

    def __init__(self, dict, protocol=None, writeback=False):
        self.dict = dict
        if protocol is None:
            protocol = 3
        self._protocol = protocol
        self.writeback = writeback
        self.cache = {}

    def __iter__(self):
        return (loads(k) for k in self.dict.keys())

    def __len__(self):
        return len(self.dict)

    def __contains__(self, key):
        return dumps(key) in self.dict

    def get(self, key, default=None):
        if dumps(key) in self.dict:
            return self[key]
        return default

    def __getitem__(self, key):
        try:
            value = self.cache[key]
        except KeyError:
            f = BytesIO(self.dict[dumps(key)])
            value = Unpickler(f).load()
            if self.writeback:
                self.cache[key] = value
        return value

    def __setitem__(self, key, value):
        if self.writeback:
            self.cache[key] = value
        f = BytesIO()
        p = Pickler(f, self._protocol)
        p.dump(value)
        self.dict[dumps(key)] = f.getvalue()

    def __delitem__(self, key):
        del self.dict[dumps(key)]
        try:
            del self.cache[key]
        except KeyError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if self.dict is None:
            return
        try:
            self.sync()
            try:
                self.dict.close()
            except AttributeError:
                pass
        finally:
            # Catch errors that may happen when close is called from __del__
            # because CPython is in interpreter shutdown.
            try:
                self.dict = _ClosedDict()
            except Exception:
                self.dict = None

    def __del__(self):
        if not hasattr(self, 'writeback'):
            # __init__ didn't succeed, so don't bother closing
            # see http://bugs.python.org/issue1339007 for details
            return
        self.close()

    def sync(self):
        if self.writeback and self.cache:
            self.writeback = False
            for key, entry in self.cache.items():
                self[key] = entry
            self.writeback = True
            self.cache = {}
        if hasattr(self.dict, 'sync'):
            self.dict.sync()


class FlexBSDDBShelve(_Shelve):
    """Shelf implementation using the "BSD" db interface.

    This adds methods first(), next(), previous(), last() and
    set_location() that have no counterpart in [g]dbm databases.

    The actual database must be opened using one of the "bsddb"
    modules "open" routines (i.e. bsddb.hashopen, bsddb.btopen or
    bsddb.rnopen) and passed to the constructor.

    See the module's __doc__ string for an overview of the interface.
    """

    def __init__(self, dict, protocol=None, writeback=False):
        _Shelve.__init__(self, dict, protocol, writeback)

    def set_location(self, key):
        (key, value) = self.dict.set_location(key)
        f = BytesIO(value)
        return (loads(key), Unpickler(f).load())

    def next(self):
        (key, value) = next(self.dict)
        f = BytesIO(value)
        return (loads(key), Unpickler(f).load())

    def previous(self):
        (key, value) = self.dict.previous()
        f = BytesIO(value)
        return (loads(key), Unpickler(f).load())

    def first(self):
        (key, value) = self.dict.first()
        f = BytesIO(value)
        return (loads(key), Unpickler(f).load())

    def last(self):
        (key, value) = self.dict.last()
        f = BytesIO(value)
        return (loads(key), Unpickler(f).load())


class FlexShelve(_Shelve):
    """Shelf implementation using the "dbm" generic dbm interface.

    This is initialized with the filename for the dbm database.
    See the module's __doc__ string for an overview of the interface.
    """

    def __init__(self, filename, flag='c', protocol=None, writeback=False):
        import dbm
        super().__init__(dbm.open(filename, flag), protocol, writeback)


class FastFlexShelve(collections.abc.MutableMapping):
    """
    lightweight shelve version that work with any opbject that can pickle as key

    adapted from
    https://stackoverflow.com/questions/31565921/object-storage-in-python-that-allow-tuples-as-keys
    """
    def __init__(self, file_name, flag="c", protocol=None, writeback=False):
        self.udict = shelve.open(file_name, flag, protocol, writeback)

    def __getitem__(self, key):
        return self.udict[dumps(key)]

    def __setitem__(self, key, value):
        self.udict[pickle.dumps(key).decode()] = value

    def __delitem__(self, key):
        del self.udict[pickle.dumps(key).decode()]

    def keys(self):
        return (pickle.loads(key) for key in self.udict.keys())

    def __iter__(self):
        return (pickle.loads(k) for k in self.udict.kesy())

    def __len__(self):
        return len(self.udict)

    def __contains__(self, key):
        return pickle.dumps(key) in self.udict

    def sync(self):
        self.udict.sync()

    def close(self):
        self.udict.close()


class OrderedSet(collections.abc.MutableSet):
    """
    OrderedSet: Behave like a set but items are ordered
    by insertion

    Adapted from an ActiveState Recipy by
    Raymond Hettinger: http://code.activestate.com/recipes/576694/
    """

    def __init__(self, iterable=None):
        self.end = end = []
        end += [None, end, end]         # sentinel node for doubly linked list
        self.map = {}                   # key --> [key, prev, next]
        if iterable is not None:
            self |= iterable

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[1]
            curr[2] = end[1] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:
            key, prev, next = self.map.pop(key)
            prev[2] = next
            next[1] = prev

    def __iter__(self):
        end = self.end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def pop(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = self.end[1][0] if last else self.end[2][0]
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)
