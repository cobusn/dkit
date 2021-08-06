from dkit.data.containers import DictionaryEmulator

__all__ = [
    "DataTrie"
]


class DataTrie(object):
    """Simple data trie with payload"""
    class TrieNode(DictionaryEmulator):

        def __init__(self, prefix, data=None):
            super().__init__()
            self.prefix = prefix
            self.data = data
            self.children = dict()

    def __init__(self):
        self.children = {}
        self.root = self.TrieNode("", None)

    def insert(self, prefix, data):
        """insert word into trie"""
        current = self.root
        for i, char in enumerate(prefix):
            if char not in current:
                prefix = prefix[0:i+1]
                current[char] = self.TrieNode(prefix, data)
            current = current[char]
        current.closed = False

    def find(self, word):
        """locate first complete prefix"""
        current = self.root
        for char in word:
            if char not in current:
                return current.data
            current = current[char]
        return current.data
