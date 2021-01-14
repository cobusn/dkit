
class SchemaMap(object):

    def __init__(self, depth):
        self.depth = depth

    def infer_types(self, data):
        """infer types of dataset"""
        typemap = {}
        return typemap

    def __call__(self, data):
        self.data = data
