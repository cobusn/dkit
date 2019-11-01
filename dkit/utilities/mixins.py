class SerDeMixin(object):
    """
    useful serialize/deserialize methods
    """
    def as_dict(self):

        retval = {
            "~>": self.__class__.__name__.lower()
        }
        for k, v in ((k, v) for k, v in self.__dict__.items() if not k.startswith("_")):

            # list like objects
            if isinstance(v, (list, set, tuple)):
                retval[k] = [i.as_dict() if hasattr(i, "as_dict") else i for i in v]

            # dict like objects
            elif isinstance(v, (dict)):
                retval[k] = {j: i.as_dict() if hasattr(i, "as_dict") else i for j, i in v.items()}

            # normal attributes
            else:
                retval[k] = v.as_dict() if hasattr(v, "as_dict") else v

        return retval

    @classmethod
    def from_dict(cls, the_dict):
        return cls(**the_dict)
