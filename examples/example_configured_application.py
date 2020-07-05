"""
Example usage of Configured Applicatons.
"""
import sys
sys.path.insert(0, "..") # noqa

from dkit import base


# Application with Configuration functionality
class ArgApplication(base.ConfiguredApplication, base.InitArgumentsMixin, base.InitConfigMixin):

    def __init__(self, **kwargs):
        super(ArgApplication, self).__init__(**kwargs)


if __name__ == "__main__":
    # Print MRO
    for m in ArgApplication.__mro__:
        print(m)

    mixd = ArgApplication()

    print(mixd.arguments)
    print(mixd.config)
