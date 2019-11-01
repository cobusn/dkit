import doctest
import unittest
import pkgutil
import sys; sys.path.insert(0, "..")  # noqa
import dkit


def load_checks(suite, mod):
    for importer, name, ispkg in pkgutil.walk_packages(mod.__path__, mod.__name__ + '.'):
        print(name)
        suite.addTests(doctest.DocTestSuite(name))


suite = unittest.TestSuite()
load_checks(suite, dkit)

#  runner = unittest.TextTestRunner(verbosity=2)
runner = unittest.TextTestRunner()
runner.run(suite)
