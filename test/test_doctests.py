import doctest
import unittest
import pkgutil
import sys; sys.path.insert(0, "..")  # noqa
import dkit

EXCLUDE = set(
    [
        "dkit.doc.builder",
        "dkit.doc.canned",
        "dkit.doc.json_renderer",
        "dkit.doc.latex_renderer",
        "dkit.doc.reportlab_renderer",
        "dkit.etl.extensions.ext_sql_alchemy", # unless cxOracle installed
    ]
)


def load_checks(suite, mod):
    for importer, name, ispkg in pkgutil.walk_packages(mod.__path__, mod.__name__ + '.'):
        print(name)
        if name not in EXCLUDE:
            suite.addTests(doctest.DocTestSuite(name))


suite = unittest.TestSuite()
load_checks(suite, dkit)

#  runner = unittest.TextTestRunner(verbosity=2)
runner = unittest.TextTestRunner()
runner.run(suite)
