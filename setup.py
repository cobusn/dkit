# from distutils.core import setup
from setuptools import setup
from Cython.Build import cythonize

setup(
    cffi_modules=["build_tdigest.py:tdigest_ffi"],
    ext_modules=cythonize(
        [
            "dkit/data/stats.py",
            "dkit/utilities/instrumentation.py",
        ],
        compiler_directives={'language_level': "3"},
    ),
)
