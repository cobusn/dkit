from setuptools import setup
from Cython.Build import cythonize
import dkit

"""
Setup
"""

setup(
    name='dkit',
    version=dkit.__version__,
    description='General purpose library',
    ext_modules=cythonize(
        [
            "dkit/data/stats.py",
            "dkit/utilities/instrumentation.py",
        ],
        compiler_directives={'language_level': "3"},
    ),
    author='Cobus Nel',
    author_email='cobus at nel.org.za',
    setup_requires=["cffi>=1.4.0"],
    install_requires=[
        "cffi>=1.4.0",
        "numpy",
        "scipy",
    ],
    cffi_modules=["build_tdigest.py:tdigest_ffi"],
    packages=[
        "dkit",
        "dkit.algorithms",
        "dkit.data",
        "dkit.doc",
        "dkit.etl",
        "dkit.etl.extensions",
        "dkit.parsers",
        "dkit.plot",
        "dkit.shell",
        "dkit.utilities",
    ],
)
