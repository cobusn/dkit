from setuptools import setup
from Cython.Build import cythonize
import dkit
from typing import List

"""
Setup
"""


def load_dependency(filename: str) -> List[str]:
    """load dependency files"""
    with open(filename, "rt") as infile:
        return [i for i in infile if len(i) > 0]


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
    build_requires=[
        "cffi>=1.4.0",
        "cython",
    ],
    test_require=load_dependency("test_requirements.txt"),
    install_requires=load_dependency("requirements.txt"),
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
        "dkit.resources",
        "dkit.shell",
        "dkit.utilities",
        "lib_dk"
    ],
    entry_points={
        "console_scripts": [
            "dk = dk:main",
        ]
    },
    include_package_data=True,
)
