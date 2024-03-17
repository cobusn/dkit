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
        "boltons",
        "cffi>=1.4.0",
        "mistune",
#        "numpy",
#        "scipy",
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
        # "dkit.resources",
        "dkit.shell",
        "dkit.utilities",
    ],
    include_package_data=True,
    package_data={
        'dkit/resources': [
            "dkit/resources/SourceSansPro-Bold.ttf",
            "dkit/resources/SourceSansPro-Regular.ttf",
            "dkit/resources/SourceSansPro-Italic.ttf",
            "dkit/resources/SourceSansPro-Bold.ttf",
            "dkit/resources/SourceSansPro-BoldItalic.ttf",
            "dkit/resources/lorem.txt",
            # "dkit/resources/background.pdf",
            "dkit/resources/ddfrontpage.pdf",
            "dkit/resources/cars.csv",
            "dkit/resources/rl_stylesheet.yaml",
        ],
    },
)
