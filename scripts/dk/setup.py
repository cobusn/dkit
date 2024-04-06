from setuptools import setup
from dk import VERSION

setup(
    name='dk',
    version=VERSION,
    description="Data processing tool Kit",
    long_description="""utility for data processing""",
    classifiers=[],
    keywords=['etl', 'statistics', 'data science', 'database', 'csv', 'json'],
    author='Cobus Nel',
    author_email='cobus@nel.org.za',
    url='',
    license='Open',
    packages=['lib_dk', ],
    include_package_data=True,
    zip_safe=False,
    py_modules=['dk', ],
    install_requires=[
        "pyperclip",
    ],
    entry_points={
        'console_scripts': ['dk=dk:main'],
    },
)
