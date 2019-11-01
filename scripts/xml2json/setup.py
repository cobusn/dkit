#
# Copyright (C) 2016  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#

from setuptools import setup

version = "0.1"

setup(
    name='xml2json',
    version=version,
    description="Convert XML to jsonl",
    long_description="""Convert XML to jsonl""",
    classifiers=[],
    author='Cobus Nel',
    author_email='',
    url='',
    license='Open',
    include_package_data=True,
    zip_safe=False,
    py_modules=["xml2json"],
    install_requires=[
        '# -*- Extra requirements: -*-'
        ],
    entry_points={
     'console_scripts': ['xml2json=xml2json:main'],
     },
    )
