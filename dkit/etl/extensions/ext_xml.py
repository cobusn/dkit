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
from .. import source
from ...data import xml_helper
"""
dkit.etl extensions for xml documents
"""


class XmlSource(source.AbstractMultiReaderSource):
    """
    Source class for documents transformed from XML

    Sample usage:

     .. include:: ../../examples/example_xml_source.py
        :literal:

    Produces:

      .. include:: ../../examples/example_xml_source.out
        :literal:

    :param reader_list: list of reader instances
    :param boundary: name of boundary element
    :param fields_dict: dictionary of field definitions
    """
    def __init__(self, reader_list, boundary, fields_dict):
        super().__init__(reader_list)
        self.transformer = xml_helper.XmlTransformer(boundary, fields_dict)

    def __iter__(self):
        """
        iterate through values
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for row in self.transformer.iter_file(o_reader):
                    stats.increment()
                    yield row
            else:
                with o_reader.open() as in_file:
                    for row in self.transformer.iter_file(in_file):
                        stats.increment()
                        yield row
        stats.stop()

    @classmethod
    def from_recipe(cls, reader_list, recipe):
        """
        dictionary based constructor

        recipe must be in the following format:

        recipe is a dictionary instance in the following
        format::

         {
            "boundary": "row",
            "fields": {
                "id": 'int(xpath("/@id"))'
                "name": 'xpath("/name/text()")'
                ...
            }
         }


        """
        return cls(
            reader_list,
            recipe["boundary"],
            recipe["fields_dict"],
        )
