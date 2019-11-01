# Copyright (c) 2017 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import unittest
import yaml
import sys
sys.path.insert(0, "..")
from dkit.data.xml_helper import XmlTransformer


class TestXmlTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open("data/metadata.yml") as metadata:
            cls.template = yaml.load(metadata, Loader=yaml.SafeLoader)["transforms"]["cd_template"]
            metadata.seek(0)
            cls.bs_template = yaml.load(metadata, Loader=yaml.SafeLoader)["transforms"]["bs_template"]
        with open("data/cd_catalog.xml") as xmlfile:
            cls.data = xmlfile.read()

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse(self):
        boundary = self.template["boundary"]
        fields = self.template["fields"]

        # run transformer
        t = XmlTransformer(boundary, fields)
        with open("data/cd_catalog.xml", "rb") as f:
            self.assertEqual(
                len(list(t.iter_file(f))),
                26
            )
            # for row in t.iter_file(f):
            #     print(row)

    def _test_ns(self):
        boundary = self.bs_template["boundary"]
        fields = self.bs_template["fields"]
        print(fields)
        # run transformer
        t = XmlTransformer(boundary, fields)
        with open("data/bsoft.xml", "rb") as f:
            for row in t.iter_file(f):
                print(row)


if __name__ == '__main__':
    unittest.main()
