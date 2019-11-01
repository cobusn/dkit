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
import unittest
import sys

sys.path.insert(0, "..")
from faker import Faker
from dkit.data import fake_helper

fake_gen = Faker()


class TestFake(unittest.TestCase):

    def test_person(self):
        persons = list(fake_helper.persons(10))
        self.assertEqual(len(persons), 10)

    def test_id(self):
        persons = list(fake_helper.persons(10))
        for i in [fake_helper.za_id_number(i) for i in persons]:
            self.assertEqual(len(i), 13)

    def test_task(self):
        fake_gen.add_provider(fake_helper.TaskProvider)
        for i in range(1000):
            task = fake_gen.task()
            self.assertTrue(len(task) > 0)

    def test_document(self):
        fake_gen.add_provider(fake_helper.DocumentProvider)
        for i in range(1000):
            document = fake_gen.document()
            self.assertTrue(len(document) > 0)

    def test_application(self):
        fake_gen.add_provider(fake_helper.ApplicationProvider)
        for i in range(1000):
            application = fake_gen.application()
            self.assertTrue(len(application) > 0)


if __name__ == '__main__':
    unittest.main()
