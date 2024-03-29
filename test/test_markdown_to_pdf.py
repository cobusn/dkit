import unittest
import sys
sys.path.insert(0, "..")  # noqa
from dkit.doc.builder import SimpleDocRenderer


class TestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_build_file(self):
        builder = SimpleDocRenderer(
            "John Doe",
            "Build a PDF",
            "From Markdown",
            "john.doe@acme.com",
            "111-11111"
        )
        builder.build_from_files(
            "output/simple_doc.pdf",
            "input_files/input.md"
        )


if __name__ == '__main__':
    unittest.main()
