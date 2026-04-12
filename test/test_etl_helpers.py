import sys; sys.path.insert(0, "..")  # noqa
import unittest
import jinja2

from dkit.etl.helpers import is_select_statement, TemplateSQLExtractor


class TestIsSelectStatement(unittest.TestCase):

    def test_select_statement(self):
        """accept a basic select query"""
        self.assertTrue(
            is_select_statement("SELECT * FROM table_name")
        )

    def test_select_statement_with_where(self):
        """accept a select query with a where clause"""
        self.assertTrue(
            is_select_statement("select id, name from customer where active = 1")
        )

    def test_select_statement_with_whitespace_and_semicolon(self):
        """accept a formatted select query with trailing semicolon"""
        self.assertTrue(
            is_select_statement("  SELECT col\nFROM table_name\nWHERE x = 1;  ")
        )

    def test_non_sql_text(self):
        """reject ordinary descriptive text"""
        self.assertFalse(
            is_select_statement("This helper extracts rows from a table.")
        )

    def test_non_select_statement(self):
        """reject non-select SQL"""
        self.assertFalse(
            is_select_statement("INSERT INTO table_name VALUES (1)")
        )


class TestTemplateSQLExtractor(unittest.TestCase):

    def test_make_sql_raises_on_missing_inline_template_variable(self):
        """raise when an inline SQL template is rendered with missing variables"""
        extractor = TemplateSQLExtractor(
            sql_services=None,
            conn="dummy",
            entity="dummy",
            query_sql="SELECT * FROM customer WHERE id = {{ customer_id }}",
        )

        with self.assertRaises(jinja2.exceptions.UndefinedError):
            extractor.make_sql()


if __name__ == "__main__":
    unittest.main()
