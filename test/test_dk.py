import io
import shutil
import sys
import unittest
import os
from unittest.mock import patch
from dkit import exceptions
from lib_dk.dk import DataKit
from lib_dk import (
    admin_module,
    connections_module,
    endpoints_module,
    explore_module,
    queries_module,
    relations_module,
    run_module,
    schema_module,
    transform_module,
    xml_module
)


class TestDataKitDispatch(unittest.TestCase):

    def test_schema_and_stylesheet_shorthands_are_unambiguous(self):
        "schema and stylesheet commands have distinct case-sensitive prefixes"
        self.assertEqual(DataKit(["dk", "s"]).get_method(), "do_schemas")
        self.assertEqual(
            DataKit(["dk", "S"]).get_method(), "do_Stylesheets"
        )


class TestDK(unittest.TestCase):

    def go(self, tests):
        for test in tests:
            self.test_module(test).run()


class TestConfig(unittest.TestCase):
    """
    Test config module
    """
    def test_init_raises(self):
        """check that exception is raised if ini file exist"""
        with self.assertRaises(exceptions.DKitApplicationException):
            admin_module.AdminModule(
                ["init_config", "--config", "dk_testdata/tst.ini"]
            ).run()
        # os.remove("dk_testdata/tst.ini")

    def test_init_config(self):
        """admin init_config"""
        admin_module.AdminModule(
            ["init_config", "--config", "dk_testdata/ini.cfg"]
        ).run()
        if os.path.exists("dk_testdata/ini.cfg"):
            os.remove("dk_testdata/ini.cfg")

    def test_init_model(self):
        """admin init_model"""
        with self.assertRaises(exceptions.DKitApplicationException):
            admin_module.AdminModule(
                ["init_model", "--model", "dk_testdata/model_test.json"]
            ).run()
        os.remove("dk_testdata/model_test.json")
        admin_module.AdminModule(
            ["init_model", "--model", "dk_testdata/model_test.json"]
        ).run()

    def test_convert(self):
        """admin convert"""
        admin_module.AdminModule(
            ["convert", "-m", "model.yml", "--destination", "dk_testdata/model_test.json"]
        ).run()

        # using default model
        admin_module.AdminModule(
            ["convert", "-m", "model.yml", "--destination", "dk_testdata/model_test.json"]
        ).run()


class TestConnections(unittest.TestCase):
    """
    test endpoints module
    """
    def test_0_add(self):
        """connections add"""
        connections_module.ConnectionsModule(
            ["add", "-m", "model.yml", "-c", "test.uri", "dk_testdata/mpg.csv"]
        ).run()

    def test_1_ls(self):
        """connections ls -y model.yml"""
        connections_module.ConnectionsModule(
            ["ls", "-m", "model.yml"]
        ).run()

    def test_2_print(self):
        """print connection"""
        connections_module.ConnectionsModule(
            ["print", "-m", "model.yml", "-c", "test.uri"]
        ).run()

    def test_3_rm(self):
        """connections rm"""
        connections_module.ConnectionsModule(
            ["rm", "-m", "model.yml", "-c", "test.uri"]
        ).run()


class TestExplore(TestDK):
    """
    Test config module
    """
    @classmethod
    def setUpClass(cls):
        cls.test_module = explore_module.ExploreModule

    def test_count(self):
        """admin init_config"""
        tests = [
            ["count", "-d", "manufacturer", "dk_testdata/mpg.csv"],
        ]
        self.go(tests)

    def test_distinct(self):
        """admin init_config"""
        tests = [
            ["distinct", "-d", "manufacturer", "dk_testdata/mpg.csv"],
            ["distinct", "-l", "-d", "manufacturer", "dk_testdata/mpg.csv"],
        ]
        self.go(tests)

    def test_fields(self):
        """x fields"""
        tests = [
            ["fields", "dk_testdata/mpg.csv"],
            ["fields", "-l", "dk_testdata/mpg.csv"],
        ]
        self.go(tests)

    def test_head(self):
        "x head"
        tests = [
            ["head", "dk_testdata/mpg.csv"],
            ["head", "-n", "5", "dk_testdata/mpg.csv"],
        ]
        self.go(tests)

    def test_regex(self):
        """x search"""
        tests = [
            ["search", "-e", "audi", "dk_testdata/mpg.csv"],
            ["search", "-e", "audi", "dk_testdata/mpg.csv", "--table"],
            ["search", "-e", "audi", "dk_testdata/mpg.csv", "-i", "--table"],
            ["match", "-e", "audi", "dk_testdata/mpg.csv", "-i", "--table"],
        ]
        self.go(tests)

    def test_table(self):
        "x table"
        tests = [
            ["table", "dk_testdata/mpg.csv"],
        ]
        self.go(tests)

    def test_dash_reads_jsonl_from_stdin(self):
        "'-' is shorthand for jsonl:///stdio"
        with open("dk_testdata/mpg.jsonl") as f:
            content = f.read()
        old_stdin = sys.stdin
        sys.stdin = io.StringIO(content)
        try:
            explore_module.ExploreModule(["count", "-d", "manufacturer", "-"]).run()
        finally:
            sys.stdin = old_stdin

    def test_stdio_uri_reads_jsonl_from_stdin(self):
        "jsonl:///stdio, spelled out, still works"
        with open("dk_testdata/mpg.jsonl") as f:
            content = f.read()
        old_stdin = sys.stdin
        sys.stdin = io.StringIO(content)
        try:
            explore_module.ExploreModule(
                ["count", "-d", "manufacturer", "jsonl:///stdio"]
            ).run()
        finally:
            sys.stdin = old_stdin

    @unittest.skipUnless(shutil.which("chafa"), "chafa is not installed")
    def test_histogram(self):
        "x histogram, to the terminal via chafa"
        tests = [
            ["histogram", "-d", "displ", "dk_testdata/mpg.jsonl"],
        ]
        self.go(tests)

    def test_histogram_to_file(self):
        "x histogram, to a file"
        outfile = "dk_testdata/_test_histogram.png"
        tests = [
            ["histogram", "-d", "displ", "-o", outfile, "dk_testdata/mpg.jsonl"],
        ]
        self.go(tests)
        self.assertTrue(os.path.exists(outfile))
        os.remove(outfile)

    def test_histogram_uses_theme_positive_color(self):
        "histogram bars use the active theme's positive color"
        with patch("dkit.plot2.quick.bar", return_value=object()) as bar, \
                patch("dkit.shell.terminal_image.show"):
            explore_module.ExploreModule([
                "histogram", "-d", "displ", "dk_testdata/mpg.jsonl",
            ]).run()

        self.assertEqual(bar.call_args.kwargs["color"], "positive")

    def test_summary(self):
        "x summary"
        tests = [
            ["summary", "-d", "displ", "dk_testdata/mpg.jsonl"],
            ["summary", "-f", "${displ} > 3", "-d", "displ", "dk_testdata/mpg.jsonl"],
        ]
        self.go(tests)

    @unittest.skipUnless(shutil.which("chafa"), "chafa is not installed")
    def test_plot(self):
        "x plot, to the terminal via chafa"
        tests = [
            ["plot", "-x", "cty", "-y", "hwy", "dk_testdata/mpg.jsonl"],
            ["plot", "-x", "cty", "-y", "hwy", "--type", "bar", "dk_testdata/mpg.jsonl"],
            ["plot", "-x", "cty", "-y", "hwy", "--type", "line", "dk_testdata/mpg.jsonl"],
            ["plot", "-x", "cty", "-y", "hwy", "--type", "area", "dk_testdata/mpg.jsonl"],
            ["plot", "-x", "cty", "-y", "hwy", "--type", "impulse", "dk_testdata/mpg.jsonl"],
        ]
        self.go(tests)

    def test_plot_uses_field_names_as_axis_labels(self):
        "plot uses the selected field names as axis labels"
        with patch("dkit.plot2.quick.scatter", return_value=object()) as scatter, \
                patch("dkit.shell.terminal_image.show"):
            explore_module.ExploreModule([
                "plot", "-x", "cty", "-y", "hwy",
                "dk_testdata/mpg.jsonl",
            ]).run()

        self.assertEqual(scatter.call_args.kwargs["xlabel"], "cty")
        self.assertEqual(scatter.call_args.kwargs["ylabel"], "hwy")

    def test_plot_to_file(self):
        "x plot, to a file"
        outfile = "dk_testdata/_test_plot.png"
        tests = [
            ["plot", "-x", "cty", "-y", "hwy", "-o", outfile, "dk_testdata/mpg.jsonl"],
        ]
        self.go(tests)
        self.assertTrue(os.path.exists(outfile))
        os.remove(outfile)


class TestRelations(TestDK):

    @classmethod
    def setUpClass(cls):
        cls.test_module = relations_module.RelationsModule

    def test_0_add(self):
        tests = [
            [
                "add", "-m", "dk_testdata/northwind.yml",
                "-M", "CustomerCustomerDemo", "--mc", "CustomerID",
                "-O", "Customers", "--oc", "CustomerID"
            ]
        ]
        self.go(tests)

    def test_1_ls(self):
        tests = [
            ["ls", "-m", "dk_testdata/northwind.yml", ]
        ]
        self.go(tests)

    def test_2_print(self):
        tests = [
            ["print", "-m", "dk_testdata/northwind.yml", "-r", "customercustomerdemo_customers", ]
        ]
        self.go(tests)

    def test_3_rm(self):
        # Relations moved to a new module
        tests = [
            ["rm", "-m", "dk_testdata/northwind.yml", "-r", "customercustomerdemo_customers", ]
        ]
        self.go(tests)

    def test_4_sql_reflect(self):
        tests = [
            ["sql-reflect", "-m", "dk_testdata/northwind.yml",  "--append", "-c", "northwind",
             "EmployeeTerritories"],
            # ["rm", "-m", "dk_testdata/northwind.yml", "-r", "employeeterritories_employees"],
        ]
        self.go(tests)


class TestRun(TestDK):
    """
    test run module
    """
    @classmethod
    def setUpClass(cls):
        cls.test_module = run_module.RunModule

    def test_etl_file(self):
        """test etl json to csv using uri"""
        tests = [
            ["etl", "-m", "dk_testdata/model.yml", "-o", "dk_testdata/test.csv", "dk_testdata/mpg.jsonl"]
        ]
        self.go(tests)

    def test_query(self):
        """test running a query"""
        tests = [
            ["query", "-m", "dk_testdata/northwind.yml", "--query", "select * from mpg",
             "sqlite:///dk_testdata/mpg.db"],
        ]
        self.go(tests)

    def test_melt(self):
        tests = [
            ["melt", "-i", "Year", "-K", "month", "-V", "temp",
             "dk_testdata/nottem.jsonl"]
        ]
        self.go(tests)

    def test_pivot(self):
        tests = [
            ["pivot", "-g", "manufacturer", "--mean", "hwy", "-p", "class", "--table",
             "dk_testdata/mpg.jsonl"],
            ["pivot", "-g", "manufacturer", "--mean", "hwy", "-p", "class",
             "dk_testdata/mpg.jsonl"],
        ]
        self.go(tests)


class TestTransforms(TestDK):
    """
    test query module
    """
    @classmethod
    def setUpClass(cls):
        cls.test_module = transform_module.TransformModule

    def test_transforms(self):
        """queries add"""
        tests = [
            ["create", "-m", "dk_testdata/model.yml", "-e", "mpg", "-t", "tmpg"],
            ["ls", "-m", "dk_testdata/model.yml"],
            ["print", "-m", "dk_testdata/model.yml", "-t", "tmpg"],
            ["rm", "--yes", "-m", "dk_testdata/model.yml", "-t", "tmpg"]
        ]
        self.go(tests)

    def test_uuid(self):
        tests = [
            ["uuid", "dk_testdata/mpg.csv"],
        ]
        self.go(tests)


class _TestEndpoints(TestDK):
    """
    test query module
    """
    @classmethod
    def setUpClass(cls):
        cls.test_module = endpoints_module.EndpointsModule

    def _test_0_add(self):
        """queries add"""
        tests = [
            ["add", "-m", "dk_testdata/model.yml", "-e", "nw_customers", "--file",
             "dk_testdata/select.sql"]
        ]
        self.go(tests)


class TestQueries(TestDK):
    """
    test query module
    """
    @classmethod
    def setUpClass(cls):
        cls.test_module = queries_module.QueriesModule

    def test_0_add(self):
        """queries add"""
        tests = [
            ["add", "-m", "dk_testdata/model.yml", "-q", "qMpg", "--file", "dk_testdata/select.sql"]
        ]
        self.go(tests)

    def test_1_ls(self):
        """queries ls -y model.yml"""
        tests = [
            ["ls", "-m", "dk_testdata/model.yml"]
        ]
        self.go(tests)

    def test_2_print(self):
        """print endpoint"""
        tests = [
            ["print", "-m", "dk_testdata/model.yml", "-q", "qMpg"]
        ]
        self.go(tests)

    def test_3_rm(self):
        """queries rm"""
        tests = [
            ["rm", "-m", "dk_testdata/model.yml", "-q", "qMpg"]
        ]
        self.go(tests)


class TestSchema(TestDK):

    @classmethod
    def setUpClass(cls):
        cls.test_module = schema_module.SchemaModule

    def test_infer(self):
        "s infer"
        tests = [
            ["infer", "dk_testdata/mpg.csv"],
            ["infer", "-e", "mpg", "dk_testdata/mpg.csv"],
        ]
        self.go(tests)

    def test_ls(self):
        "s ls"
        tests = [
            ["ls"],
            ["ls", "-l"],
        ]
        self.go(tests)

    def test_grep(self):
        "s grep"
        tests = [
            ["grep", ".*"],
        ]
        self.go(tests)

    def test_fgrep(self):
        "s fgrep"
        tests = [
            ["fgrep", ".*"],
        ]
        self.go(tests)

    def test_print(self):
        "s print"
        tests = [
            ["print", "-e", "mpg"],
        ]
        self.go(tests)

    def test_export(self):
        "s export"
        tests = [
            ["export", "-t", "dot", "-o", "dk_testdata/test.dot"],
            ["export", "-t", "model", "-o", "dk_testdata/model.yml"],
            ["export", "-t", "spark", "-o", "dk_testdata/spark.py"],
        ]
        self.go(tests)

    def test_sql_tables(self):
        pass

    def test_sql_reflect(self):
        pass


class TestXML(TestDK):
    """
    test XML module
    """
    @classmethod
    def setUpClass(cls):
        cls.test_module = xml_module.XMLModule

    def test_0_stat(self):
        """queries add"""
        tests = [
            ["stats", "dk_testdata/books.xml"],
            ["stats", "-l", "dk_testdata/books.xml"],
            ["stats", "-l", "--sort", "dk_testdata/books.xml"],
            ["stats", "-l", "--sort", "-N", "dk_testdata/books.xml"],
            ["stats", "-l", "--sort", "-N", "--reversed", "dk_testdata/books.xml"],
        ]
        self.go(tests)


if __name__ == '__main__':
    unittest.main()
