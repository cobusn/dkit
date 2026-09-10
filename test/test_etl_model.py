import sys
import unittest
import warnings
from configparser import ConfigParser
sys.path.insert(0, "..") # noqa
from dkit.etl.model import Entity, Relation, ModelManager, Connection, \
    Endpoint, Query, Secret
from dkit.etl import source
from create_data import NROWS
import jinja2
from pydantic import BaseModel

schema_1 = {
    "_id": "Integer(primary_key=True)",
    "name": "String(str_len=20)",
    "surname": "String()",
    "age": "Integer()",
    "parent": "Integer(index=True)",
}


class TestMapBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass


def add_sqlite_connection(c: ModelManager):
    c.add_connection(
        "sqlite",
        "sqlite:///input_files/sample.db",
    )
    return c


def add_encrypted_connection(c):
    c.add_connection(
        "mysql",
        "mysql+mysqldb://user:pass@localhost:3306/test",
        "password"
    )
    return c


def add_entity(c: ModelManager):
    c.entities["person"] = Entity(schema_1)
    return c


def add_endpoint(c: ModelManager):
    c.add_endpoint(
        "mysql_endpoint",
        "mysql",
        "person",
        "person"
    )
    return c


def add_sqlite_endpoint(c: ModelManager):
    c.add_endpoint(
        "sqlite_endpoint",
        "sqlite",
        "data",
        "person",
    )
    return c


class TestSecret(TestMapBase):

    def setUp(self):
        self.m = ModelManager.from_file("data/secret.yml")
        self.t = Secret("key", "value", {"prop": 10})

    def add_secret(self):
        self.m.add_secret("se1", self.t.key, self.t.secret, self.t.parameters)

    def test_read(self):
        self.add_secret()
        s2 = self.m.get_secret("se1")
        self.assertEqual(s2, self.t)

    def test_a_load(self):
        self.add_secret()
        self.m.save("data/test_save.yml")
        m = ModelManager.from_file("data/test_save.yml")
        self.assertEqual(
            self.m.get_secret("se1"), m.get_secret("se1")
        )


class TestEntity(TestMapBase):

    FIELD_DICT = {
        "_id":    {"type": "integer", "primary_key": True},
        "name":   {"type": "string", "str_len": 20},
        "surname": {"type": "string"},
        "age":    {"type": "integer"},
        "parent": {"type": "integer", "index": True},
    }

    def test_coerce(self):
        m = ModelManager.from_file("data/mtcars.yml")
        e = m.entities["mtcars"]
        with source.load("data/mtcars.csv") as in_src:
            transformed = list(e(in_src))
        self.assertTrue(isinstance(transformed[0]["carb"], int))

    def test_from_dict(self):
        e = Entity.from_dict(self.FIELD_DICT)
        self.assertIsInstance(e, Entity)
        decoded = Entity.decode(e.store)
        self.assertEqual(decoded["_id"]["type"], "integer")
        self.assertEqual(decoded["name"]["str_len"], 20)

    def test_from_cerberus_alias(self):
        """from_cerberus must remain a working alias for from_dict."""
        e1 = Entity.from_dict(self.FIELD_DICT)
        e2 = Entity.from_cerberus(self.FIELD_DICT)
        self.assertEqual(e1.store, e2.store)

    def test_as_entity_validator_schema_structure(self):
        e = Entity.from_dict(self.FIELD_DICT)
        v = e.as_entity_validator()
        self.assertIn("_id", v.schema)
        self.assertEqual(v.schema["_id"]["type"], "integer")
        self.assertTrue(v.schema["_id"]["primary_key"])
        self.assertEqual(v.schema["name"]["str_len"], 20)

    def test_sorted_dict_pk_first(self):
        e = Entity.from_dict(self.FIELD_DICT)
        keys = list(e.sorted_dict.keys())
        self.assertEqual(keys[0], "_id")

    def test_iter_validate_yields_all_rows(self):
        e = Entity.from_dict(self.FIELD_DICT)
        rows = [
            {"_id": 1, "name": "Alice", "surname": "Smith", "age": 30, "parent": 0},
            {"_id": 2, "name": "Bob",   "surname": "Jones", "age": 25, "parent": 1},
        ]
        result = list(e.iter_validate(rows))
        self.assertEqual(len(result), 2)

    def test_from_iterable_infers_fields(self):
        rows = [
            {"id": 1, "label": "foo", "value": 1.5},
            {"id": 2, "label": "bar", "value": 2.5},
        ]
        e = Entity.from_iterable(rows)
        self.assertIn("id", e.store)
        self.assertIn("label", e.store)
        self.assertIn("value", e.store)


class TestQuery(TestMapBase):
    q = """
    select * from persons where id = {{ _id }}
    """

    def test_variables(self):
        q = Query(self.q, "test query")
        self.assertEqual(
            q.variables,
            {"_id"},
        )

    def test_render_undefined(self):
        """
        Should raise jinja2 errror if variables not
        defined
        """
        q = Query(self.q, "test query")
        with self.assertRaises(jinja2.exceptions.UndefinedError):
            _ = q()

    def test_render(self):
        """test render"""
        q = Query(self.q, "test query")
        self.assertEqual(
            q(_id=1),
            "select * from persons where id = 1"
        )

    def test_listing(self):
        qlist = {"1": Query("t1"), "2": Query("{{ _id }}", "d2")}
        ref = [
            {'name': '1', 'description': '', 'variables': 0},
            {'name': '2', 'description': 'd2', 'variables': 1}
        ]
        ls = Query.get_listing(qlist)
        self.assertEqual(ref, ls)


class TestModel(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.schema_1 = {
            "_id": "Integer(primary_key=True)",
            "name": "String(str_len=20)",
            "surname": "String()",
            "age": "Integer()",
            "parent": "Integer(index=True)",
        }
        cls.c = ModelManager.from_file()

    def test_constructors(self):
        """
        Test various constructor configurations
        """
        # no params
        m = ModelManager.from_file()
        self.assertTrue(isinstance(m, ModelManager))
        # filename provided
        m = ModelManager.from_file("model.yml")
        self.assertTrue(isinstance(m, ModelManager))

        # config filename
        m = ModelManager.from_file("model.yml", "dk.ini")
        self.assertTrue(isinstance(m, ModelManager))
        # config instance
        m = ModelManager.from_file("model.yml", ConfigParser())
        self.assertTrue(isinstance(m, ModelManager))

    def test_etl_objects_are_pydantic_models(self):
        """ETL configuration objects should use Pydantic validation."""
        self.assertTrue(issubclass(Connection, BaseModel))
        self.assertTrue(issubclass(Endpoint, BaseModel))
        self.assertTrue(issubclass(Relation, BaseModel))
        self.assertTrue(issubclass(Query, BaseModel))
        self.assertTrue(issubclass(Secret, BaseModel))

    def test_add_connection(self):
        c = ModelManager.from_file()
        c = add_encrypted_connection(c)
        conn = c.get_connection("mysql")
        self.assertEqual(conn.password, "password")

    def test_get_uri(self):
        c = ModelManager.from_file()
        add_encrypted_connection(c)
        add_entity(c)
        add_endpoint(c)
        uri = c.get_uri("::mysql_endpoint")
        self.assertEqual(uri["password"], "password")

    def test_source_1(self):
        c = ModelManager.from_file()
        with c.source("input_files/sample.jsonl.bz2") as src:
            self.assertEqual(len(list(src)), NROWS)

    def test_source_2(self):
        c = ModelManager.from_file()
        add_sqlite_connection(c)
        add_sqlite_endpoint(c)
        with c.source("::sqlite_endpoint") as src:
            self.assertEqual(len(list(src)), NROWS)

    def test_unbounded_sink(self):
        m = ModelManager.from_file()
        with m.source("input_files/sample.jsonl.bz2") as src:
            with m.sink("output/model.jsonl") as snk:
                snk.process(src)

    def _todo_test_bounded_sink(self):
        m = ModelManager.from_file()

    def test_entity(self):
        reversed = {
            '_id': {'type': 'integer', 'primary_key': True},
            'name': {'type': 'string', 'str_len': 20},
            'surname': {'type': 'string'},
            'age': {'type': 'integer'},
            'parent': {'type': 'integer', "index": True}
        }

        schema = Entity(self.schema_1)
        decoded = schema.decode(schema.store)
        self.assertEqual(decoded, reversed)

        encoded = schema.encode(decoded)
        self.assertEqual(encoded, self.schema_1)

    def test_relation(self):
        m = ModelManager(None)
        m.load("data/etl_schema.yml")
        m.entities["left"] = Entity(self.schema_1)
        m.entities["right"] = Entity(self.schema_1)
        r = Relation("left", ["parent"], "right", ["_id"])
        m.relations["left_right"] = r
        m.save("data/etl_schema_relation.yml")
        m.load("data/etl_schema_relation.yml")

    def test_endpoints(self):
        """
        test add/edit/delete connections
        """
        m = ModelManager(None)
        conn = Connection.from_uri("sqlite:///test.db")
        m.connections["test"] = conn
        m.save("data/conn.yml")
        m = ModelManager(None).load("data/conn.yml")
        self.assertEqual(m.connections["test"], conn)

    def test_model_manager(self):
        m = ModelManager(None)
        m.load("data/etl_schema.yml")
        m.entities["test"] = Entity(self.schema_1)
        m.save("data/etl_schema.yml")

        n = ModelManager(None)
        n.load("data/etl_schema.yml")
        self.assertEqual(m.as_dict(), n.as_dict())


if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=ImportWarning)
        unittest.main()
