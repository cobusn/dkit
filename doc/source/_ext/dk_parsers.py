"""Factory functions returning the ``dk`` CLI's argparse parsers.

Used only by ``sphinx-argparse`` (via the ``:ref:`` option) to render the
CLI reference page. Each function instantiates the corresponding
``lib_dk`` module class, which builds its ``argparse.ArgumentParser`` in
``__init__``, and sets ``prog`` to the invocation shown to users.
"""
from argparse import ArgumentParser


def _parser(module_name, class_name, prog):
    import importlib
    module = importlib.import_module(f"lib_dk.{module_name}")
    cls = getattr(module, class_name)
    instance = cls([])
    instance.parser.prog = prog
    return instance.parser


def admin_parser() -> ArgumentParser:
    return _parser("admin_module", "AdminModule", "dk admin")


def build_parser() -> ArgumentParser:
    return _parser("build_module", "BuildModule", "dk build")


def connections_parser() -> ArgumentParser:
    return _parser("connections_module", "ConnectionsModule", "dk connections")


def diff_parser() -> ArgumentParser:
    return _parser("diff_module", "DiffModule", "dk diff")


def endpoints_parser() -> ArgumentParser:
    return _parser("endpoints_module", "EndpointsModule", "dk endpoints")


def mapping_parser() -> ArgumentParser:
    return _parser("relations_module", "RelationsModule", "dk mapping")


def queries_parser() -> ArgumentParser:
    return _parser("queries_module", "QueriesModule", "dk queries")


def run_parser() -> ArgumentParser:
    return _parser("run_module", "RunModule", "dk run")


def schemas_parser() -> ArgumentParser:
    return _parser("schema_module", "SchemaModule", "dk schemas")


def transforms_parser() -> ArgumentParser:
    return _parser("transform_module", "TransformModule", "dk transforms")


def vault_parser() -> ArgumentParser:
    return _parser("store_module", "VaultModule", "dk vault")


def xplore_parser() -> ArgumentParser:
    return _parser("explore_module", "ExploreModule", "dk xplore")


def xml_parser() -> ArgumentParser:
    return _parser("xml_module", "XMLModule", "dk XML")
