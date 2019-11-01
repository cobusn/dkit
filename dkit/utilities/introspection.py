#!/usr/bin/env python
"""
Introspection for python

Various methods and classes that assist in introspecting
python code.abs

Useful for documentation purposes
"""
import importlib
import inspect
import pkgutil
import sys
from typing import List

from ..shell import console


def is_list(the_object):
    """
    return true if the_object is a list or tuple but
    not a string
    """
    return isinstance(the_object, (list, tuple)) \
        and not isinstance(the_object, (str, bytes))


def get_class_names(module) -> List[str]:
    """
    extract  class names from a module or package

    Args:
        * module: module or package instance
    Returns:
        * list of class names
    """

    return [i[0] for i in inspect.getmembers(sys.modules[module.__name__], inspect.isclass)]


def get_function_names(object_: object) -> List[str]:
    """
    extract function names from a module or package

    Args:
        * module: module or package instance
    Returns:
        * list of function names
    """
    return [
        i[0] for i in inspect.getmembers(object_)
        if inspect.isfunction(i)
    ]


def get_module_names(module) -> List[str]:
    """
    extract module  names from a module or package

    Args:
        * module: module or package instance
    Returns:
        * list of module  names
    """
    if "__path__" in dir(module):
        search = module.__path__ if module else None
        return [
            i.name
            for i in filter(lambda x: not x.ispkg, pkgutil.iter_modules(search))
        ]
    else:
        return []


def get_method_names(object_) -> List[str]:
    """
    extract method  names from a module or package

    Args:
        * module: module or package instance
    Returns:
        * list of function names
    """
    return [
        i[0]
        for i in
        inspect.getmembers(object_, inspect.ismethod)
        + inspect.getmembers(object_, inspect.ismethoddescriptor)
    ]


def get_root_packages_names() -> List[str]:
    """
    extract root package names

    Returns:
        * list of all package names in path
    """
    return sorted([
        i.name
        for i in
        list(filter(lambda x: x.ispkg, pkgutil.iter_modules(None)))
    ])


def get_routine_names(object_: object) -> List[str]:
    """
    extract routine names from a module or package

    Args:
        * module: module or package instance
    Returns:
        * list of routine names
    """
    return [i[0] for i in inspect.getmembers(object_, inspect.isroutine)]


def get_packages_names(module) -> List[str]:
    """
    extract package names from a module or package

    Args:
        * module: module or package instance
    Returns:
        * list of package names
    """
    if '__path__' in dir(module):
        return [
            i.name
            for i in list(filter(lambda x: x.ispkg, pkgutil.iter_modules(module.__path__)))
        ]
    else:
        return []


def get_property_names(module) -> List[str]:
    """
    extract property  names from a module or package

    Args:
        * module: module or package instance
    Returns:
        * list of property names
    """
    return [
        i[0] for i in inspect.getmembers(module, inspect.isdatadescriptor)
        if not i[0].startswith("__")
    ]


def get_attribute_names(object_: object) -> List[str]:
    """
    extract list of initialized variables

    Args:
        object

    Returns:
        * list of variable names
    """
    return [
        i for i in dir(object_) if not any(
            [
                i.startswith("_"),
                callable(getattr(object_, i)),
                inspect.ismodule(getattr(object_, i))
            ]
        )]


class AbstractDocumenter(object):

    primitive = "object"

    def __str__(self):
        retval = f"\n{self.primitive}: {self.object_.__name__}\n"
        if self.doc:
            retval += "\n" + self.doc + "\n"
        return retval

    def format_list(self, the_list: List[str], threshold=20) -> str:
        """
        format a list to string.

        If the length of the list is longer than the threshold,
        use columnize instead
        """
        if len(the_list) > threshold:
            retval = "\n" + console.to_columns(the_list)
        else:
            retval = ""
            for item in the_list:
                retval += f"\n- {item}"
        return retval


class Documenter(AbstractDocumenter):

    _class_map = {}
    _root_documenter = None

    def __init__(self, object_, column_threshold=10, show_dunders=False):
        self.object_ = object_
        self.show_dunders = show_dunders
        self.column_threshold = column_threshold

    @property
    def doc(self) -> str:
        documentation = inspect.getdoc(self.object_)
        return documentation if documentation else ""

    @property
    def routines(self) -> List[str]:
        """list of method names"""
        routines_ = get_routine_names(self.object_)
        if not self.show_dunders:
            return [f"{i}()" for i in routines_ if not i.startswith("_")]
        else:
            return [f"{i}()" for i in routines_]

    @property
    def methods(self) -> List[str]:
        """list of method names"""
        methods_ = get_method_names(self.object_)
        if not self.show_dunders:
            return [i for i in methods_ if not i.startswith("_")]
        else:
            return methods_

    @property
    def functions(self) -> List[str]:
        """list of function names"""
        functions_ = get_function_names(self.object_)
        if not self.show_dunders:
            return [f"{i}()" for i in functions_ if not i.startswith("_")]
        else:
            return [f"{i}()" for i in functions_]

    @property
    def attributes(self):
        return get_attribute_names(self.object_)

    @staticmethod
    def _get_object(obj_path):
        nodes = obj_path.split(".")
        _object = importlib.import_module(nodes[0])
        is_a_module = True
        for i, node_name in enumerate(nodes[1:]):
            # when node_name is a package or module
            if is_a_module and \
                    node_name in get_packages_names(_object) \
                    + get_module_names(_object):
                _object = importlib.import_module(".".join(nodes[0:i+2]))
            else:
                is_a_module = False
                _object = getattr(_object, node_name)
        return _object

    @classmethod
    def from_path(cls, obj_path: str, dunders: bool = False):
        """
        Instantiate appropriate Documenter object
        """
        if len(obj_path) == 0:
            # Show all packages in python path
            return cls._root_documenter()
        else:
            # Document specific object
            _object = cls._get_object(obj_path)
            for k, v in cls._class_map.items():
                if k(_object) is True:
                    return v(_object, dunders)
            return None


class ClassDocumenter(Documenter):

    primitive = "class"

    @property
    def mro(self) -> List[str]:
        """list of base classes"""
        try:
            mro = [i.__name__ for i in inspect.getmro(self.object_)]
        except AttributeError as E:
            mro = [str(E)]
        return mro

    @property
    def properties(self) -> List[str]:
        """list of property names"""
        return get_property_names(self.object_)

    def __str__(self):
        doc = super().__str__()
        if self.mro:
            doc += "\nMethod Resolution Order:"
            for i in reversed(self.mro):
                doc += f"\n- {i}"
        if self.properties:
            doc += "\n\nProperties:"
            doc += self.format_list(self.properties, threshold=self.column_threshold)
        if self.routines:
            doc += "\n\nRoutines:"
            doc += self.format_list(self.routines, threshold=self.column_threshold)
        if self.attributes:
            doc += "\n\nAttributes:"
            doc += self.format_list(self.attributes, threshold=self.column_threshold)
        return doc


class FunctionDocumenter(Documenter):

    primitive = "function"

    def __init__(self, object_, show_dunders=False):
        super().__init__(object_, show_dunders)
        self.sig = inspect.signature(self.object_)

    @property
    def returns(self):
        if self.sig.return_annotation != inspect.Signature.empty:
            return str(self.sig.return_annotation)
        else:
            return None

    @property
    def parameters(self):
        return [i for i in self.sig.parameters]

    @property
    def annotated_parameters(self):
        retval = []
        for param in self.sig.parameters.values():
            entry = param.name
            if param.annotation != inspect.Parameter.empty:
                entry = f"{param.annotation}: {entry}"
            if param.default != inspect.Parameter.empty:
                entry = f"{entry}={param.default}"
            retval.append(entry)
        return retval

    def __str__(self):
        params = ", ".join(self.parameters)
        doc = f"\n{self.primitive}: {self.object_.__name__}({params})\n"
        if self.doc:
            doc += "\n" + self.doc + "\n"
        if self.annotated_parameters:
            doc += "\nParameters:"
            for param_ in self.annotated_parameters:
                doc += f"\n- {param_}"
        doc += "\n\nReturns:"
        if self.returns is not None:
            doc += f"\n- {self.returns}"
        else:
            doc += "\n- Not Annotated"
        return doc


class RootDocumenter(AbstractDocumenter):

    @property
    def packages(self):
        return get_root_packages_names()

    def __str__(self):
        if self.packages:
            doc = "\nPackages:\n"
            doc += self.format_list(self.packages)
        return doc


class ModuleDocumenter(Documenter):

    primitive = "module"

    @property
    def classes(self):
        """list of class names"""
        return get_class_names(self.object_)

    @property
    def packages(self):
        return get_packages_names(self.object_)

    @property
    def modules(self):
        return get_module_names(self.object_)

    def __str__(self):
        doc = super().__str__()
        if self.packages:
            doc += "\nPackages:\n"
            doc += self.format_list(self.packages, threshold=self.column_threshold)
        if self.modules:
            doc += "\n\nModules:"
            doc += self.format_list(self.modules, threshold=self.column_threshold)
        if self.classes:
            doc += "\n\nClasses:"
            doc += self.format_list(self.classes, threshold=self.column_threshold)
        if self.routines:
            doc += "\n\nRoutines:"
            doc += self.format_list(self.routines, threshold=self.column_threshold)
        if self.attributes:
            doc += "\n\nAttributes:"
            doc += self.format_list(self.attributes, threshold=self.column_threshold)
        return doc


Documenter._class_map.update({
        inspect.isclass: ClassDocumenter,
        inspect.ismodule: ModuleDocumenter,
        inspect.isroutine: FunctionDocumenter
    }
)

Documenter._root_documenter = RootDocumenter
