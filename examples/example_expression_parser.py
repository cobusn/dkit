import sys; sys.path.insert(0, "..") # noqa
from dkit.parsers.infix_parser import ExpressionParser  # noqa

data = {"a": 10, "b": 20}

p = ExpressionParser("1/${a} * ${b}")
print(p(data))
