import ast
import tokenize
import intervaltree


def _compute_interval(node):
    min_lineno = node.lineno
    max_lineno = node.lineno
    for node in ast.walk(node):
        if hasattr(node, "lineno"):
            min_lineno = min(min_lineno, node.lineno)
            max_lineno = max(max_lineno, node.lineno)
    return (min_lineno, max_lineno + 1)


def file_to_tree(filename):
    with tokenize.open(filename) as f:
        parsed = ast.parse(f.read(), filename=filename)
    tree = intervaltree.IntervalTree()
    for node in ast.walk(parsed):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            start, end = _compute_interval(node)
            tree[start:end] = node
    return tree


if __name__ == "__main__":
    o = file_to_tree("create_sample.py")[7]
    print([z.data.name for z in o])
