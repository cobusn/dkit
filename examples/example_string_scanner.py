from dkit.parsers.helpers import SearchScanner


def assign(result):
    print("Assignment", result.groups())


def operator(result):
    print("Operator:", result.groups())


def number(result):
    print("Number:", result.groups())


def variable(result):
    print("Variable:", result.groups())


def remaining(text):
    print("Remainder", "'" + text + "'")


rules = [
    (r"(=)", assign),
    (r"([a-z]+)", variable),
    (r"(\d+)", number),
    (r"([+_*/])", operator),
]

scanner = SearchScanner(rules, remaining, break_on_match=False)
scanner.scan("a = 10 + 5 + 4")
