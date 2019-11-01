from dkit.data.filters import ExpressionFilter

data = [
    {"year": 1990},
    {"year": 2001},
    {"year": 1998},
]

for i in filter(ExpressionFilter("${year} <= 2000"), data):
    print(i)
