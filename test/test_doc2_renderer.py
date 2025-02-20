import unittest
import sys; sys.path.insert(0, "..")  # noqa
from dkit.doc2.md_to_doc import DocRenderer
import mistune
import json


markdown = """# Heading 1 with *emph*
This is a paragraph with some *italic text*  and some **Bold**.
what is _this_

* **Item 1**: you know
    - Lower
    - and lower
* Item 2

[Alt *Text*](https://example.com)
![Image **Description**](/path/to/image.jpg)
"""

"""
**Bold text** _Italic text_
```python
print("Hello, world!")
```
"""


def test_doc():
    renderer = DocRenderer()
    for token in mistune.create_markdown(renderer=renderer)(markdown):
        print(token)


if __name__ == '__main__':
    test_doc()
