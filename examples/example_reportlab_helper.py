import sys; sys.path.insert(0, "..")   # noqa
from dkit.doc2.rl_renderer import RLRenderer
from dkit.doc2 import document as doc
from lorem_text import lorem
# from dkit.doc2.renderer import render_doc_format
from dkit.etl import source


md = """# Heading 1 with *emph*
This is a paragraph with some *italic text*  and some **Bold**.
what is _this_
"""
md2 = """# Some lists
And now for some list items:
* **Item 1** you know
* Item 2
* Item 3
    - sub 1
    - sub 2
"""

md_code = """Lets add some code:
```python
import reportlab
print(reportlab)
..
..
bla bla bla
```

Here is also some inline code `cat test.json | jq .`
"""

md_html = """here are some inline html
<h1>heading</h1>
"""

md_big = """
# Heading 1

## Heading 2

### Heading 3

#### Heading 4

##### Heading 5

###### Heading 6
This is a paragraph of text.  It has some **bold** text and some *italics*.
It also has some `inline code`.

Tenetur enim fuga, dolorum exercitationem commodi voluptate voluptates animi
alias, sit corporis repellat, quibusdam illo nemo doloremque alias corporis
atque natus earum error, illum suscipit aut architecto odit quaerat debitis
ipsa qui aspernatur. Ipsa voluptate velit deleniti expedita, sunt velit quos
nam placeat, velit accusamus vel ut optio ducimus nemo nihil magnam,
reprehenderit voluptatibus enim at minus. Architecto ad officia quibusdam,
incidunt quia rem enim, numquam vel molestiae iste quis fugiat voluptas ipsum
debitis aspernatur praesentium, accusamus obcaecati dicta ipsum assumenda
itaque nisi alias exercitationem, amet natus delectus optio laboriosam illo rk

Tenetur enim fuga, dolorum exercitationem commodi voluptate voluptates animi
alias, sit corporis repellat, quibusdam illo nemo doloremque alias corporis
atque natus earum error, illum suscipit aut architecto odit quaerat debitis
ipsa qui aspernatur. Ipsa voluptate velit deleniti expedita, sunt velit quos
nam placeat, velit accusamus vel ut optio ducimus nemo nihil magnam,
reprehenderit voluptatibus enim at minus. Architecto ad officia quibusdam,
incidunt quia rem enim, numquam vel molestiae iste quis fugiat voluptas ipsum
debitis aspernatur praesentium, accusamus obcaecati dicta ipsum assumenda
itaque nisi alias exercitationem, amet natus delectus optio laboriosam illo r
This include hard break

This is another paragraph.  We can create a [link](https://www.example.com) to
a website.  This is an example of an [inline link with a
title](https://www.example.com "This is the title").

Here's an unordered list:

* Item 1
* Item 2
    * Sub-item 1
    * Sub-item 2
* Item 3


Here's an ordered list:

1. Item 1
2. Item 2
3. Item 3

Here's a blockquote:

> This is a blockquote.  It can span multiple lines.
> It's useful for quoting other text.
>
> And another

Here's a code block (using backticks):

```python
print("Hello, world!")
```

Here's another code block (using indented 4 spaces):

    print("Hello, world! from indented code block")

Here's an image:
"""
"""
![Image alt text](https://via.placeholder.com/150 "Placeholder Image")
"""
pdf_doc = doc.Document("Test Document", "Reportlab Helper", "Author Name",
                       email="email@address.com")
pdf_doc.add_markdown("# Heading 1")
pdf_doc.add_markdown(lorem.paragraph())
pdf_doc.add_markdown(lorem.paragraph())
pdf_doc.add_markdown("{{ image('examples/data/plotdata.pdf', title='the title', height=3) }}")
pdf_doc.add_element(doc.Heading([doc.Str("Heading 2")], 1))
pdf_doc.add_element(doc.Paragraph([doc.Str(lorem.paragraph())]))
pdf_doc.add_markdown(md)
pdf_doc.add_markdown(md2)
pdf_doc.add_markdown(md_code)
pdf_doc.add_markdown(md_big)


class Data:

    def __init__(self):
        with source.load("examples/data/nottem_temp.jsonl") as infile:
            self.data = list(infile)[:12]

    @doc.wrap_json
    def table(self):
        table = doc.Table(
            self.data,
            [
                doc.Column("Year", "Year"),
                doc.Column("Month", "Month"),
                doc.Column("Temp", "Temp", align="r"),
            ],
            align="left"
        )
        return table


md_table = """
# A Table
Lets add a ... drumroll ... Table

{{ data.table() }}
"""
pdf_doc.add_markdown(md_table, data=Data())

RLRenderer(pdf_doc, allow_soft_breaks=False).render("test.pdf")
