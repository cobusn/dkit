# heading with **bold**
This is a first paragraph testing the *viability* of
the md to latex conversion.

## Heading L2
text 2 
### Heading L3
text 3

# Verbatim 

## inline
This is `inline` verbatim text
## simple

    line 1 of verbatim text
    line 2 of verbatim texta

## code
The following represent verbatim code in python:

```python
import string
print(string.punctiuation)
```

## latex
This is how to include verbatim latex:

```texinclude
\emph{verbatim text}
```

## jsoninclude
This is how to add json directly to the ast:

```jsoninclude
{"~>": "paragraph", "data": [{"~>": "text", "data": "\nThis is the included data"}]}
```

## verbatim

```
verbatim
```

# Lists
##  Unorderedlists
text in the *sub* heading:

* **one**
* two

Final

## Ordered lists

1. One
2. Two

# Image
Here is an image:

![w=12,h=5}](input_files/python-logo.png).

# Math

Simple $m$ symbol

# Table
A simple table:

| Tables:5 |      Are      |  Cool |
|----------|:-------------:|------:|
| col 1 is |  left-aligned | $1600 |
| col 2 is |    centered   |   $12 |
| col 3 is | right-aligned |    $1 |

# An h1 header
==============

Paragraphs are separated by a blank line.

2nd paragraph. *Italic*, **bold**, `monospace`. Itemized lists
look like:

  * this one
  * that one
  * the other one

Note that --- not considering the asterisk --- the actual text
content starts at 4-columns in.

> Block quotes are
> written like so.
>
> They can span multiple paragraphs,
> if you like.

Use 3 dashes for an em-dash. Use 2 dashes for ranges (ex. "it's all in
chapters 12--14"). Three dots ... will be converted to an ellipsis.

An h2 header
------------

Here's a numbered list:

 1. first item
 2. second item
 3. third item

Note again how the actual text starts at 4 columns in (4 characters
from the left side). Here's a code sample:

    # Let me re-iterate ...
    for i in 1 .. 10 { do-something(i) }

As you probably guessed, indented 4 spaces. By the way, instead of
indenting the block, you can use delimited blocks, if you like:

```
define foobar() {
    print "Welcome to flavor country!";
}
```

(which makes copying & pasting easier). You can optionally mark the
delimited block for Pandoc to syntax highlight it:

```python
import time
# Quick, count to ten!
for i in range(10):
    # (but not *too* quick)
    time.sleep(0.5)
    print i
```

### An h3 header ###

Now a nested list:

 1. First, get these ingredients:

      * carrots
      * celery
      * lentils

 2. Boil some water.

 3. Dump everything in the pot and follow
    this algorithm:

        find wooden spoon
        uncover pot
        stir
        cover pot
        balance wooden spoon precariously on pot handle
        wait 10 minutes
        goto first step (or shut off burner when done)

    Do not bump wooden spoon or it will fall.

Notice again how text always lines up on 4-space indents (including
that last line which continues item 3 above). Here's a link to [a
website](http://foo.bar). Here's a link to a [local doc](local-doc.html). 

Inline math equations go in like so: $\omega = d\phi / dt$. Display
math should get its own line and be put in in double-dollarsigns:

$$I = \int \rho R^{2} dV$$

Done.
