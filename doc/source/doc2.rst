*****
doc2
*****

.. automodule:: dkit.doc2.document

Document
========

.. autoclass:: dkit.doc2.document.Document
   :members:

Content elements
=================

.. autoclass:: dkit.doc2.document.Heading
   :members:

.. autoclass:: dkit.doc2.document.Paragraph
   :members:

.. inheritance-diagram:: dkit.doc2.document.Block

.. autoclass:: dkit.doc2.document.Block
   :members:

.. inheritance-diagram:: dkit.doc2.document.BlockQuote

.. autoclass:: dkit.doc2.document.BlockQuote
   :members:

.. autoclass:: dkit.doc2.document.List
   :members:

.. autoclass:: dkit.doc2.document.ListItem
   :members:

.. autoclass:: dkit.doc2.document.CodeBlock
   :members:

.. inheritance-diagram:: dkit.doc2.document.Image

.. autoclass:: dkit.doc2.document.Image
   :members:
   :exclude-members: from_dict, to_dict

.. inheritance-diagram:: dkit.doc2.document.Table

.. autoclass:: dkit.doc2.document.Table
   :members:
   :exclude-members: from_dict, to_dict

.. inheritance-diagram:: dkit.doc2.document.Column
   :private-bases:

.. autoclass:: dkit.doc2.document.Column
   :members:

.. inheritance-diagram:: dkit.doc2.document.PageBreak

.. autoclass:: dkit.doc2.document.PageBreak
   :members:
   :exclude-members: from_dict, to_dict

Renderers
==========

HtmlRenderer
-------------

.. automodule:: dkit.doc2.html_renderer

.. autoclass:: dkit.doc2.html_renderer.HtmlRenderer
   :members:

RLRenderer
-----------

.. automodule:: dkit.doc2.rl_renderer

.. autoclass:: dkit.doc2.rl_renderer.RLRenderer
   :members:

DocxRenderer
-------------

.. automodule:: dkit.doc2.docx_renderer

.. autoclass:: dkit.doc2.docx_renderer.DocxRenderer
   :members:

MarkdownRenderer
-----------------

.. automodule:: dkit.doc2.md_renderer

.. autoclass:: dkit.doc2.md_renderer.MarkdownRenderer
   :members:

Builder
========

.. automodule:: dkit.doc2.builder

.. inheritance-diagram:: dkit.doc2.builder.DocumentInfo

.. autoclass:: dkit.doc2.builder.DocumentInfo
   :members:

.. inheritance-diagram:: dkit.doc2.builder.DocumentConfiguration

.. autoclass:: dkit.doc2.builder.DocumentConfiguration
   :members:

.. inheritance-diagram:: dkit.doc2.builder.DocumentDefinition

.. autoclass:: dkit.doc2.builder.DocumentDefinition
   :members:

.. autoclass:: dkit.doc2.builder.Builder
   :members:

.. autoclass:: dkit.doc2.builder.ProjectFolderInitializer
   :members:
   :special-members: __call__
