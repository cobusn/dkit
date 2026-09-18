********************************
How to author a dkit style pack
********************************

A style pack is an installable distribution containing one ``style.yaml``
manifest and native renderer resources. The manifest is declarative: dkit
loads it through installed-distribution metadata and never imports provider
Python code.

Manifest and resources
======================

The reference pack is ``dkit/stylepacks/dkit_blue``. A pack should provide
shared ``colors``, ``typography``, ``page``, and ``charts`` tokens, then list
the native files under ``formats``:

.. code-block:: yaml

   schema_version: 1
   id: company-blue
   name: Company Blue
   requires_dkit: ">=26.9"
   typography:
     heading: Noto Sans
     body: Noto Sans
     mono: DejaVu Sans Mono
   fonts:
     - family: Noto Sans
       regular: fonts/NotoSans-Regular.ttf
       bold: fonts/NotoSans-Bold.ttf
       italic: fonts/NotoSans-Italic.ttf
       bold_italic: fonts/NotoSans-BoldItalic.ttf
   page:
     size: a4                 # a4, letter, legal, or a5
     orientation: portrait    # or landscape
   formats:
     reportlab:
       layout: reportlab/layout.yaml
       cover: assets/cover.pdf
     html:
       stylesheet: html/document.css
       email_stylesheet: html/email.css
     latex:
       class: company-blue
       resources: latex
       engine: pdflatex
     docx:
       template: docx/template.docx
     matplotlib:
       screen: matplotlib/screen.mplstyle
       print: matplotlib/print.mplstyle

All referenced files must be inside the manifest directory. Keep third-party
font, image, template, and code licences in a ``LICENSES`` subdirectory.
Bundled font faces are registered for Matplotlib and ReportLab; standalone
HTML embeds them as data URLs. Email output continues to use CSS fallbacks.
Matplotlib files should use matplotlib's native syntax; remember that a
leading ``#`` starts a comment in an ``.mplstyle`` file.

Package and register
====================

Include every native resource in the wheel. For setuptools projects this can
be done with package data or a ``MANIFEST.in`` rule, followed by a clean-wheel
check. Install the wheel into the target virtual environment and register it:

.. code-block:: console

   pip install company_style_pack.whl
   dk Stylesheets register company-blue --distribution company-style-pack \
      --manifest company_style_pack/styles/company-blue/style.yaml
   dk Stylesheets validate company-blue
   dk Stylesheets show company-blue

Registration validates the manifest, dkit compatibility, every referenced
resource, and a deterministic style-root fingerprint before changing
``~/.dk.ini``. Re-registering changed resources requires ``--replace``.
Use ``--config`` to select another INI file.

Build and preview
=================

Select a registered pack for one-shot documents or configure a report:

.. code-block:: console

   dk build doc --style company-blue --format reportlab \
      --title "Quarterly report" --output report.pdf report.md
   dk Stylesheets preview company-blue --output preview

The preview contains ReportLab PDF, LaTeX PDF, DOCX, HTML, email HTML, and
screen/print chart examples. ``configuration.style`` in ``report.yaml`` and
``[DOC] style`` in the DK INI file provide project and default selection.

Runnable examples
=================

The ``examples/doc2_stylepacks`` folder demonstrates the API, one-shot
Markdown, and report-project build paths. To render every supported output
for one registered pack into a review folder, run:

.. code-block:: console

   python examples/doc2_stylepacks/build_all.py dkit-blue --output output

The output includes inlined email HTML and a complete ``.eml`` MIME message;
no mail is sent.

Trust boundary
==============

Only install style distributions from sources you trust. A pack is not
allowed to execute Python during registration, but its native resources are
consumed by document tools: LaTeX classes and packages are code executed by
TeX, templates can affect generated documents, and fonts/images may carry
their own licensing or security considerations. Validate and fingerprint
packs in the same environment where they will be used, and re-register after
upgrading a style distribution.
