"""
Build an email message from a dkit Document.

The example creates a multipart/alternative message containing both a
plain-text and CSS-inlined HTML representation of the document.  It writes
the resulting MIME message to ``examples/output/example_email.eml``; it does
not send an email.
"""
from pathlib import Path
import sys
from textwrap import dedent

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dkit.doc2 import document as doc
from dkit.doc2.html_renderer import HtmlRenderer
from dkit.utilities.smtp_helper import DocumentMessage


HERE = Path(__file__).resolve().parent


def build_document():
    """Build the document used for the example email."""
    report = doc.Document(
        title="Quarterly Update",
        sub_title="Q2 2026",
        author="Data Team",
        title_date="2026/06/30",
    )
    report.add_template(dedent("""
        ## Summary

        This message provides a brief overview of the **Q2 2026** results.
        All figures are preliminary and subject to revision.

        ## Highlights

        - Revenue increased by *12 %* compared to Q1.
        - Three new integrations were shipped on schedule.
        - Customer satisfaction reached **4.8 / 5**.

        ## Action Items

        1. Review the attached dashboard before the Friday stand-up.
        2. Confirm headcount numbers with HR by end of week.
        3. Reply to this message with any corrections.

        ## Notes

        > If you have questions, please open a ticket via the service portal
        > or reply directly to this message.

        Regards,
        *Data Team*
    """))
    return report


def build_email(report):
    """Build a MIME email without sending it."""
    return DocumentMessage(
        subject="Quarterly Update — Q2 2026",
        sender="noreply@example.com",
        recipients=["recipient@example.com"],
        document=report,
        css=HtmlRenderer.get_email_css(),
    )


if __name__ == "__main__":
    output_dir = HERE / "output"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "example_email.eml"

    message = build_email(build_document())
    output_file.write_text(message.get_mime_content(), encoding="utf-8")
    print(f"Wrote {output_file}")
