#
# XML to Json converter
#
# Cobus Nel 2017
#

import argparse
import json
import sys

import lxml.etree as et
from dkit.data import xml_helper as xh
from dkit.utilities import log_helper as lh

logger = lh.stderr_logger()

VERSION = "0.1"


def xml_to_rows(file_ob, boundary, transform_attributes):
    """
    transform xml to rows

    :param boundary: row boundary tag
    :param transform_attributes: add '@' in front of attr name
    """
    for (event, node) in et.iterparse(file_ob, tag=boundary):
        yield xh.etree_to_dict(
            node,
            transform_attributes=transform_attributes
        )
        # Below is required to free up used memory
        node.clear()
        while node.getprevious() is not None:
            del node.getparent()[0]


def transform(args):
    """
    read xml and transform to json
    """
    boundary = args.boundary
    verbose = args.verbose
    for count, row in enumerate(
        xml_to_rows(args.infile, boundary, args.transform_attributes)
    ):
        args.outfile.write(json.dumps(row[boundary]) + "\n")
        if count % 10000 == 0 and verbose:
            logger.info("Processed {} rows".format(count))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'infile',
        nargs='?',
        type=argparse.FileType("rb"),
        help="Input file (Default to stdout)",
    )

    parser.add_argument(
        '-o',
        '--outfile',
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Output file (Default to stdout)",
    )

    parser.add_argument(
        "-b",
        "--boundary",
        required=True,
        help="Boundary XML tag name",
    )

    parser.add_argument(
        "-t",
        "--transform-attributes",
        default=False,
        help="transform attrribute names (add @ char to name)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Update to stderr",
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s {}'.format(VERSION),
    )

    args = parser.parse_args()
    transform(args)


if __name__ == "__main__":
    main()
