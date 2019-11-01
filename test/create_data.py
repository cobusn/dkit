#
# Copyright (C) 2016  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
from __future__ import print_function
import csv
import os
import json
import datetime
import bz2

from faker import Factory
import random

import sys; sys.path.insert(0, "..") # noqa
from dkit.etl.extensions.ext_msgpack import MsgpackSink
from dkit.etl.extensions import ext_tables, ext_sql_alchemy, ext_bxr
from dkit.etl.writer import FileWriter, Bz2Writer
from dkit.etl import schema, sink
from dkit.etl.transform import CoerceTransform
FIELD_NAMES = ["id", "name", "company", "ip", "birthday", "year", "score"]
NROWS = 500


def generate_data():
    print("Creating data..")
    fake = Factory.create()

    clientlist = []
    field_names = FIELD_NAMES

    for _ in range(NROWS):
            clientlist.append([fake.ssn(), fake.name(), fake.company(),
                               fake.ipv4(), str(fake.date_time()), int(fake.year()),
                               random.uniform(0, 100)])
    return clientlist, field_names


def write_csv(clientlist, field_names):
    print("Writing CSV")
    with open(os.path.join("input_files", "sample.csv"), "w") as outfile:
        w = csv.writer(outfile, lineterminator="\n")
        w.writerow(field_names)
        for r in clientlist:
            w.writerow(r)


def write_csv_bz2(clientlist, field_names):
    print("Writing csv.bz2")
    with bz2.open(os.path.join("input_files", "sample.csv.bz2"), "wt") as outfile:
        w = csv.writer(outfile, lineterminator="\n")
        w.writerow(field_names)
        for r in clientlist:
            w.writerow(r)


def write_jsonl(clientlist, field_names, stream):

    def default_encoder(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        else:
            raise TypeError

    for row_object in clientlist:
        # ensure_ascii=False is essential or errors/corruption will occur
        dict_obj = dict(zip(field_names, row_object))
        json_record = json.dumps(dict_obj, ensure_ascii=False, default=default_encoder)
        stream.write(json_record + "\n")


def write_bxr(clientlist, field_names):
    print("Writing bxr")
    dict_obj = [dict(zip(field_names, row)) for row in clientlist]
    ext_bxr.BXRSink(FileWriter("input_files/sample.bxr")).process(dict_obj)


def write_bxr_bz2(clientlist, field_names):
    print("Writing bxr bzip")
    dict_obj = [dict(zip(field_names, row)) for row in clientlist]
    ext_bxr.BXRSink(
        Bz2Writer("input_files/sample.bxr.bz2")
    ).process(dict_obj)


def write_msgpak(clientlist, field_names):
    print("Writing msgpack")
    dict_obj = [dict(zip(field_names, row)) for row in clientlist]
    MsgpackSink(FileWriter("input_files/sample.mpak", "wb")).process(dict_obj)


def write_msgpak_bz2(clientlist, field_names):
    print("Writing msgpack")
    dict_obj = [dict(zip(field_names, row)) for row in clientlist]
    MsgpackSink(Bz2Writer("input_files/sample.mpak.bz2", "wb")).process(dict_obj)


def write_pkl(the_data, the_schema):
    print("writing pkl")
    sink.PickleSink(FileWriter("input_files/sample.pkl", mode="wb")).process(the_data)


def write_hdf5(the_data, the_schema):
    print("writing hdf5")
    accessor = ext_tables.PyTablesAccessor("input_files/sample.h5")
    accessor.create_table("/data", the_schema)
    snk = ext_tables.PyTablesSink(accessor, "/data")
    snk.process(the_data)


def write_sqlite(the_data, the_schema):
    print("Writing sqlite3 database")
    accessor = ext_sql_alchemy.SQLAlchemyAccessor("sqlite:///input_files/sample.db")
    accessor.create_table("data", the_schema)
    print(the_schema)
    ext_sql_alchemy.SQLAlchemySink(accessor, "data").process(
        CoerceTransform(the_schema)(the_data)
    )


if __name__ == "__main__":

    # Generate data and schema
    client_data, field_names = generate_data()
    the_data = [dict(zip(field_names, row)) for row in client_data]
    the_schema = schema.EntityValidator.from_iterable(the_data)

    write_csv(client_data, field_names)
    write_csv_bz2(client_data, field_names)
    write_msgpak(client_data, field_names)
    write_msgpak_bz2(client_data, field_names)
    write_bxr(client_data, field_names)
    write_bxr_bz2(client_data, field_names)

    # Write json to text
    print("Write jsonl")
    with open(os.path.join("input_files", "sample.jsonl"), 'w') as f:
        write_jsonl(client_data, field_names, f)

    # Write json to Bzip2
    print("Write jsonl.bz2")
    stream = bz2.open(os.path.join("input_files", "sample.jsonl.bz2"), "wt")
    write_jsonl(client_data, field_names, stream)
    stream.close()

    write_pkl(the_data, the_schema)
    write_hdf5(the_data, the_schema)
    write_sqlite(the_data, the_schema)
