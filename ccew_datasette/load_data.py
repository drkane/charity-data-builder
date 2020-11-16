from collections import defaultdict
import io
from zipfile import ZipFile
from sqlite_utils import Database

import bcp
import tqdm

from tables import TABLES, SUBSID_TABLES
from views import VIEWS


def clean_row(row):
    for v in row:
        v = v.strip()
        if v == "":
            v = None
        yield v


def generate_rows(reader, table_name, table_def):
    column_names = list(table_def["columns"].keys())

    for row in reader:
        if table_name in SUBSID_TABLES:
            if row[1] != "0":
                continue
            del row[1]

        row = dict(zip(column_names, clean_row(row)))

        if table_name.startswith("subsidiary"):
            if row["subno"] == "0":
                continue

        yield row


def generate_rows_objects(reader, table_name, table_def):
    column_names = list(table_def["columns"].keys())
    objects = defaultdict(list)

    for row in reader:
        if table_name in SUBSID_TABLES:
            if row[1] != "0":
                continue
            pk = (row[0],)

        if table_name.startswith("subsidiary"):
            if row[1] == "0":
                continue
            pk = (row[0], row[1])

        row = list(clean_row(row))
        seqno = str(int(row[2]) + 1).zfill(4)
        if row[3]:
            if row[3].endswith(seqno):
                row[3] = row[3][:-4]
            objects[pk].append(row[3])

    for pk, objects in tqdm.tqdm(objects.items()):
        yield dict(zip(column_names, list(pk + ("".join(objects),))))


def generate_rows_registration(reader, table_name, table_def):
    column_names = list(table_def["columns"].keys())
    registration = defaultdict(list)

    for row in reader:
        if table_name in SUBSID_TABLES:
            if row[1] != "0":
                continue
            del row[1]

        row = dict(zip(column_names, clean_row(row)))
        pk = tuple(row[c] for c in table_def.get("primary_key", []))

        if table_name.startswith("subsidiary"):
            if row["subno"] == "0":
                continue

        registration[pk].append(row)

    for pk, reg_items in tqdm.tqdm(registration.items()):
        if len(reg_items) == 1:
            yield reg_items[0]
        row = reg_items[0]
        row['remdate'] = reg_items[-1]['remdate']
        row['remcode'] = reg_items[-1]['remcode']
        yield row


def ccew_to_zip(zip_location, output_db="ccew.db", recreate=True):
    db = Database(output_db, recreate=recreate)
    with ZipFile(zip_location) as z:
        for table, table_def in TABLES.items():
            f = "extract_{}.bcp".format(table.replace("subsidiary_", ""))

            print("Creating table: {}".format(table))

            db[table].create(
                table_def["columns"],
                pk=table_def.get("primary_key", []),
                foreign_keys=table_def.get("foreign_key", []),
            )

            for i in table_def.get("indexes", []):
                db[table].create_index(i)

            with z.open(f, "r") as bcpfile:
                bcpreader = bcp.reader(io.TextIOWrapper(bcpfile, encoding="latin1"))
                if "objects" in table:
                    row_generator = generate_rows_objects
                elif "registration" in table:
                    row_generator = generate_rows_registration
                else:
                    row_generator = generate_rows
                db[table].insert_all(
                    row_generator(tqdm.tqdm(bcpreader), table, table_def),
                    ignore=True,
                    batch_size=100000,
                )

            if table_def.get("fts"):
                db[table].enable_fts(table_def.get("fts"), create_triggers=True)

    for view_name, view_def in VIEWS.items():
        print("Inserting view: {}".format(view_name))
        db.create_view(view_name, view_def, replace=True)
