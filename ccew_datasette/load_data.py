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
    pk_indexes = [column_names.index(pk) for pk in table_def.get("primary_key", [])]

    if "objects" in table_name:
        column_names.insert(-1, "seqno")
        objects = []
        last_pk = None

    for row in reader:
        if table_name in SUBSID_TABLES:
            if row[1] != "0":
                continue
            del row[1]

        row = dict(zip(column_names, clean_row(row)))

        if table_name.startswith("subsidiary"):
            if row["subno"] == "0":
                continue

        if pk_indexes:
            pk = tuple([row[c] for c in table_def.get("primary_key", [])])

        if "objects" in table_name:
            if last_pk and last_pk != pk:
                yield {
                    "object": "".join(objects),
                    **{
                        c: last_pk[i]
                        for i, c in enumerate(table_def.get("primary_key", []))
                    },
                }
                objects = []
            last_pk = pk
            seqno = str(int(row["seqno"]) + 1).zfill(4)
            if row["object"]:
                if row["object"].endswith(seqno):
                    row["object"] = row["object"][:-4]
                objects.append(row["object"])

        else:
            yield row


def ccew_to_zip(zip_location, output_db="ccew.db", recreate=True):
    db = Database(output_db, recreate=recreate)
    # with ZipFile(zip_location) as z:
    #     for table, table_def in TABLES.items():
    #         f = "extract_{}.bcp".format(table.replace("subsidiary_", ""))

    #         db[table].create(
    #             table_def["columns"],
    #             pk=table_def.get("primary_key", []),
    #             foreign_keys=table_def.get("foreign_key", []),
    #         )

    #         for i in table_def.get("indexes", []):
    #             db[table].create_index(i)

    #         print(db[table].schema)

    #         with z.open(f, "r") as bcpfile:
    #             bcpreader = bcp.reader(io.TextIOWrapper(bcpfile, encoding="latin1"))
    #             db[table].insert_all(
    #                 generate_rows(tqdm.tqdm(bcpreader), table, table_def),
    #                 ignore=True,
    #                 batch_size=100000,
    #             )

    #         if table_def.get("fts"):
    #             db[table].enable_fts(table_def.get("fts"), create_triggers=True)

    for view_name, view_def in VIEWS.items():
        db.create_view(view_name, view_def, replace=True)
