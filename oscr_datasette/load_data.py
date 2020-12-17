import io
import csv
from zipfile import ZipFile
from sqlite_utils import Database

import tqdm

from views import VIEWS

NUMBER_FIELDS = (
    "Most recent year income",
    "Most recent year expenditure",
    "Donations and legacies income",
    "Charitable activities income",
    "Other trading activities income",
    "Investments income",
    "Other income",
    "Raising funds spending",
    "Charitable activities spending",
    "Other spending",
)


def clean_row(row):
    for f in ("Reason For Removal", "Ceased Date"):
        if f not in row:
            row[f] = None
    row["active"] = row.get("Ceased Date") is None
    for k, v in row.items():
        if isinstance(v, str):
            v = v.strip()
            if v == "":
                v = None
        if k in ("Purposes", "Beneficiaries", "Activities"):
            if v:
                t = list(csv.reader([v], quotechar="'"))
                if t:
                    v = t[0]
                else:
                    v = None
        if k in NUMBER_FIELDS and v:
            v = int(v)
        yield k.strip(), v


def oscr_to_zip(zip_location, output_db="oscr.db", recreate=True):
    db = Database(output_db, recreate=recreate)
    with ZipFile(zip_location) as z:
        for f in z.filelist:
            if not f.filename.endswith(".csv"):
                continue
            with z.open(f) as csvfile:
                reader = csv.DictReader(io.TextIOWrapper(csvfile, encoding="utf8"))

                db["oscr"].insert_all(
                    tqdm.tqdm(dict(clean_row(row)) for row in reader),
                    ignore=True,
                    batch_size=100000,
                    pk="Charity Number",
                )
                db["oscr"].enable_fts(
                    ["Charity Name", "Objectives"], replace=True, create_triggers=True
                )
                for i in [
                    ["Charity Status"],
                    ["Constitutional Form"],
                    ["Geographical Spread"],
                    ["Main Operating Location"],
                ]:
                    db["oscr"].create_index(i, if_not_exists=True)

    for view_name, view_def in VIEWS.items():
        print("Inserting view: {}".format(view_name))
        db.create_view(view_name, view_def, replace=True)
