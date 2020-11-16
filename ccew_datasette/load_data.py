import io
from zipfile import ZipFile
from sqlite_utils import Database

import bcp
import tqdm

TABLES = {
    "aoo_ref": {
        "columns": {
            # (1) 	A (wide), B (LA), C (GLA/met county), D (country), E (continent)
            "aootype": str,
            # Values if aootype=A are: 1 England/Wales; 2 England; 3 Wales; 4 London.
            "aookey": int,
            "aooname": str,  # (50) 	name of an area of operation
            # (50) 	for searches, 'City of' removed from aooname
            "aoosort": str,
            "welsh": str,  # (1) 	Flag: Y or blank
            # may be blank. If aootype=D then holds continent; if aootype=B then holds GLA/met county
            "master": int,
        },
        "primary_key": ["aootype", "aookey"],
    },
    "class_ref": {
        "columns": {
            "classno": int,  # classification code
            "classtext": str,  # (60) 	description of a classification code
        },
        "primary_key": ["classno"],
    },
    "remove_ref": {
        "columns": {
            "code": str,  # (3) 	Register removal reason code
            "text": str,  # (25) 	Removal reason description
        },
        "primary_key": ["code"],
    },
    "charity": {
        "columns": {
            "regno": int,  # registered number of a charity
            "name": str,  # (150) 	main name of the charity
            "orgtype": str,  # (2) 	R (registered) or RM (removed)
            "gd": str,  # (250) 	Description of Governing Document
            "aob": str,  # (175) 	area of benefit - may not be defined
            # (1) 	area of benefit defined by Governing Document (T/F)
            "aob_defined": str,
            "nhs": str,  # (1) 	NHS charity (T/F)
            "ha_no": str,  # (20) 	Housing Association number
            "corr": str,  # (70) 	Charity correspondent name
            "add1": str,  # (35) 	address line of charity's correspondent
            "add2": str,  # (35) 	address line of charity's correspondent
            "add3": str,  # (35) 	address line of charity's correspondent
            "add4": str,  # (35) 	address line of charity's correspondent
            "add5": str,  # (35) 	address line of charity's correspondent
            "postcode": str,  # (8) 	postcode of charity's correspondent
            "phone": str,  # (23) 	telephone of charity's correspondent
            "fax": str,  # (23) 	fax of charity's correspondent
        },
        "primary_key": ["regno"],
        "fts": ["name"],
    },
    "subsidiary_charity": {
        "columns": {
            "regno": int,  # registered number of a charity
            # subsidiary number of a charity (may be 0 for main/group charity)
            "subno": int,
            "name": str,  # (150) 	main name of the charity
            "orgtype": str,  # (2) 	R (registered) or RM (removed)
            "gd": str,  # (250) 	Description of Governing Document
            "aob": str,  # (175) 	area of benefit - may not be defined
            # (1) 	area of benefit defined by Governing Document (T/F)
            "aob_defined": str,
            "nhs": str,  # (1) 	NHS charity (T/F)
            "ha_no": str,  # (20) 	Housing Association number
            "corr": str,  # (70) 	Charity correspondent name
            "add1": str,  # (35) 	address line of charity's correspondent
            "add2": str,  # (35) 	address line of charity's correspondent
            "add3": str,  # (35) 	address line of charity's correspondent
            "add4": str,  # (35) 	address line of charity's correspondent
            "add5": str,  # (35) 	address line of charity's correspondent
            "postcode": str,  # (8) 	postcode of charity's correspondent
            "phone": str,  # (23) 	telephone of charity's correspondent
            "fax": str,  # (23) 	fax of charity's correspondent
        },
        "primary_key": ["regno", "subno"],
        "foreign_key": [("regno", "charity", "regno")],
        "fts": ["name"],
    },
    "acct_submit": {
        "columns": {
            "regno": int,  # registered number of a charity
            "submit_date": str,  # date submitted
            "arno": str,  # (4) 	annual return mailing cycle code
            # (4) 	Charity's financial year end date (may be blank)
            "fyend": str,
        },
        "foreign_key": [("regno", "charity", "regno")],
    },
    "ar_submit": {
        "columns": {
            "regno": int,  # registered number of a charity
            "arno": str,  # (4) 	annual return mailing cycle code
            "submit_date": str,  # date submitted
        },
        "foreign_key": [("regno", "charity", "regno")],
    },
    "charity_aoo": {
        "columns": {
            "regno": int,  # registered number of a charity
            "aootype": str,  # (1) 	A B or D
            "aookey": int,  # up to three digits
            "welsh": str,  # (1) 	Flag: Y or blank
            # may be blank. If aootype=D then holds continent; if aootype=B then holds GLA/met county
            "master": int,
        },
        "foreign_key": [
            ("regno", "charity", "regno"),
            # (("aootype", "aookey"), "aoo_ref", ("aootype", "aookey")),
        ],
    },
    "class": {
        "columns": {
            "regno": int,  # registered number of a charity
            # classification code for a charity (multiple occurrences possible)
            "class": int,
        },
        "foreign_key": [
            ("regno", "charity", "regno"),
            ("class", "class_ref", "classno"),
        ],
    },
    "financial": {
        "columns": {
            "regno": int,  # registered number of a charity
            "fystart": str,  # Charity's financial year start date
            "fyend": str,  # Charity's financial year end date
            "income": int,
            "expend": int,
        },
        "foreign_key": [("regno", "charity", "regno")],
    },
    "main_charity": {
        "columns": {
            "regno": int,  # registered number of a charity
            "coyno": str,  # company registration number
            "trustees": str,  # (1) 	trustees incorporated (T/F)
            "fyend": str,  # (4) 	Financial year end
            # (1) 	requires correspondence in both Welsh & English (T/F)
            "welsh": str,
            # date for latest gross income (blank if income is an estimate)
            "incomedate": str,
            "income": int,
            "grouptype": str,  # (4) 	may be blank
            "email": str,  # (255) 	email address
            "web": str,  # (255) 	website address
        },
        "primary_key": ["regno"],
        "foreign_key": [("regno", "charity", "regno")],
    },
    "name": {
        "columns": {
            "regno": int,  # registered number of a charity
            "nameno": int,  # number identifying a charity name
            # (150) 	name of a charity (multiple occurrences possible)
            "name": str,
        },
        "primary_key": ["regno", "nameno"],
        "foreign_key": [("regno", "charity", "regno")],
        "fts": ["name"],
    },
    "subsidiary_name": {
        "columns": {
            "regno": int,  # registered number of a charity
            # subsidiary number of a charity (may be 0 for main/group charity)
            "subno": int,
            "nameno": int,  # number identifying a charity name
            # (150) 	name of a charity (multiple occurrences possible)
            "name": str,
        },
        "primary_key": ["regno", "subno", "nameno"],
        "foreign_key": [
            # (("regno", "subno"), "subsidiary_charity", ("regno", "subno"))
        ],
        "fts": ["name"],
    },
    "objects": {
        "columns": {
            "regno": int,  # registered number of a charity
            "object": str,  # (255) 	Description of objects of a charity
        },
        "primary_key": ["regno"],
        "foreign_key": [("regno", "charity", "regno")],
        "fts": ["object"],
    },
    "subsidiary_objects": {
        "columns": {
            "regno": int,  # registered number of a charity
            # subsidiary number of a charity (may be 0 for main/group charity)
            "subno": int,
            "object": str,  # (255) 	Description of objects of a charity
        },
        "primary_key": ["regno", "subno"],
        "foreign_key": [
            # (("regno", "subno"), "subsidiary_charity", ("regno", "subno"))
        ],
        "fts": ["object"],
    },
    "partb": {
        "columns": {
            "regno": str,  # (14) CHARACTER SET latin1 NOT NULL DEFAULT '0',
            "artype": str,  # (4) CHARACTER SET latin1 NOT NULL DEFAULT '',
            "fystart": str,  # DEFAULT NULL,
            "fyend": str,  # NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "inc_leg": int,  # (20) DEFAULT NULL,
            "inc_end": int,  # (20) DEFAULT NULL,
            "inc_vol": int,  # (20) DEFAULT NULL,
            "inc_fr": int,  # (20) DEFAULT NULL,
            "inc_char": int,  # (20) DEFAULT NULL,
            "inc_invest": int,  # (20) DEFAULT NULL,
            "inc_other": int,  # (20) DEFAULT NULL,
            "inc_total": int,  # (20) DEFAULT NULL,
            "invest_gain": int,  # (20) DEFAULT NULL,
            "asset_gain": int,  # (20) DEFAULT NULL,
            "pension_gain": int,  # (20) DEFAULT NULL,
            "exp_vol": int,  # (20) DEFAULT NULL,
            "exp_trade": int,  # (20) DEFAULT NULL,
            "exp_invest": int,  # (20) DEFAULT NULL,
            "exp_grant": int,  # (20) DEFAULT NULL,
            "exp_charble": int,  # (20) DEFAULT NULL,
            "exp_gov": int,  # (20) DEFAULT NULL,
            "exp_other": int,  # (20) DEFAULT NULL,
            "exp_total": int,  # (20) DEFAULT NULL,
            "exp_support": int,  # (20) DEFAULT NULL,
            "exp_dep": int,  # (20) DEFAULT NULL,
            "reserves": int,  # (20) DEFAULT NULL,
            "asset_open": int,  # (20) DEFAULT NULL,
            "asset_close": int,  # (20) DEFAULT NULL,
            "fixed_assets": int,  # (20) DEFAULT NULL,
            "open_assets": int,  # (20) DEFAULT NULL,
            "invest_assets": int,  # (20) DEFAULT NULL,
            "cash_assets": int,  # (20) DEFAULT NULL,
            "current_assets": int,  # (20) DEFAULT NULL,
            "credit_1": int,  # (20) DEFAULT NULL,
            "credit_long": int,  # (20) DEFAULT NULL,
            "pension_assets": int,  # (20) DEFAULT NULL,
            "total_assets": int,  # (20) DEFAULT NULL,
            "funds_end": int,  # (20) DEFAULT NULL,
            "funds_restrict": int,  # (20) DEFAULT NULL,
            "funds_unrestrict": int,  # (20) DEFAULT NULL,
            "funds_total": int,  # (20) DEFAULT NULL,
            "employees": int,  # (11) DEFAULT NULL,
            "volunteers": int,  # (11) DEFAULT NULL,
            "cons_acc": str,  # (1) CHARACTER SET latin1 DEFAULT NULL,
            "charity_acc": str,  # (1) CHARACTER SET latin1 DEFAULT NULL,
        },
        "foreign_key": [("regno", "charity", "regno")],
    },
    "registration": {
        "columns": {
            "regno": int,  # registered number of a charity
            "regdate": str,  # date of registration for a charity
            "remdate": str,  # Removal date of a charity - Blank for Registered Charities
            "remcode": str,  # (3) 	Register removal reason code
        },
        "primary_key": ["regno", "regdate"],
        "foreign_key": [
            ("regno", "charity", "regno"),
            ("remcode", "remove_ref", "code"),
        ],
    },
    "subsidiary_registration": {
        "columns": {
            "regno": int,  # registered number of a charity
            # subsidiary number of a charity (may be 0 for main/group charity)
            "subno": int,
            "regdate": str,  # date of registration for a charity
            "remdate": str,  # Removal date of a charity - Blank for Registered Charities
            "remcode": str,  # (3) 	Register removal reason code
        },
        "primary_key": ["regno", "subno", "regdate"],
        "foreign_key": [
            # (("regno", "subno"), "subsidiary_charity", ("regno", "subno")),
            ("remcode", "remove_ref", "code")
        ],
    },
    "trustee": {
        "columns": {
            "regno": int,  # registered number of a charity
            "trustee": str,  # (255) 	Name of a charity trustee
        },
        "foreign_key": [("regno", "charity", "regno")],
        "fts": ["trustee"],
    },
}
SUBSID_TABLES = [
    t.replace("subsidiary_", "") for t in TABLES.keys() if t.startswith("subsidiary_")
]


def clean_row(row):
    for v in row:
        v = v.strip()
        if v == "":
            v = None
        yield v


def generate_rows(reader, table_name, table_def):
    pk_seen = set()
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
            # if pk in pk_seen and not "objects" in table_name:
            #     continue
            # pk_seen.add(pk)

        if "objects" in table_name:
            if last_pk and last_pk != pk:
                yield list(last_pk) + ["".join(objects)]
                objects = []
            last_pk = pk
            seqno = str(int(row["seqno"]) + 1).zfill(4)
            if row["object"]:
                if row["object"].endswith(seqno):
                    row["object"] = row["object"][:-4]
                objects.append(row["object"])

        else:
            yield row


def ccew_to_zip(zip_location, output_db="ccew.db"):
    db = Database(output_db, recreate=True)
    with ZipFile(zip_location) as z:
        for table, table_def in TABLES.items():
            f = "extract_{}.bcp".format(table.replace("subsidiary_", ""))

            db[table].create(
                table_def["columns"],
                pk=table_def.get("primary_key", []),
                foreign_keys=table_def.get("foreign_key", []),
            )

            print(table)
            print(table_def["columns"])

            with z.open(f, "r") as bcpfile:
                bcpreader = bcp.reader(io.TextIOWrapper(bcpfile, encoding="latin1"))
                db[table].insert_all(
                    generate_rows(tqdm.tqdm(bcpreader), table, table_def),
                    ignore=True,
                    batch_size=100000
                )

            if table_def.get("fts"):
                db[table].enable_fts(table_def.get("fts"), create_triggers=True)
