import argparse
import tempfile
import os

import requests
from tqdm import tqdm

from load_data import oscr_to_zip


def fetch_html(infile):
    _, tmp_zip_location = tempfile.mkstemp()
    response = requests.get(infile, stream=True)
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 Kibibyte
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
    with open(tmp_zip_location, "wb") as tmp_zip_file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            tmp_zip_file.write(data)
    progress_bar.close()
    return tmp_zip_location


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Turn OSCR data file into an SQLite DB"
    )
    parser.add_argument("oscr", nargs="+", help="ZIP file with OSCR data")
    parser.add_argument("--data-dir", help="Data directory", default="data")
    parser.add_argument(
        "--oscr-out",
        help="Location of the output database (within the data directory)",
        default="oscr.db",
    )
    parser.add_argument(
        "--recreate",
        help="Recreate the database from scratch",
        dest="recreate",
        action="store_true",
    )
    parser.add_argument("--no-recreate", dest="recreate", action="store_false")
    parser.set_defaults(recreate=True)
    args = parser.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    oscr_out = os.path.join(args.data_dir, args.oscr_out)

    recreate = args.recreate
    for oscr_in in args.oscr:
        print("Opening: {}".format(oscr_in))
        if oscr_in.startswith("http"):
            tmp_zip_location = fetch_html(oscr_in)
            oscr_to_zip(tmp_zip_location, oscr_out, recreate=recreate)
        else:
            oscr_to_zip(oscr_in, oscr_out, recreate=recreate)
        recreate = False
