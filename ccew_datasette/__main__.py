import argparse
import tempfile

import requests
from tqdm import tqdm

from load_data import ccew_to_zip


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Turn Charity Commission data file into an SQLite DB')
    parser.add_argument('ccew', help='ZIP file with Charity Commission data')
    parser.add_argument('--ccew-out', help='Location of the output database', default="data\ccew.db")
    parser.add_argument('--recreate', help="Recreate the database from scratch", dest='recreate', action='store_true')
    parser.add_argument('--no-recreate', dest='recreate', action='store_false')
    parser.set_defaults(recreate=True)
    args = parser.parse_args()

    if args.ccew.startswith("http"):
        _, tmp_zip_location = tempfile.mkstemp()
        response = requests.get(args.ccew, stream=True)
        total_size_in_bytes= int(response.headers.get('content-length', 0))
        block_size = 1024 #1 Kibibyte
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
        with open(tmp_zip_location, 'wb') as tmp_zip_file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                tmp_zip_file.write(data)
        progress_bar.close()
        ccew_to_zip(tmp_zip_location, args.ccew_out, recreate=args.recreate)
    else:
        ccew_to_zip(args.ccew, args.ccew_out, recreate=args.recreate)
