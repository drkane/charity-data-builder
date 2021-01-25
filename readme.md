# Charity Data Builder

A set of utilities to create SQlite databases with data from the Charity Commission for England and Wales (CCEW) and the Scottish Charity Regulator (OSCR).

These databases can then be served using [datasette](https://datasette.io/).

## Install the library

I recommend using a virtual environment * [pip tools](https://github.com/jazzband/pip-tools)

```sh
# create a virtual environment called venv
python -m venv env

# then enter the virtual environment
# on windows:
env\Scripts\activate
# on unix/mac:
source env/bin/activate

python -m pip install pip-tools
pip-sync
```

## Commands to populate the data

You need to get a link to the latest Charity Commission data file from <https://register-of-charities.charitycommission.gov.uk/register/full-register-download>.

```sh
# CCEW - all charities
python load_ccew https://register-of-charities.charitycommission.gov.uk/documents/34602/417919/Main+Monthly+Extract+zip+file.zip/881761e4-c0c5-aa0e-376e-a3070124041a?t=1604602947324

# OSCR - active charities
python load_oscr https://www.oscr.org.uk/umbraco/Surface/FormsSurface/CharityRegDownload
# OSCR - new 
python load_oscr --no-recreate https://www.oscr.org.uk/umbraco/Surface/FormsSurface/CharityFormerRegDownload
```

By default the databases are saved to the `data/` directory.

## Serve as a datasette instance

```sh
datasette data/
```

The `data/metadata.json` file defines some useful views and queries 
within the database.
