# BFR_ComparePavingPlans
Compare 5-year resurfacing plans and track changes

## Setup
### Dependencies
To scrape PDFs, you need poppler-utils which is only on Linux or Windows Subsystem for Linux (WSL). 
The other tools are available on other architecture. 

PennDOT's most recent five year plan included Excel files, so the ability to scrape pdfs 
may no longer be necessary. The code has been modularized to handle both PDFs and Excel.

The server, or your Linux machine (WSL for windows works) needs to have:
- Python 3
- poppler-utils
- postgresql
- postgis
- and gdal (specifically ogr2ogr)


You need a .env file with these variables set. 

```
ORA_UN=
ORA_PW=
LINUX_TNS=
WINDOWS_TNS=
DATA_ROOT=/mnt/g/Shared drives/Complete Streets Resurfacing Program/5-year Plans/2024-2028/
DNS=
PG_URL=postgresql://user:pw@host:port/db
PG_UN=
PG_PW=
PG_DB=
PG_HOST=
PG_PORT=
```

Create an empty virtual environment at the root of the project.

```shell
python -m venv ve
```

Activate it. For Linux/Mac:
```shell
source ve/bin/activate
```

For Windows CMD:
```shell
ve\Scripts\activate.bat
```

Install the requirements:
```shell
pip install -r requirements.txt
```

## Run Order

### Map new paving plan:

1. Import latest database records from Oracle DB using `import_oracle.py`
2. Pull data from new 5-year plan PDFs using `scrape_tables.py`
3. Map new plan segments using `map_plan.py`

### Compare new plan to database records
Assuming `import_oracle.py` and `scrape_tables.py` are run:

4. Modify tables for easy comparison using `prepare_tables_for_comparison.py`
5. Identify changes status of screened segments using `changes_in_screened_segments.py`


### todo 
- [ ] bundle this with rocket fuel so updates are automatic
- [ ] current plan and last plan need to be env vars and updated in scripts
- [ ] bundle this with rocket fuel so updates are automatic
