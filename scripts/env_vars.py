from dotenv import dotenv_values
from sqlalchemy import create_engine

config = dotenv_values(".env")
DATA_ROOT = config["DATA_ROOT"]

# postgresql
POSTGRES_URL = config["PG_URL"]
ENGINE = create_engine(POSTGRES_URL)
# oracle
ORA_UN = config["ORA_UN"]
ORA_PW = config["ORA_PW"]
LINUX_TNS = config["LINUX_TNS"]
WINDOWS_TNS = config["WINDOWS_TNS"]
DNS = config["DNS"]
