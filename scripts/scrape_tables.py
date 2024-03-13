"""
scrape_packages.py
------------------
This script loops over every PDF within a given folder.
For each PDF, it extracts tabular data from every page
using pdftotext (which is a linux module)

"""
import subprocess
import os
import re
from env_vars import ENGINE
from sqlalchemy import text


def create_txt(filename: str, filepath: str) -> str:
    """
    Uses subprocess module and pdftotext tool to turn pdf to text file.
    """
    f = filename.strip(".pdf")
    subprocess.run(
        f"/usr/bin/pdftotext -layout -colspacing 2 -nopgbrk '{filepath}' 'data/text/{f}.txt'",
        shell=True,
        check=True,
    )
    clean_textfile(f)
    return f + ".txt"


def clean_textfile(filename: str):
    """Strip problematic chars out of textfile"""
    directory = "data/text"
    with open(directory + "/" + filename + ".txt", "r") as file:
        content = file.read()
    replaced = content.replace("'", "feet")
    with open(directory + "/" + filename + ".txt", "w") as file:
        file.write(replaced)


def pipe_to_postgres(filename: str, pg_tablename: str):
    """Open file, run cleanup and insert functions"""
    directory = "data/text"
    with open(directory + "/" + filename, "r") as file:
        print(f"importing {filename} to postgres...")
        table = process_file(file, filename)
        handle_insertions(table, filename, pg_tablename)


def handle_insertions(table: list, filename: str, pg_tablename: str):
    """Ingests a table representing one municipality and inserts into postgres"""
    name = filename.split(".")[0].replace("-", " to ")
    county = name.split()[0]
    engine = ENGINE
    with engine.connect() as connection:
        for row in table:
            row.append(county)
            row = tuple(row)

            sql = text(f'INSERT INTO "{pg_tablename}" VALUES {row};')

            try:
                connection.execute(sql)
            except Exception as e:
                print(f"Error inserting row: {row} - {e}")
                connection.rollback()
            connection.commit()


def process_file(file, filename):
    """Create PG table for file and turn rows in textfile to lists"""
    # remove .txt extension and dash
    rows = []
    year = 0
    for line in file:
        row = split_line(line)
        if row:
            a = current_year(row)
            if a == "need year":
                row[0] = year
            else:
                row[0] = a
                year = a
            rows.append(row)
            handle_municipalities(row)

    return rows


def split_line(line: str) -> list:
    """
    Splits the line on places with 3 or more whitespace chars,
    returns a list representing one row

    Returns none for unneeded rows
    """
    pattern = r"\s{3,}"
    r = re.split(pattern, line.strip())
    if line.startswith(" ") and r:
        r.insert(0, "")

    if r[0] == "LOCATION FROM":
        return None
    elif r[0] == "LOCATION FROM":
        return None
    elif r[0] == "LOCATION TO":
        return None
    elif r[0] == "Calendar year":
        return None
    elif r[0][0:4] == "Page":
        return None
    if len(r) < 5:
        return None
    else:
        return r


def current_year(r: list):
    """
    If list is blank, function indicates a year is needed, otherwise returns the year
    """
    if r is None:
        pass
    elif r[0] == "":
        return "need year"
    else:
        return r[0]


def handle_municipalities(row: list):
    """Fills in blank strings to ensure all rows are same length"""

    # Insert blanks for philly or places w/ only one/two munis
    if row[-2] == "PHILADELPHIA":
        row.insert(-1, "")
        row.insert(-1, "")
    # Rows where two munis are filled in
    elif len(row) == 12:
        row.insert(-1, "")
    # Rows where one muni is filled in
    elif len(row) == 11:
        row.insert(-1, "")
        row.insert(-1, "")
    # Rows with all three munis (do nothing)
    elif len(row) == 13:
        pass
    else:
        print(row, len(row))
        raise Exception("Unhandled row length")


def create_pg_table(tablename):
    """Creates Postgres table with columns"""
    engine = ENGINE
    with engine.connect() as connection:
        connection.execute(
            text(
                f"""
            drop table if exists public."{tablename}";
            create table public."{tablename}"(
            "Calendar year" char(4),
            "State Route" varchar,
            "Loc Road Name RMS" varchar,
            "Intersection From" varchar,
            "Segment From" varchar,
            "Offset From" varchar,
            "Intersection To" varchar,
            "Segment To" varchar,
            "Offset to" varchar,
            "Municipality Name1" varchar,
            "Municipality Name2" varchar,
            "Municipality Name3" varchar,
            "Miles Planned" numeric,
            "County" varchar);

        """
            )
        )
        connection.commit()


def main():
    directory = "data/PDFs"
    pg_tablename = "DistrictPlan"
    create_pg_table(pg_tablename)
    for filename in os.listdir(directory):
        file_and_ext = filename.split(".")
        if len(file_and_ext) > 2:
            raise Exception(
                f"File: {filename}\nExtra period(s) found in filename. Rename file."
            )
        else:
            filepath = f"data/PDFs/{filename}"
            pipe_to_postgres(create_txt(filename, filepath), pg_tablename)


if __name__ == "__main__":
    main()
