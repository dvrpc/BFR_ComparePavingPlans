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
import csv
import openpyxl


def create_txt(file_and_ext: list, filepath: str) -> str:
    """
    Uses subprocess module and pdftotext tool to turn pdf to text file.
    There are sometimes hidden sheets, so be sure to specify the sheet if xlsx.
    """

    filename = file_and_ext[0]  # filename, no extension
    ext = file_and_ext[1]  # extension, no period (eg 'xlsx')
    outpath = f"data/text/{filename}.txt"
    sheet_name = (
        "Sheet1"  # important as there may be hidden sheets in Penndot xlsx files
    )

    if ext == "pdf":
        subprocess.run(
            f"/usr/bin/pdftotext -layout -colspacing 2 -nopgbrk '{filepath}' '{outpath}'",
            shell=True,
            check=True,
        )
        flag = "pdf"
    elif ext == "xlsx":
        workbook = openpyxl.load_workbook(filepath, data_only=True)
        sheet = workbook[sheet_name]

        with open(outpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="|")
            for row in sheet.iter_rows(values_only=True):
                writer.writerow([cell if cell is not None else "" for cell in row])
        flag = "xlsx"
    else:
        print(file_and_ext[1])
        raise Exception("Unexpected filetype.")
    clean_textfile(filename)
    return (filename + ".txt", flag)


def clean_textfile(filename: str):
    """Strip problematic chars out of textfile"""
    directory = "data/text"
    with open(directory + "/" + filename + ".txt", "r") as file:
        content = file.read()
    replaced = content.replace("'", "").replace('"', "")
    with open(directory + "/" + filename + ".txt", "w") as file:
        file.write(replaced)


def pipe_to_postgres(file_and_flag: tuple, pg_tablename: str):
    """Open file, run cleanup and insert functions"""
    filename = file_and_flag[0]
    flag = file_and_flag[1]
    directory = "data/text"
    try:
        with open(directory + "/" + filename, "r") as file:
            print(f"importing {filename} to postgres...")
            table = process_file(file, filename, flag)
            handle_insertions(table, filename, pg_tablename)
    except FileNotFoundError:
        print(filename + " textfile not found, continuing")


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


def process_file(file, filename: str, flag: str):
    """Create PG table for file and turn rows in textfile to lists"""

    if flag == "pdf" or flag == "xlsx":
        rows = []
        year = 0
        for line in file:
            row = split_line(line, flag)
            if row:
                a = current_year(row)
                if a == "need year":
                    row[1] = year
                else:
                    row[1] = a
                    year = a
                if flag == "pdf":
                    handle_municipalities(row, flag)
                else:
                    row.pop(0)  # remove empty first col, 'a' in excel
                    row = row[
                        :19
                    ]  # remove any cols beyond the last, as there are some empties
                    handle_municipalities(row, flag)
                if row[0] == 0:
                    pass
                else:
                    rows.append(row)
                    print(row)
        return rows
    else:
        raise Exception("flag set for unplanned filetype")


def split_line(line: str, flag: str) -> list:
    """
    Splits the line on places with 3 or more whitespace chars,
    returns a list representing one row

    Returns none for unneeded rows
    """

    def contains_keyword(lst, keyword: str):
        """
        Checks if any item in the list contains the specified keyword.
        """
        return any(keyword in item for item in lst)

    if flag == "pdf":
        pattern = r"\s{3,}"
        r = re.split(pattern, line.strip())
        if line.startswith(" ") and r:
            r.insert(0, "")
    elif flag == "xlsx":
        pattern = r"\|"
        r = re.split(pattern, line.strip())

    if contains_keyword(r, "LOCATION FROM"):
        return None
    elif contains_keyword(r, "Loc Road Name RM"):
        return None
    elif contains_keyword(r, "Result"):
        return None
    elif contains_keyword(r, "Calendar year"):
        return None
    elif contains_keyword(r, "ANTICIPATED"):
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
    elif r[1] == "":
        return "need year"
    else:
        return r[1]


def handle_municipalities(row: list, flag: str):
    """Fills in blank strings to ensure all rows are same length"""
    if flag == "pdf":
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
    elif flag == "xlsx":
        if row[-8] == "PHILADELPHIA":
            row.insert(-7, "")
            row.insert(-7, "")
        else:
            pass


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
            "Leg Dist 1" varchar,
            "Leg Dist 2" varchar,
            "Leg Dist 3" varchar,
            "Senatorial Dist1" varchar,
            "Senatorial Dist2" varchar,
            "Cong District" varchar,
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
            pipe_to_postgres(create_txt(file_and_ext, filepath), pg_tablename)


if __name__ == "__main__":
    main()
