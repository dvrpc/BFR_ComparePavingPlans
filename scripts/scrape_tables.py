"""
scrape_packages.py
------------------
This script loops over every PDF within a given folder.
For each PDF, it extracts tabular data from every page
using pdftotext (which is a linux module)

"""
import subprocess
import os
from pathlib import Path
import re
import env_vars as ev
from env_vars import ENGINE


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
    return f + ".txt"


def pipe_to_postgres(filename: str):
    directory = "data/text"
    with open(directory + "/" + filename, "r") as file:
        tables = process_file(file)
        print(
            tables
        )  # a list of lists, with each item represeting one municipality's table


def process_file(file):
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
    return rows


def split_line(line: str) -> list:
    """
    Splits the line on places with 3 or more whitespace chars,
    returns a list representing one row
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
    Logic for reinserting blank/missing values
    since files are not csv or tab deliminated
    """
    if r is None:
        pass
    elif r[0] == "":
        return "need year"
    else:
        return r[0]


def main():
    directory = "data/PDFs"
    for filename in os.listdir(directory):
        file_and_ext = filename.split(".")
        if len(file_and_ext) > 2:
            raise Exception(
                f"File: {filename}\nExtra period(s) found in filename. Rename file."
            )
        else:
            filepath = f"data/PDFs/{filename}"
            pipe_to_postgres(create_txt(filename, filepath))

    # counties = ["Bucks", "Chester", "Delaware", "Montgomery", "Philadelphia"]

    # for county in counties:
    #     print(county)
    #     parse_all_pdfs(county)


if __name__ == "__main__":
    main()
