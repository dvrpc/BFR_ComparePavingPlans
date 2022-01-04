"""
scrape_packages.py
------------------
This script loops over every PDF within a given folder.
For each PDF, it extracts tabular data from every page

"""

from __future__ import annotations
from pathlib import Path
import tabula
import pandas as pd

pd.options.mode.chained_assignment = None
from PyPDF2 import PdfFileReader
import env_vars as ev
from env_vars import ENGINE


def get_number_of_pages_in_pdf(filepath):
    """
    Open a PDF file and return the number of pages within
    """
    with open(filepath, "rb") as pdf_file:
        pdf_reader = PdfFileReader(pdf_file)
        number_of_pages = pdf_reader.numPages

    return number_of_pages


def fill_empty_years(df):
    """
    fills empty year values and deletes yearly results rows
    """
    frames = []
    r = df.index[df["SR"] == "Result"]
    n = df.index[df["SR"].isnull()]
    c = df.index[df["Year"] == "Calendar year"]

    if len(r) == 0:
        if len(n) > 1:
            # if there is no total result row because they were left null, the nulls will be next to each other
            # drop the second one from the list
            if n[-1] - n[-2] == 1:
                n = n[:-1]
        r = n
        for i in range(0, len(r)):
            page_start = 0
            if i == 0:
                chunk = df.iloc[page_start : r[i]]
                # starting from after the misplaced heading name
                year = df.iloc[c[0] + 1]["Year"]
                chunk["Year"].fillna(year, inplace=True)
                frames.append(chunk)
            else:
                chunk = df.iloc[r[i - 1] + 1 : r[i]]
                year = df.iloc[r[i - 1] + 1]["Year"]
                chunk["Year"].fillna(year, inplace=True)
                frames.append(chunk)
    else:
        for i in range(0, len(r)):
            page_start = 0
            if i == 0:
                chunk = df.iloc[page_start : r[i]]
                # starting from after the misplaced heading name
                year = df.iloc[c[0] + 1]["Year"]
                chunk["Year"].fillna(year, inplace=True)
                frames.append(chunk)
            else:
                chunk = df.iloc[r[i - 1] + 1 : r[i]]
                year = df.iloc[r[i - 1] + 1]["Year"]
                chunk["Year"].fillna(year, inplace=True)
                frames.append(chunk)

    allyears = pd.concat(frames, ignore_index=True)

    return allyears


def drop_total_results_row(df):
    for idx, row in df.iterrows():
        if row["Year"] == "Result":
            df.drop(idx, inplace=True)
    return df


def parse_single_page(county, file, page_number):

    table = tabula.read_pdf(file, pages=page_number, pandas_options={"header": None})[0]
    df = pd.DataFrame(table)

    # drop extra/empty columns (empty except header row)
    cols_to_drop = []
    for i in range(0, len(df.columns)):
        if df[i].isnull()[1:].all():
            cols_to_drop.append(i)

    if len(cols_to_drop) > 0:
        df.drop(cols_to_drop, axis=1, inplace=True)

    # rename columns
    column_names = [
        "Year",
        "SR",
        "Road Name",
        "From",
        "SegmentFrom",
        "OffsetFrom",
        "To",
        "SegmentTo",
        "OffsetTo",
        "Municipality1",
        "Municipality2",
        "Municipality3",
        "Miles Planned",
    ]
    if county == "Philadelphia":
        phila_columns = column_names[:-3]
        phila_columns.append(column_names[-1])
        df.columns = phila_columns
    else:
        df.columns = column_names

    # fill in empty years
    df = fill_empty_years(df)

    # check number of rows header is read into (differs by file)
    if str(df["SR"][0]) == "nan":
        rows_to_drop = [0, 1]
        df.drop(rows_to_drop, inplace=True)

    else:
        rows_to_drop = [0]
        df.drop(rows_to_drop, inplace=True)

    # drop the last row with the plan total
    df = drop_total_results_row(df)

    return df


def parse_all_pdfs(county):
    file = fr"D:/dvrpc_shared/BFR_ComparePavingPlans/data/PDFs/{county} County Five Year Plan 2022-2026.pdf"
    frames = []
    num_pages = get_number_of_pages_in_pdf(file)
    for i in range(1, num_pages + 1):
        df = parse_single_page(county, file, page_number=i)
        frames.append(df)

    allpgs = pd.concat(frames, ignore_index=True)

    output_filepath = fr"D:/dvrpc_shared/BFR_ComparePavingPlans/data/CSVs/{county}_fiveyearplan_2022_2026.csv"

    allpgs.to_csv(output_filepath, index=False)
    allpgs.to_sql(f"{county}_County_Plan", ENGINE, if_exists="replace")


def main():
    counties = ["Bucks", "Chester", "Delaware", "Montgomery", "Philadelphia"]

    for county in counties:
        print(county)
        parse_all_pdfs(county)


if __name__ == "__main__":
    main()
