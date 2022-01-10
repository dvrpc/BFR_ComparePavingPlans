"""
changes_in_screened_segments.py
------------------
This script comapres the new 5 year plan to
the most recent plan from the oracle database,
focusing on the already screened segments
(admin selected and active). 

This collection of functions and loops is designed to 
be run manually in a notebook for detailed troubelshooting.

"""

import pandas as pd
import env_vars as ev
from env_vars import ENGINE


def read_oracle():
    # select only records from latest 5 year plan, assuming other changes were resolved
    from_oracle = pd.read_sql(
        """
        SELECT 
            cast("CALENDAR_YEAR" as text) as "CALENDAR_YEAR" ,
            to_char(CAST("STATE_ROUTE" AS numeric), 'fm0000') AS sr,
            "LOC_ROAD_NAME_RMS" ,
            "INTERSECTION_FROM" ,
            "SEGMENT_FROM" ,
            to_char(cast("OFFSET_FROM" as numeric), 'fm0000') as offsetfrom ,
            "INTERSECTION_TO" ,
            "SEGMENT_TO" ,
            "OFFSET_TO" ,
            "MUNICIPALITY_NAME1" ,
            "MUNICIPALITY_NAME2" ,
            "MUNICIPALITY_NAME3" ,
            cast("PLANNED" as text) as "PLANNED" ,
            cast("CNT_CODE" as numeric) as "CNT_CODE",
            "SHORTCODE" as shortcode, 
            "LONGCODE" as longcode
        FROM from_oracle
        WHERE "SOURCE" = '5-year plan 2020-2024'
        """,
        con=ENGINE,
    )

    # remove duplicate rows from oracle table
    from_oracle = from_oracle[~from_oracle.duplicated()]

    return from_oracle


def read_plans():

    district_plan = pd.read_sql(
        """
        SELECT *
        FROM "District_Plan";
        """,
        con=ENGINE,
    )

    district_plan = district_plan.drop("level_0", axis=1)
    district_plan = district_plan.drop("index", axis=1)

    return district_plan


def track_changes():
    district_plan = read_plans()
    from_oracle = read_oracle()

    new_seg_counter = 0
    no_change_counter = 0
    year_change_counter = 0
    length_change_counter = 0
    new_year_and_length_counter = 0

    new_segments = []
    length_change = []
    year_change = []
    no_change = []
    for i, row in district_plan.iterrows():
        county_subset = from_oracle[from_oracle["CNT_CODE"] == int(row["cty_code"])]
        if len(county_subset) > 0:
            # if shortcode matches anything in DB
            if county_subset["shortcode"].str.contains(row["shortcode"]).any():
                match_row = county_subset.loc[
                    county_subset["shortcode"] == row["shortcode"]
                ]
                if match_row["longcode"].item() == row["longcode"]:
                    if match_row["CALENDAR_YEAR"].item() == row["Year"]:
                        no_change_counter += 1
                        no_change.append(row)
                    else:
                        year_change_counter += 1
                        year_change.append(row)
                else:
                    if match_row["CALENDAR_YEAR"].item() == row["Year"]:
                        length_change_counter += 1
                        length_change.append(row)
                    else:
                        new_year_and_length_counter += 1
                        year_change_counter += 1
                        year_change.append(row)
                        length_change_counter += 1
                        length_change.append(row)
            else:
                new_seg_counter += 1
                new_segments.append(row)
        else:
            new_seg_counter += 1
            new_segments.append(row)

    # convert lists to data frames
    # update for everything: source, shortcode, longcode
    new_segments = pd.DataFrame(new_segments)  # append to DB
    length_change = pd.DataFrame(length_change)  # update offset fields
    year_change = pd.DataFrame(year_change)  # change year
    no_change = pd.DataFrame(no_change)

    # add source column to each DF
    new_segments["SOURCE"] = "5-year plan 2022-2026"
    length_change["SOURCE"] = "5-year plan 2022-2026"
    year_change["SOURCE"] = "5-year plan 2022-2026"
    no_change["SOURCE"] = "5-year plan 2022-2026"

    # write to csv
    # new_segments.to_csv("D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/new_segments.csv", index=False)
    # length_change.to_csv("D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/length_change.csv", index=False)
    # year_change.to_csv("D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/year_change.csv", index=False)
    # no_change.to_csv("D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/no_change.csv", index=False)
    return new_segments, length_change, year_change, no_change


new_segments, length_change, year_change, no_change = track_changes()


def grab_active_records():
    active_records = pd.read_sql(
        """
            SELECT 
                "GISID",
                cast("CALENDAR_YEAR" as text) as "CALENDAR_YEAR" ,
                to_char(CAST("STATE_ROUTE" AS numeric), 'fm0000') AS sr,
                "LOC_ROAD_NAME_RMS" ,
                "INTERSECTION_FROM" ,
                "SEGMENT_FROM" ,
                to_char(cast("OFFSET_FROM" as numeric), 'fm0000') as offsetfrom ,
                "INTERSECTION_TO" ,
                "SEGMENT_TO" ,
                "OFFSET_TO" ,
                "MUNICIPALITY_NAME1" ,
                "MUNICIPALITY_NAME2" ,
                "MUNICIPALITY_NAME3" ,
                cast("PLANNED" as text) as "PLANNED" ,
                cast("CNT_CODE" as numeric) as "CNT_CODE",
                "SHORTCODE" as shortcode, 
                "LONGCODE" as longcode
            FROM from_oracle
            where "ADMINSELECTED" = 'Y'
            and "ISACTIVE" = 'Yes'
            """,
        con=ENGINE,
    )

    # remove duplicate rows from oracle table
    active_records = active_records[~active_records.duplicated()]
    return active_records


def review_2022_projects(df):
    # review changes to active 2022 segments
    new_segments, length_change, year_change, no_change = track_changes()
    projects2022 = df[df["CALENDAR_YEAR"] == "2022"]

    length_change_2022 = []
    year_change_2022 = []
    no_change_2022 = []

    names = ["length_change", "year_change", "no_change"]
    tables = [length_change, year_change, no_change]
    append_to = [length_change_2022, year_change_2022, no_change_2022]
    for i in range(0, len(tables)):
        table = tables[i]
        name = names[i]
        li = append_to[i]
        counter = 0
        for i, row in projects2022.iterrows():
            if table["shortcode"].str.contains(row["shortcode"]).any():
                counter += 1
                match_row = table.loc[table["shortcode"] == row["shortcode"]]
                row["new_year"] = match_row["Year"].item()
                li.append(row)

        print(name, counter)

    length_change_2022 = pd.DataFrame(length_change_2022)
    year_change_2022 = pd.DataFrame(year_change_2022)
    no_change_2022 = pd.DataFrame(no_change_2022)

    year_change_2022.to_csv(
        "D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/2022_seg_yearchanges.csv",
        index=False,
    )


def review_2023_projects(df):
    # review changes to active 2023 segments
    new_segments, length_change, year_change, no_change = track_changes()
    projects2023 = df[df["CALENDAR_YEAR"] == "2023"]

    length_change_2023 = []
    year_change_2023 = []
    no_change_2023 = []

    names = ["length_change", "year_change", "no_change"]
    tables = [length_change, year_change, no_change]
    append_to = [length_change_2023, year_change_2023, no_change_2023]
    for i in range(0, len(tables)):
        table = tables[i]
        name = names[i]
        li = append_to[i]
        counter = 0
        for i, row in projects2023.iterrows():
            if table["shortcode"].str.contains(row["shortcode"]).any():
                counter += 1
                match_row = table.loc[table["shortcode"] == row["shortcode"]]
                row["new_year"] = match_row["Year"].item()
                li.append(row)

        print(name, counter)

    length_change_2023 = pd.DataFrame(length_change_2023)
    year_change_2023 = pd.DataFrame(year_change_2023)
    no_change_2023 = pd.DataFrame(no_change_2023)

    year_change_2023.to_csv(
        "D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/2023_seg_yearchanges.csv",
        index=False,
    )


def main():
    active_records = grab_active_records()
    review_2022_projects(active_records)
    review_2023_projects(active_records)
