"""
compare_plans.py
------------------
This script comapres the new 5 year plan to
the most recent plan from the oracle database.

When a new plan is received, update "SOURCE" on line 38

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


def main():
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
    new_segments.to_csv(
        "D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/new_segments.csv",
        index=False,
    )
    length_change.to_csv(
        "D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/length_change.csv",
        index=False,
    )
    year_change.to_csv(
        "D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/year_change.csv",
        index=False,
    )
    no_change.to_csv(
        "D:/dvrpc_shared/BFR_ComparePavingPlans/data/to_change/no_change.csv",
        index=False,
    )


if __name__ == "__main__":
    main()
