"""
compare_plans.py
------------------
This script...

"""
import geopandas as gpd
import pandas as pd
import env_vars as ev
from env_vars import ENGINE


format_tables_for_join = """
    with oldplan as(
        select 
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
            "CNT_CODE" ,
            CONCAT("STATE_ROUTE","SEGMENT_FROM", "SEGMENT_TO") as shortcode, 
            CONCAT("STATE_ROUTE","SEGMENT_FROM", to_char(cast("OFFSET_FROM" as numeric), 'fm0000'), "SEGMENT_TO", to_char(cast("OFFSET_TO" as numeric), 'fm0000')) as longcode
        from from_oracle 
    ), 
    newplan as (
        select 
            *,
            "Year" as yr,
            CONCAT("sr","sf", "st") AS shortcode,
            trim(trailing '.0' from CONCAT("sr",cast("sf" as text), cast("OffsetFrom" as text), cast("st" as text), to_char(cast("OffsetTo" as numeric), 'fm0000'))) as longcode
        from mapped_plan
    )
    """

# find base length of new plan for comparison purposes
new_plan = pd.read_sql(
    """
    SELECT *
    FROM mapped_plan;
    """,
    con=ENGINE,
)

# these records are the same in the new plan as they are in the database
nochange = pd.read_sql(
    fr"""
    {format_tables_for_join}
    SELECT 
        a.*,
        b.yr as new_year,
        b."Miles Planned",
        b.shortcode,
        b.longcode
    FROM oldplan a
    INNER JOIN newplan b
    ON a.longcode = b.longcode
    WHERE a."CALENDAR_YEAR" = b.yr
    AND a."PLANNED" = b."Miles Planned"
    ORDER BY a."CALENDAR_YEAR", a."CNT_CODE"
    """,
    con=ENGINE,
)

# these records cover the same segments, but have different offsets, thus different planned miles
length_change = pd.read_sql(
    fr"""
    {format_tables_for_join}
    SELECT 
        a.*,
        b.yr as new_year,
        b."Miles Planned",
        b.shortcode,
        b.longcode
    FROM oldplan a
    INNER JOIN newplan b
    ON a.shortcode = b.shortcode
    WHERE a.longcode <> b.longcode
    ORDER BY a."CALENDAR_YEAR", a."CNT_CODE"
    """,
    con=ENGINE,
)

# these records have changed years but, the extents are the same
year_change = pd.read_sql(
    fr"""
    {format_tables_for_join}
    SELECT 
        a.*,
        b.yr as new_year,
        b."Miles Planned",
        b.shortcode,
        b.longcode
    FROM oldplan a
    INNER JOIN newplan b
    ON a.longcode = b.longcode
    WHERE a."CALENDAR_YEAR" <> b.yr
    ORDER BY a."CALENDAR_YEAR", a."CNT_CODE"
    """,
    con=ENGINE,
)

# these records are in the database but are not in the new plan
removed_from_plan = pd.read_sql(
    fr"""
    {format_tables_for_join}
    SELECT 
        a.*
    FROM oldplan a
    WHERE NOT EXISTS(
        SELECT *
        FROM newplan b
        WHERE a.shortcode = b.shortcode
        )
    AND a."CALENDAR_YEAR" NOT IN ('2018', '2019', '2020', '2021')
    ORDER BY a."CALENDAR_YEAR", a."CNT_CODE"
    """,
    con=ENGINE,
)


new_to_plan = pd.read_sql(
    fr"""
    {format_tables_for_join}
    SELECT 
        b.*
    FROM newplan b
    WHERE NOT EXISTS(
        SELECT a.*
        FROM oldplan a
        WHERE a.shortcode = b.shortcode
        )
    ORDER BY b.yr, b.co_no
    """,
    con=ENGINE,
)

minor_length_change = pd.read_sql(
    fr"""
    {format_tables_for_join}
    SELECT 
        a.*,
        b.yr as new_year,
        b."Miles Planned",
        b.shortcode,
        b.longcode
    FROM oldplan a
    INNER JOIN newplan b
    ON a.shortcode = b.shortcode
    WHERE a."CALENDAR_YEAR" = b.yr
    AND a."PLANNED" = b."Miles Planned"
    ORDER BY a."CALENDAR_YEAR", a."CNT_CODE"
    """,
    con=ENGINE,
)


print("Records in new plan: ", len(new_plan))
print("No change: ", len(nochange))
print("Length change: ", len(length_change))
print("Year change: ", len(year_change))
print("Removed from plan: ", len(removed_from_plan))
print("New to plan: ", len(new_to_plan))
print("Minor length change: ", len(minor_length_change))

# now that these are all in here, cut phila via dataframe and see what needs to be done with each

new_plan_by_year = new_plan.groupby(["Year", "co_no"])["Year"].count()
new_record_by_year = new_to_plan.groupby(["yr", "co_no"])["yr"].count()
to_blend = [new_plan_by_year, new_record_by_year]
compare_years = pd.concat(to_blend, ignore_index=True, axis=1)
print(compare_years)


l = [
    len(nochange),
    len(length_change),
    len(year_change),
    len(new_to_plan),
    len(minor_length_change),
]
# print(sum(l))
