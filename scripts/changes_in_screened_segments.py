from env_vars import ENGINE
from sqlalchemy import text
import os

last_plan = "5-year plan 2022-2026"
current_plan = "5-year plan 2024-2028"

VIEWS = f"""
-- new segments
create or replace view newsegments as
select a.gisid, b.*, '{current_plan}' as source  from oracle_copy a
right join "DistrictPlan" b
on a.shortcode = b.shortcode 
where gisid is null;


--No Change: If both shortcode and longcode match, and CALENDAR_YEAR is the same.
create or replace view nochange as
select a.gisid, b.*, '{current_plan}' as source from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode
where a."source" = '{last_plan}'
and a.shortcode = b.shortcode
and a.longcode = b.longcode
and a.calendar_year = b."Calendar year"::numeric;

--Year Change Only: If shortcode and longcode match, but CALENDAR_YEAR differs.
create or replace view diff_year as
select a.gisid, b.*, '{current_plan}' as source  from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode
where a."source" = '{last_plan}'
and a.shortcode = b.shortcode
and a.longcode = b.longcode
and a.calendar_year != b."Calendar year"::numeric;

--Length Change Only: If shortcode matches, longcode differs, but CALENDAR_YEAR is the same.
create or replace view diff_geom_same_year as
select a.gisid, b.*, '{current_plan}' as source  from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode
where a."source" = '{last_plan}'
and a.shortcode = b.shortcode
and a.longcode != b.longcode
and a.calendar_year = b."Calendar year"::numeric;


--New Year and Length: If shortcode matches, but both longcode and CALENDAR_YEAR differ.
create or replace view diff_geom_diff_year as
select a.gisid, b.*, '{current_plan}' as source  from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode
where a."source" = '{last_plan}'
and a.shortcode = b.shortcode
and a.longcode != b.longcode
and a.calendar_year != b."Calendar year"::numeric;

commit;
"""


def update_columns():
    """
    update all short and log codes
    """
    try:
        con = ENGINE.connect()
        con.execute(text(VIEWS))
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        con.close()


def create_csvs():
    """Export views to csv
    Important: user must have access to the folder.
    In linux, try chmod 777 ./data/to_change
    because postgres user (or whatever user in your .env)
    needs write access to dump out these csvs.
    """

    views = [
        "diff_geom_diff_year",
        "diff_geom_same_year",
        "diff_year",
        "newsegments",
        "nochange",
    ]

    directory = os.getcwd() + "/data/to_change"
    print(os.getcwd())

    for view in views:
        command = f"COPY (select * from {view}) TO '{directory}/{view}.csv' DELIMITER ',' CSV HEADER;"
        try:
            con = ENGINE.connect()
            print(command)
            con.execute(text(command))
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            con.close()


if __name__ == "__main__":
    update_columns()
    create_csvs()
