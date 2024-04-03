from env_vars import ENGINE, POSTGRES_URL
from sqlalchemy import text
import os
import psycopg2
import csv

last_plan = "5-year plan 2022-2026"
current_plan = "5-year plan 2024-2028"
views = [
    "new_segments",
    "no_change",
    "year_change",
    "length_change",
    "year_length_change",
]

district_update = """
    update district_plan
"""

VIEWS = f"""
-- new segments
create or replace view {views[0]} as
select a.gisid, b.*, '{current_plan}' as source  from oracle_copy a
right join "DistrictPlan" b
on a.shortcode = b.shortcode 
where gisid is null;


--No Change: If both shortcode and longcode match, and CALENDAR_YEAR is the same.
create or replace view {views[1]} as
select a.gisid, b.*, '{current_plan}' as source from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode
where a."source" = '{last_plan}'
and a.shortcode = b.shortcode
and a.longcode = b.longcode
and a.calendar_year = b."Calendar year"::numeric;

--Year Change Only: If shortcode and longcode match, but CALENDAR_YEAR differs.
create or replace view {views[2]} as
select a.gisid, b.*, '{current_plan}' as source  from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode
where a."source" = '{last_plan}'
and a.shortcode = b.shortcode
and a.longcode = b.longcode
and a.calendar_year != b."Calendar year"::numeric;

--Length Change Only: If shortcode matches, longcode differs, but CALENDAR_YEAR is the same.
create or replace view {views[3]} as
select a.gisid, b.*, '{current_plan}' as source  from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode
where a."source" = '{last_plan}'
and a.shortcode = b.shortcode
and a.longcode != b.longcode
and a.calendar_year = b."Calendar year"::numeric;


--New Year and Length: If shortcode matches, but both longcode and CALENDAR_YEAR differ.
create or replace view {views[4]} as
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


def create_csvs(views, database_uri):
    directory = os.getcwd() + "/data/to_change"
    print(os.getcwd())

    with psycopg2.connect(database_uri) as conn:
        cursor = conn.cursor()

        for view in views:
            file_path = f"{directory}/{view}.csv"
            print(f"Exporting {view} to {file_path}")

            cursor.execute(f"SELECT * FROM {view}")
            columns = [desc[0] for desc in cursor.description]

            text_columns = [
                "State Route",
                "Segment From",
                "Segment To",
                "Offset From",
                "Offset to",
                "cty_code",
            ]

            with open(file_path, "w", newline="") as csvfile:
                csvwriter = csv.writer(
                    csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
                )
                csvwriter.writerow(columns)

                for row in cursor:
                    formatted_row = [
                        f'"{value}"'
                        if columns[i] in text_columns and isinstance(value, str)
                        else value
                        for i, value in enumerate(row)
                    ]
                    csvwriter.writerow(formatted_row)

            print(f"{view} exported successfully.")


if __name__ == "__main__":
    update_columns()
    create_csvs(views, POSTGRES_URL)
