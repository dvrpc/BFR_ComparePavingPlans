"""
map_plan.py
------------------
This script maps the segments on the 5-year
repaving plan for each county in District 6.

"""

from env_vars import POSTGRES_URL, ENGINE, DATA_ROOT
from sqlalchemy import text
import requests
import os


def read_in_penndot_rms():
    print("Gathering PennDOT data from PennDOT rest service...")
    url = "https://gis.penndot.pa.gov/gis/rest/services/opendata/roadwaysegments/MapServer/3/query?where=DISTRICT_NO%20%3D%20'06'&outFields=*&outSR=4326&f=geojson"
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception(
            "Check the PennDOT RMS URL, might be broken. Non-200 status code."
        )

    engine = ENGINE
    with engine.connect() as connection:
        connection.execute(text("create extension if not exists postgis;"))

    print("Enabling PostGIS... Importing PennDOT RMS data into Postgres DB..")

    os.system(f'ogr2ogr -f "PostgreSQL" "{POSTGRES_URL}" "{url}" -nln rms -overwrite')


def export_geoms():
    sql_query = """

        SELECT
      "Calendar year",
      to_char(cast("State Route" as numeric), $$fm0000$$) as sr,
      "Loc Road Name RMS",
      "Intersection From",
      to_char(cast("Segment From" as numeric), $$fm0000$$) as sf,
      "Offset From",
      "Intersection To",
      to_char(cast("Segment To" as numeric), $$fm0000$$) as st,
      "Offset to",
      "Municipality Name1",
      "Miles Planned",
      "County",
      r.seg_no,
      r.wkb_geometry as geom
    FROM "DistrictPlan" a
    LEFT JOIN rms r ON
      r.st_rt_no = to_char(cast("State Route" as numeric), $$fm0000$$) AND
      r.cty_code = (
        CASE
          WHEN a."County" = $$Philadelphia$$ THEN $$67$$
          WHEN a."County" = $$Bucks$$ THEN $$09$$
          WHEN a."County" = $$Delaware$$ THEN $$23$$
          WHEN a."County" = $$Chester$$ THEN $$15$$
          WHEN a."County" = $$Montgomery$$ THEN $$46$$
          ELSE $$00$$
        END
      )
    WHERE
      r.seg_no >= to_char(cast("Segment From" as numeric), $$fm0000$$) AND
      r.seg_no <= to_char(cast("Segment To" as numeric), $$fm0000$$) AND
      "Calendar year" IS NOT NULL

        """
    ogr2ogr_cmd = f"ogr2ogr -f 'GeoJSON' '{DATA_ROOT}geojson/mapped_plan.geojson' '{POSTGRES_URL}' -sql '{sql_query}' "
    os.system(ogr2ogr_cmd)
    print(f"created geojson, storing at: {DATA_ROOT}geojson")


def main():
    read_in_penndot_rms()
    export_geoms()


if __name__ == "__main__":
    main()
