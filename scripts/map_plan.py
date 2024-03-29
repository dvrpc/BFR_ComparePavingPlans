"""
map_plan.py
------------------
This script maps the segments on the 5-year
repaving plan for each county in District 6.

"""

import env_vars as ev
from env_vars import POSTGRES_URL
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

    print("importing PennDOT RMS data into Postgres")

    os.system(f'ogr2ogr -f "PostgreSQL" "{POSTGRES_URL}" "{url}" -nln rms -overwrite')


def map_each_county(county):
    print(rf"Mapping {county}")
    gdf = gpd.GeoDataFrame.from_postgis(
        rf"""
        WITH tblA AS(
        SELECT 
            "Year",
            to_char(CAST("SR" AS numeric), 'fm0000') AS sr,
            "Road Name",
            "From",
            CAST("SegmentFrom" as numeric) AS sf,
            "OffsetFrom",
            "To",
            CAST("SegmentTo" as numeric) AS st,
            "OffsetTo",
            "Municipality1",
            "Municipality2",
            "Municipality3",
            "Miles Planned",
            CAST(cty_code as numeric) as co_no
        FROM "{county}_County_Plan" 
        ),
        tblB AS(
            SELECT
                "ST_RT_NO" as srno ,
                CAST("CTY_CODE" AS numeric) as co_no,
                CAST("SEG_NO" AS numeric) as seg_no,
                geometry 
            FROM penndot_rms
            ),
        tblC AS(
            SELECT 
                a.*,
                b.seg_no,
                b.geometry
            FROM tblB b
            LEFT OUTER JOIN tblA a
            ON a.sr = b.srno
            AND a.co_no = b.co_no)
        SELECT *
        FROM tblC 
        WHERE seg_no >= sf
        AND seg_no <= st
        AND "Year" IS NOT NULL;
    """,
        con=ENGINE,
        geom_col="geometry",
    )

    return gdf


def map_phila():
    county = "Philadelphia"
    print(rf"Mapping {county}")
    gdf = gpd.GeoDataFrame.from_postgis(
        rf"""
        WITH tblA AS(
        SELECT 
            "Year",
            to_char(CAST("SR" AS numeric), 'fm0000') AS sr,
            "Road Name",
            "From",
            CAST("SegmentFrom" as numeric) AS sf,
            "OffsetFrom",
            "To",
            CAST("SegmentTo" as numeric) AS st,
            "OffsetTo",
            "Municipality1",
            "Miles Planned",
            CAST(cty_code as numeric) as co_no
        FROM "{county}_County_Plan" 
        ),
        tblB AS(
            SELECT
                "ST_RT_NO" as srno ,
                CAST("CTY_CODE" AS numeric) as co_no,
                CAST("SEG_NO" AS numeric) as seg_no,
                geometry 
            FROM penndot_rms
            ),
        tblC AS(
            SELECT 
                a.*,
                b.seg_no,
                b.geometry
            FROM tblA a
            LEFT OUTER JOIN tblB b
            ON a.sr = b.srno
            AND a.co_no = b.co_no)
        SELECT *
        FROM tblC 
        WHERE seg_no >= sf
        AND seg_no <= st
        AND "Year" IS NOT NULL;
    """,
        con=ENGINE,
        geom_col="geometry",
    )

    return gdf


def combine_counties():
    suburban = ["Bucks", "Chester", "Delaware", "Montgomery"]
    frames = []
    for cnty in suburban:
        gdf = map_each_county(cnty)
        frames.append(gdf)
    mapped_suburban = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True))

    mapped_phila = map_phila()
    mapped_frames = [mapped_suburban, mapped_phila]
    mapped_plan = gpd.GeoDataFrame(pd.concat(mapped_frames, ignore_index=True))

    return mapped_plan


def write_postgis(gdf):
    # output spatial file (postgis, shp, and geojson)
    gdf.to_postgis("mapped_plan", con=ENGINE, if_exists="replace")
    print("To database: Complete")


def concat_munis():
    gdf = gpd.GeoDataFrame.from_postgis(
        rf"""
        SELECT *,
        ("Municipality1"|| coalesce (', ' || "Municipality2", '') || coalesce (', ' || "Municipality3", '')) as muni
        FROM mapped_plan
        """,
        con=ENGINE,
        geom_col="geometry",
    )

    return gdf


def write_all_outputs(gdf):
    # output spatial file (postgis, shp, and geojson)
    gdf.to_postgis("mapped_plan", con=ENGINE, if_exists="replace")
    print("To database: Complete")
    gdf.to_file(rf"{ev.DATA_ROOT}/shapefiles/mapped_plan.shp")
    print("To shapefile: Complete")
    gdf.to_file(
        rf"{ev.DATA_ROOT}/geojsons/mapped_plan.geojson",
        driver="GeoJSON",
    )
    print("To GeoJSON: Complete")


def main():
    read_in_penndot_rms()
    # counties = ["Bucks", "Chester", "Delaware", "Montgomery", "Philadelphia"]
    # for county in counties:
    #     add_county_code(county)
    # write_postgis(combine_counties())
    # write_all_outputs(concat_munis())


if __name__ == "__main__":
    main()
