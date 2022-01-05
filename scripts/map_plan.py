"""
map_plan.py
------------------
This script maps the segments on the 5-year
repaving plan for each county in District 6.

"""

import geopandas as gpd
import pandas as pd
from geoalchemy2 import WKTElement
from sqlalchemy import engine
import env_vars as ev
from env_vars import ENGINE


def read_in_penndot_rms():
    print("Gathering PennDOT data")
    # import PennDOT's rms layer from GIS portal - RMSSEG (State Roads)
    gdf = gpd.read_file(
        "https://opendata.arcgis.com/datasets/d9a2a5df74cf4726980e5e276d51fe8d_0.geojson"
    )

    # remove null geometries
    gdf = gdf[gdf.geometry.notnull()]
    # remove records outside of district 6
    gdf = gdf.loc[gdf["DISTRICT_NO"] == "06"]

    # transform projection from 4326 to 26918
    gdf = gdf.to_crs(epsg=26918)

    # create geom column for postgis import
    gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=26918))

    # write geodataframe to postgis
    gdf.to_postgis("penndot_rms", con=ENGINE, if_exists="replace")


def add_county_code(county):
    print("Adding county codes")
    county_lookup = {
        "Bucks": "09",
        "Chester": "15",
        "Delaware": "23",
        "Montgomery": "46",
        "Philadelphia": "67",
    }
    code = county_lookup[county]

    sql1 = fr"""
    ALTER TABLE "{county}_County_Plan"    
    ADD COLUMN IF NOT EXISTS cty_code VARCHAR;
    COMMIT;
    """

    sql2 = fr"""
    UPDATE "{county}_County_Plan"
    SET cty_code = {code};
    COMMIT;
    """
    con = ENGINE.connect()
    con.execute(sql1)
    con.execute(sql2)


def map_each_county(county):
    print(fr"Mapping {county}")
    gdf = gpd.GeoDataFrame.from_postgis(
        fr"""
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
    print(fr"Mapping {county}")
    gdf = gpd.GeoDataFrame.from_postgis(
        fr"""
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


def write_outputs(gdf):
    # output spatial file (postgis, shp, and geojson)
    gdf.to_postgis("mapped_plan", con=ENGINE, if_exists="replace")
    print("To database: Complete")
    gdf.to_file(fr"{ev.DATA_ROOT}/shapefiles/mapped_plan.shp")
    print("To shapefile: Complete")
    gdf.to_file(
        fr"{ev.DATA_ROOT}/geojsons/mapped_plan.geojson",
        driver="GeoJSON",
    )
    print("To GeoJSON: Complete")


def main():
    # read_in_penndot_rms()
    counties = ["Bucks", "Chester", "Delaware", "Montgomery", "Philadelphia"]
    for county in counties:
        add_county_code(county)
    write_outputs(combine_counties())


if __name__ == "__main__":
    main()
