"""
make_folium_map.py
------------------
This script makes a HTML map using folium
and the geojson data stored within ./data/geojson
"""

from urllib import parse
import folium
from pathlib import Path
import json
from folium.map import Popup
import geopandas as gpd
import env_vars as ev

# ensure geojson files use epsg=4326
def reproject(filelocation):
    for geojsonfilepath in filelocation.rglob("*.geojson"):
        gdf = gpd.read_file(geojsonfilepath)

        # if not already projected, project it and overwrite the original file
        if gdf.crs != 4326:
            gdf.to_crs(epsg=4326, inplace=True)

        gdf.to_file(geojsonfilepath, driver="GeoJSON")


def main():

    data_dir = Path("./data/geojson")
    philly_city_hall = [39.952179401878304, -75.16376402095634]
    output_path = "./maps/5-yearPlan.html"

    reproject(data_dir)

    # make the folium map
    m = folium.Map(
        location=philly_city_hall,
        tiles="cartodbpositron",
        zoom_start=9,
    )

    # add package geojson files to the map
    for geojsonfilepath in data_dir.rglob("*.geojson"):
        file_name = geojsonfilepath.stem
        layername = "5-Year Plan 2022-2026"
        print("Adding", file_name)
        folium.GeoJson(
            json.load(open(geojsonfilepath)),
            name=layername,
            style_function=lambda x: {
                "color": "green"
                if x["properties"]["Year"] == "2022"
                else "orange"
                if x["properties"]["Year"] == "2023"
                else "purple"
                if x["properties"]["Year"] == "2024"
                else "blue"
                if x["properties"]["Year"] == "2025"
                else "pink"
                if x["properties"]["Year"] == "2026"
                else "gray",
                "weight": "4",
            },
            popup=folium.GeoJsonPopup(
                fields=["Year", "sr", "Road Name"],
                aliases=["Planned Year ", "State Route:  ", "Road Name: "],
            ),
            zoom_on_click=False,
        ).add_to(m)

    # add layer toggle box and save to HTML file
    folium.LayerControl().add_to(m)

    print("Writing HTML file to", output_path)
    m.save(output_path)


if __name__ == "__main__":
    main()
