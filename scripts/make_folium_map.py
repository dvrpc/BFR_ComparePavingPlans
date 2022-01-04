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


def add_categorical_legend(folium_map, title, colors, labels):
    if len(colors) != len(labels):
        raise ValueError("colors and labels must have the same length.")

    color_by_label = dict(zip(labels, colors))

    legend_categories = ""
    for label, color in color_by_label.items():
        legend_categories += f"<li><span style='background:{color}'></span>{label}</li>"

    legend_html = f"""
    <div id='maplegend' class='maplegend'>
      <div class='legend-title'>{title}</div>
      <div class='legend-scale'>
        <ul class='legend-labels'>
        {legend_categories}
        </ul>
      </div>
    </div>
    """
    script = f"""
        <script type="text/javascript">
        var oneTimeExecution = (function() {{
                    var executed = false;
                    return function() {{
                        if (!executed) {{
                             var checkExist = setInterval(function() {{
                                       if ((document.getElementsByClassName('leaflet-top leaflet-right').length) || (!executed)) {{
                                          document.getElementsByClassName('leaflet-top leaflet-right')[0].style.display = "flex"
                                          document.getElementsByClassName('leaflet-top leaflet-right')[0].style.flexDirection = "column"
                                          document.getElementsByClassName('leaflet-top leaflet-right')[0].innerHTML += `{legend_html}`;
                                          clearInterval(checkExist);
                                          executed = true;
                                       }}
                                    }}, 100);
                        }}
                    }};
                }})();
        oneTimeExecution()
        </script>
      """

    css = """

    <style type='text/css'>
      .maplegend {
        z-index:9999;
        float:left;
        background-color: rgba(255, 255, 255, 1);
        border-radius: 5px;
        border: 2px solid #bbb;
        padding: 10px;
        font-size:12px;
        position: fixed;
        bottom: 10px;
        left: 16px;
      }
      .maplegend .legend-title {
        text-align: left;
        margin-bottom: 5px;
        font-weight: bold;
        font-size: 90%;
        }
      .maplegend .legend-scale ul {
        margin: 0;
        margin-bottom: 5px;
        padding: 0;
        float: left;
        list-style: none;
        }
      .maplegend .legend-scale ul li {
        font-size: 80%;
        list-style: none;
        margin-left: 0;
        line-height: 18px;
        margin-bottom: 2px;
        }
      .maplegend ul.legend-labels li span {
        display: block;
        float: left;
        height: 16px;
        width: 30px;
        margin-right: 5px;
        margin-left: 0;
        border: 0px solid #ccc;
        }
      .maplegend .legend-source {
        font-size: 80%;
        color: #777;
        clear: both;
        }
      .maplegend a {
        color: #777;
        }
    </style>
    """

    folium_map.get_root().header.add_child(folium.Element(script + css))

    return folium_map


def main():

    data_dir = Path(fr"{ev.DATA_ROOT}/geojsons")
    # file = data_dir / "mapped_plan.geojson"
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

    m = add_categorical_legend(
        m,
        "Legend",
        colors=["green", "orange", "purple", "blue", "pink"],
        labels=["2022", "2023", "2024", "2025", "2026"],
    )

    # add layer toggle box and save to HTML file
    # folium.LayerControl().add_to(m)

    print("Writing HTML file to", output_path)
    m.save(output_path)


if __name__ == "__main__":
    main()
