"""
compare_plans.py
------------------
This script...

"""


import geopandas as gpd
import pandas as pd
import env_vars as ev
from env_vars import ENGINE


def compare(table):
    print("Comparing")

    sql1 = fr"""
with oldplan as(
	select 
		"CALENDAR_YEAR",
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
		"PLANNED" ,
		"CNT_CODE" ,
		CONCAT("STATE_ROUTE","SEGMENT_FROM", "SEGMENT_TO") as shortcode, 
		CONCAT("STATE_ROUTE","SEGMENT_FROM", to_char(cast("OFFSET_FROM" as numeric), 'fm0000'), "SEGMENT_TO", to_char(cast("OFFSET_TO" as numeric), 'fm0000')) as longcode
	from from_oracle 
), 
newplan as (
	select 
		*,
		"Year",
		CONCAT("sr","sf", "st") AS shortcode,
		trim(trailing '.0' from CONCAT("sr",cast("sf" as text), cast("OffsetFrom" as text), cast("st" as text), to_char(cast("OffsetTo" as numeric), 'fm0000'))) as longcode
	from mapped_plan
)
SELECT 
    a.*,
    b."Year",
    b."Miles Planned",
    b.shortcode,
    b.longcode
FROM oldplan a
INNER JOIN newplan b
ON a.longcode = b.longcode
WHERE a."CALENDAR_YEAR" = b."Year"
AND a."Planned" = b."Miles Planned"
ORDER BY a."CALENDAR_YEAR", a."CNT_CODE"
    """
    con = ENGINE.connect()
    con.execute(sql1)
    con.execute(sql2)
