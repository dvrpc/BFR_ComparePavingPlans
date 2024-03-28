


--New Segment: Identified if no matching CNT_CODE exists in from_oracle.ls 
select * from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode 
where a."source" = '5-year plan 2023-2027'
and a.cnt_code is null

--No Change: If both shortcode and longcode match, and CALENDAR_YEAR is the same.
select * from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode 
where a."source" = '5-year plan 2023-2027'
and a.shortcode = b.shortcode 
and a.longcode = b.longcode 
and a.calendar_year = b."Calendar year"::numeric  

--Year Change Only: If shortcode and longcode match, but CALENDAR_YEAR differs.
select * from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode 
where a."source" = '5-year plan 2023-2027'
and a.shortcode = b.shortcode 
and a.longcode = b.longcode 
and a.calendar_year != b."Calendar year"::numeric 

--Length Change Only: If shortcode matches, longcode differs, but CALENDAR_YEAR is the same.
select * from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode 
where a."source" = '5-year plan 2023-2027'
and a.shortcode = b.shortcode 
and a.longcode != b.longcode 
and a.calendar_year = b."Calendar year"::numeric 


--New Year and Length: If shortcode matches, but both longcode and CALENDAR_YEAR differ.
select * from oracle_copy a
inner join "DistrictPlan" b
on a.shortcode = b.shortcode 
where a."source" = '5-year plan 2023-2027'
and a.shortcode = b.shortcode
and a.longcode != b.longcode 
and a.calendar_year != b."Calendar year"::numeric 

