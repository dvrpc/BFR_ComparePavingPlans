# BFR_ComparePavingPlans
Compare 5-year resurfacing plans and track changes

## Run Order

### Map new paving plan:

1. Import latest database records from Oracle DB using `import_oracle.py`
2. Pull data from new 5-year plan PDFs using `scrape_tables.py`
3. Map new plan segments using `map_plan.py`
4. Render mapped segments in webmap using `make_folium_map.py`

### Compare new plan to database records
Assuming `import_oracle.py` and `scrape_tables.py` are run:
3. Modify tables for easy comparison using `prepare_tables_for_comparison.py`
4. Output lists of records and their changes to send for Oracle database update using `compare_plans.py`

### Identify changes in segments that have already been screened
Will require manual updates to reflect new plan years and years that have been screened.
4. Identify changes status of screened segments using `changes_in_screened_segments.py`