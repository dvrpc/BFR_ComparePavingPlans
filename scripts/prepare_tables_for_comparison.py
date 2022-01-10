"""
prepare_tables_for_comparison.py
------------------
This script modifies and updates tables in 
the postgres database so they are ready to be
compared in 'compare_plans.py'.

"""
import pandas as pd
import env_vars as ev
from env_vars import ENGINE


def combine_plans_full_district():
    counties = ["Bucks", "Chester", "Delaware", "Montgomery"]

    frames = []
    for county in counties:
        df = pd.read_sql(
            fr"""
            SELECT *
            FROM "{county}_County_Plan";
        """,
            con=ENGINE,
        )
        frames.append(df)
        # print(fr"Collected {county}")

    suburban_counties = pd.concat(frames, ignore_index=True)

    phila_county = pd.read_sql(
        """
    SELECT *
    FROM "Philadelphia_County_Plan";
    """,
        con=ENGINE,
    )

    to_combine = [suburban_counties, phila_county]
    all_counties = pd.concat(to_combine, ignore_index=True)

    all_counties.to_sql("District_Plan", ENGINE, if_exists="replace")


def add_codes_to_district_plan():
    add_codes_fields = """
    ALTER TABLE "District_Plan"
    ADD COLUMN IF NOT EXISTS longcode VARCHAR;

    ALTER TABLE "District_Plan"
    ADD COLUMN IF NOT EXISTS shortcode VARCHAR;

    COMMIT;
    """

    populate_codes = """
    UPDATE "District_Plan"
    SET shortcode = CONCAT("SR","SegmentFrom", to_char(CAST("SegmentTo" AS numeric), 'fm0000'));


    UPDATE "District_Plan"
    SET longcode = CONCAT("SR","SegmentFrom", "OffsetFrom", to_char(CAST("SegmentTo" AS numeric), 'fm0000'), to_char(CAST("OffsetTo" AS numeric), 'fm0000'));

    COMMIT;
    """
    con = ENGINE.connect()
    con.execute(add_codes_fields)
    con.execute(populate_codes)


def edit_oracle_codes():
    edit_oracle_column_type = """
    alter table from_oracle 
    alter column "SHORTCODE" type varchar;
    """

    update_oracle_codes = """
    update from_oracle 
    set "SHORTCODE" = CONCAT(to_char(CAST("STATE_ROUTE" AS numeric), 'fm0000'), to_char(cast("SEGMENT_FROM" as numeric), 'fm0000'), to_char(cast("SEGMENT_TO" as numeric), 'fm0000'))
    ;

    update from_oracle
    set "LONGCODE" = CONCAT(to_char(CAST("STATE_ROUTE" AS numeric), 'fm0000'), to_char(cast("SEGMENT_FROM" as numeric), 'fm0000'), to_char(cast("OFFSET_FROM" as numeric), 'fm0000'), to_char(cast("SEGMENT_TO" as numeric), 'fm0000'), to_char(cast("OFFSET_TO" as numeric), 'fm0000'))
    ;

    COMMIT;
    """
    con = ENGINE.connect()
    con.execute(edit_oracle_column_type)
    con.execute(update_oracle_codes)


def main():
    combine_plans_full_district()
    add_codes_to_district_plan()
    edit_oracle_codes()


if __name__ == "__main__":
    main()
