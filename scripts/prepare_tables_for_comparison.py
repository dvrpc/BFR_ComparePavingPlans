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
