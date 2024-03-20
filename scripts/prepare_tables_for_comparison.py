"""
prepare_tables_for_comparison.py
------------------
This script modifies and updates tables in 
the postgres database so they are ready to be
compared in 'compare_plans.py'.

"""
from env_vars import ENGINE
from sqlalchemy import text


def add_codes_to_district_plan():
    add_codes_fields = """
    ALTER TABLE "DistrictPlan"
    ADD COLUMN IF NOT EXISTS longcode VARCHAR;

    ALTER TABLE "DistrictPlan"
    ADD COLUMN IF NOT EXISTS shortcode VARCHAR;

    COMMIT;
    """

    populate_codes = """
    UPDATE "DistrictPlan"
    SET shortcode = CONCAT("State Route","Segment From", to_char(CAST("Segment To" AS numeric), 'fm0000'));


    UPDATE "DistrictPlan"
    SET longcode = CONCAT("State Route","Segment From", "Offset From", to_char(CAST("Segment To" AS numeric), 'fm0000'), to_char(CAST("Offset to" AS numeric), 'fm0000'));

    COMMIT;
    """
    con = ENGINE.connect()
    con.execute(text(add_codes_fields))
    con.execute(text(populate_codes))


def edit_oracle_codes():
    edit_oracle_column_type = """
    alter table oracle_copy 
    alter column "shortcode" type varchar;
    """

    update_oracle_codes = """
    update oracle_copy 
    set "shortcode" = CONCAT(to_char(CAST("state_route" AS numeric), 'fm0000'), to_char(cast("segment_from" as numeric), 'fm0000'), to_char(cast("segment_to" as numeric), 'fm0000'))
    ;

    update oracle_copy
    set "longcode" = CONCAT(to_char(CAST("state_route" AS numeric), 'fm0000'), to_char(cast("segment_from" as numeric), 'fm0000'), to_char(cast("offset_from" as numeric), 'fm0000'), to_char(cast("segment_to" as numeric), 'fm0000'), to_char(cast("offset_to" as numeric), 'fm0000'))
    ;

    COMMIT;
    """
    con = ENGINE.connect()
    con.execute(text(edit_oracle_column_type))
    con.execute(text(update_oracle_codes))


def main():
    add_codes_to_district_plan()
    edit_oracle_codes()


if __name__ == "__main__":
    main()
