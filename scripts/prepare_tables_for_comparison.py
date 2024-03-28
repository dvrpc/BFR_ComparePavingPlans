from env_vars import ENGINE
from sqlalchemy import text

EDIT_COLUMNS = """
    ALTER TABLE "DistrictPlan"
    ADD COLUMN IF NOT EXISTS longcode VARCHAR;

    ALTER TABLE "DistrictPlan"
    ADD COLUMN IF NOT EXISTS shortcode VARCHAR;

    COMMIT;
"""

UPDATE_STATEMENTS = """
    
    UPDATE "DistrictPlan"
    SET shortcode = CONCAT(
        to_char(cast("State Route" AS NUMERIC),'fm0000'),
        to_char(cast("Segment From" AS NUMERIC), 'fm0000'),
        to_char(cast("Segment To" AS NUMERIC), 'fm0000')
    );

    UPDATE "DistrictPlan"
    SET longcode = CONCAT(
        to_char(cast("State Route" AS NUMERIC),'fm0000'),
        to_char(cast("Segment From" AS NUMERIC), 'fm0000'),
        to_char(cast("Offset From" AS NUMERIC), 'fm0000'),
        to_char(CAST("Segment To" AS NUMERIC), 'fm0000'),
        to_char(CAST("Offset to" AS NUMERIC), 'fm0000')
    );

    UPDATE oracle_copy
    SET "shortcode" = CONCAT(
        TO_CHAR(CAST("state_route" AS NUMERIC), 'fm0000'),
        TO_CHAR(CAST("segment_from" AS NUMERIC), 'fm0000'),
        TO_CHAR(CAST("segment_to" AS NUMERIC), 'fm0000')
    );

    UPDATE oracle_copy
    SET "longcode" = CONCAT(
        TO_CHAR(CAST("state_route" AS NUMERIC), 'fm0000'),
        TO_CHAR(CAST("segment_from" AS NUMERIC), 'fm0000'),
        TO_CHAR(CAST("offset_from" AS NUMERIC), 'fm0000'),
        TO_CHAR(CAST("segment_to" AS NUMERIC), 'fm0000'),
        TO_CHAR(CAST("offset_to" AS NUMERIC), 'fm0000')
    );

    COMMIT;
"""


def update_columns():
    """
    update all short and log codes
    """
    try:
        con = ENGINE.connect()
        print(EDIT_COLUMNS)
        con.execute(text(EDIT_COLUMNS))
        con.execute(text(UPDATE_STATEMENTS))
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        con.close()


if __name__ == "__main__":
    update_columns()
