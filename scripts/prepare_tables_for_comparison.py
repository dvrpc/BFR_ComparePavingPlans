from env_vars import ENGINE
from sqlalchemy import text

EDIT_PG_COLUMN_TYPE = """
    ALTER TABLE "DistrictPlan"
    ADD COLUMN IF NOT EXISTS longcode VARCHAR;

    ALTER TABLE "DistrictPlan"
    ADD COLUMN IF NOT EXISTS shortcode VARCHAR;
"""

UPDATE_PG_CODES = """
    
    UPDATE "DistrictPlan"
    SET shortcode = CONCAT(
        tochar(cast("State Route" AS numeric),'fm0000')
        tochar(cast("Segment From" AS numeric), 'fm0000')
        to_char(CAST("Segment To" AS numeric), 'fm0000')
    );


    UPDATE "DistrictPlan"
    SET longcode = CONCAT(
        tochar(cast("State Route" AS numeric),'fm0000')
        tochar(cast("Segment From" AS numeric), 'fm0000')
        "tochar(cast(Offset From" AS numeric, 'fm0000')
        to_char(CAST("Segment To" AS numeric), 'fm0000') 
        to_char(CAST("Offset to" AS numeric), 'fm0000')
    );
"""

# Constants for SQL queries
EDIT_ORACLE_COLUMN_TYPE = """
    ALTER TABLE oracle_copy
    ALTER COLUMN "shortcode" TYPE VARCHAR;
"""

UPDATE_ORACLE_CODES = """
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
def edit_pg_codes():

def edit_oracle_codes():
    """
    Edits and updates Oracle codes in the database.
    """
    try:
        con = ENGINE.connect()
        con.execute(text(EDIT_ORACLE_COLUMN_TYPE))
        con.execute(text(UPDATE_ORACLE_CODES))
    except Exception as e:
        # Handle any exceptions that occur
        print(f"An error occurred: {e}")
    finally:
        # Ensure that the connection is closed
        con.close()


def main():
    """
    Main function to run the desired processes.
    """
    edit_oracle_codes()
    # Consider adding other function calls or logic here
