import pyodbc
import pandas as pd
from datetime import datetime
from pathlib import Path


# ============================================================
# 1. RUNTIME CONFIGURATION
# ============================================================

LOAD_TYPE = "INCREMENTAL"
# Change to "FULL" when you want a full load.


# ============================================================
# 2. DATABASE CONNECTION
# ============================================================

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Healthcare_Source;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)


# ============================================================
# 3. RAW BASE FOLDER
# ============================================================

RAW_BASE_PATH = Path(
    r"E:\ADE_Project\data\raw"
)


# ============================================================
# 4. CREATE DATABASE CONNECTION
# ============================================================

conn = pyodbc.connect(CONNECTION_STRING)
cursor = conn.cursor()

print("=" * 70)
print("HEALTHCARE DATA INGESTION")
print("=" * 70)
print(f"LOAD TYPE : {LOAD_TYPE}")
print(f"START TIME: {datetime.now()}")
print("=" * 70)


# ============================================================
# 5. READ ACTIVE TABLE CONFIGURATION
# ============================================================

config_query = """
SELECT
    prcs_id,
    source_table_name,
    primary_key_column,
    watermark_column,
    raw_folder
FROM dbo.etl_table_config
WHERE is_active = 1
ORDER BY prcs_id;
"""

config_df = pd.read_sql(config_query, conn)


# ============================================================
# 6. PROCESS EACH TABLE
# ============================================================

for _, config in config_df.iterrows():

    prcs_id = int(config["prcs_id"])
    table_name = config["source_table_name"]
    primary_key_column = config["primary_key_column"]
    watermark_column = config["watermark_column"]
    raw_folder = config["raw_folder"]

    run_start_time = datetime.now()

    print("\n")
    print("=" * 70)
    print(f"PROCESS ID : {prcs_id}")
    print(f"TABLE      : {table_name}")
    print(f"LOAD TYPE  : {LOAD_TYPE}")
    print("=" * 70)


    # ========================================================
    # 7. CREATE PROCESS RUN
    # ========================================================

    run_log_insert = """
    INSERT INTO dbo.etl_run_log
    (
        prcs_id,
        source_table_name,
        load_type,
        run_start_time,
        run_status
    )
    OUTPUT INSERTED.prcs_run_id
    VALUES
    (
        ?,
        ?,
        ?,
        ?,
        'RUNNING'
    );
    """

    cursor.execute(
        run_log_insert,
        (
            prcs_id,
            table_name,
            LOAD_TYPE,
            run_start_time
        )
    )

    prcs_run_id = cursor.fetchone()[0]

    conn.commit()

    print(f"PROCESS RUN ID: {prcs_run_id}")


    try:

        # ====================================================
        # 8. BUILD EXTRACTION QUERY
        # ====================================================

        if LOAD_TYPE == "FULL":

            query = f"""
            SELECT *
            FROM dbo.{table_name};
            """

            print("Running FULL LOAD")


        elif LOAD_TYPE == "INCREMENTAL":

            # ------------------------------------------------
            # Check whether watermark already exists
            # ------------------------------------------------

            watermark_check_query = """
            SELECT last_watermark
            FROM dbo.etl_watermark
            WHERE source_table_name = ?;
            """

            cursor.execute(
                watermark_check_query,
                (table_name,)
            )

            watermark_result = cursor.fetchone()


            if watermark_result is None:

                # ============================================
                # INITIAL INCREMENTAL LOAD
                # ============================================

                print("No watermark found.")
                print("Running INITIAL INCREMENTAL LOAD.")

                query = f"""
                SELECT *
                FROM dbo.{table_name};
                """

            else:

                # ============================================
                # NORMAL INCREMENTAL LOAD
                # ============================================

                print("Watermark found.")
                print("Running NORMAL INCREMENTAL LOAD.")

                query = f"""
                SELECT *
                FROM dbo.{table_name}
                WHERE {watermark_column} >
                (
                    SELECT last_watermark
                    FROM dbo.etl_watermark
                    WHERE source_table_name = ?
                );
                """


        else:

            raise ValueError(
                "Invalid LOAD_TYPE. Use FULL or INCREMENTAL."
            )


        # ====================================================
        # 9. EXECUTE EXTRACTION
        # ====================================================

        if (
            LOAD_TYPE == "INCREMENTAL"
            and watermark_result is not None
        ):

            df = pd.read_sql(
                query,
                conn,
                params=(table_name,)
            )

        else:

            df = pd.read_sql(
                query,
                conn
            )


        records_extracted = len(df)

        print(f"Records extracted: {records_extracted}")


        # ====================================================
        # 10. HANDLE NO DATA
        # ====================================================

        if records_extracted == 0:

            run_end_time = datetime.now()

            update_run_log = """
            UPDATE dbo.etl_run_log
            SET
                run_end_time = ?,
                records_extracted = 0,
                run_status = 'NO_DATA',
                error_message = NULL
            WHERE prcs_run_id = ?;
            """

            cursor.execute(
                update_run_log,
                (
                    run_end_time,
                    prcs_run_id
                )
            )

            conn.commit()

            print("No new records found.")
            print("No RAW file created.")
            print(f"prcs_run_id {prcs_run_id} marked as NO_DATA.")

            continue


        # ====================================================
        # 11. CREATE RAW FOLDER
        # ====================================================

        table_raw_path = RAW_BASE_PATH / raw_folder

        table_raw_path.mkdir(
            parents=True,
            exist_ok=True
        )


        # ====================================================
        # 12. CREATE RAW FILE NAME
        # ====================================================

        timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )

        raw_file_path = (
            table_raw_path
            / f"{table_name}_{timestamp}.csv"
        )


        # ====================================================
        # 13. WRITE RAW FILE
        # ====================================================

        df.to_csv(
            raw_file_path,
            index=False
        )

        print(f"RAW file created:")
        print(raw_file_path)


        # ====================================================
        # 14. UPDATE WATERMARK
        # ====================================================
        #
        # IMPORTANT:
        # Watermark calculation is performed entirely inside
        # SQL Server so DATETIME2(7) precision is preserved.
        #
        # We do NOT take MAX(updated_at) into Python.
        #

        if LOAD_TYPE == "INCREMENTAL":

            watermark_update_query = f"""
            DECLARE @new_watermark DATETIME2(7);

            SELECT
                @new_watermark = MAX({watermark_column})
            FROM dbo.{table_name};


            IF EXISTS
            (
                SELECT 1
                FROM dbo.etl_watermark
                WHERE source_table_name = ?
            )
            BEGIN

                UPDATE dbo.etl_watermark
                SET
                    last_watermark = @new_watermark,
                    last_run_time = ?,
                    last_run_status = 'SUCCESS'
                WHERE source_table_name = ?;

            END

            ELSE
            BEGIN

                INSERT INTO dbo.etl_watermark
                (
                    source_table_name,
                    last_watermark,
                    last_run_time,
                    last_run_status
                )
                VALUES
                (
                    ?,
                    @new_watermark,
                    ?,
                    'SUCCESS'
                );

            END
            """

            cursor.execute(
                watermark_update_query,
                (
                    table_name,
                    run_start_time,
                    table_name,
                    table_name,
                    run_start_time
                )
            )

            conn.commit()


            # ----------------------------------------------
            # Read watermark back from SQL Server
            # ----------------------------------------------

            watermark_read_query = """
            SELECT last_watermark
            FROM dbo.etl_watermark
            WHERE source_table_name = ?;
            """

            cursor.execute(
                watermark_read_query,
                (table_name,)
            )

            updated_watermark = cursor.fetchone()[0]

            print(
                f"Updated watermark: {updated_watermark}"
            )


        # ====================================================
        # 15. UPDATE RUN LOG - SUCCESS
        # ====================================================

        run_end_time = datetime.now()

        update_success_log = """
        UPDATE dbo.etl_run_log
        SET
            run_end_time = ?,
            records_extracted = ?,
            raw_file_path = ?,
            run_status = 'SUCCESS',
            error_message = NULL
        WHERE prcs_run_id = ?;
        """

        cursor.execute(
            update_success_log,
            (
                run_end_time,
                records_extracted,
                str(raw_file_path),
                prcs_run_id
            )
        )

        conn.commit()

        print(
            f"prcs_run_id {prcs_run_id} marked as SUCCESS."
        )


    # ========================================================
    # 16. ERROR HANDLING
    # ========================================================

    except Exception as e:

        run_end_time = datetime.now()

        error_message = str(e)

        print(
            f"ERROR in {table_name}: {error_message}"
        )


        update_failure_log = """
        UPDATE dbo.etl_run_log
        SET
            run_end_time = ?,
            records_extracted = ?,
            run_status = 'FAILED',
            error_message = ?
        WHERE prcs_run_id = ?;
        """

        cursor.execute(
            update_failure_log,
            (
                run_end_time,
                0,
                error_message,
                prcs_run_id
            )
        )

        conn.commit()

        print(
            f"prcs_run_id {prcs_run_id} marked as FAILED."
        )


# ============================================================
# 17. CLOSE CONNECTION
# ============================================================

cursor.close()
conn.close()


# ============================================================
# 18. COMPLETION MESSAGE
# ============================================================

print("\n")
print("=" * 70)
print("INGESTION COMPLETED")
print(f"END TIME: {datetime.now()}")
print("=" * 70)