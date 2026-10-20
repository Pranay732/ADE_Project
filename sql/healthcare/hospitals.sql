SELECT
    TRY_CAST(hospital_id AS INT) AS hospital_id,
    CAST(hospital_name AS STRING) AS hospital_name,
    CAST(hospital_type AS STRING) AS hospital_type,
    CAST(city AS STRING) AS city,
    CAST(state AS STRING) AS state,
    TRY_CAST(bed_count AS INT) AS bed_count,
    CAST(status AS STRING) AS status,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM hospitals_raw;