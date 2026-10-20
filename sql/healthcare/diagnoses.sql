SELECT
    TRY_CAST(diagnosis_id AS INT) AS diagnosis_id,
    CAST(diagnosis_code AS STRING) AS diagnosis_code,
    CAST(diagnosis_name AS STRING) AS diagnosis_name,
    CAST(diagnosis_category AS STRING) AS diagnosis_category,
    CAST(severity AS STRING) AS severity,
    CAST(status AS STRING) AS status,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM diagnoses_raw;