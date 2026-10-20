SELECT
    TRY_CAST(procedure_id AS INT) AS procedure_id,
    CAST(procedure_code AS STRING) AS procedure_code,
    CAST(procedure_name AS STRING) AS procedure_name,
    CAST(procedure_category AS STRING) AS procedure_category,
    TRY_CAST(standard_cost AS DECIMAL(12, 2)) AS standard_cost,
    CAST(status AS STRING) AS status,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM procedures_raw;