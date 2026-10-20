SELECT
    TRY_CAST(claim_line_id AS INT) AS claim_line_id,
    TRY_CAST(claim_id AS INT) AS claim_id,
    TRY_CAST(procedure_id AS INT) AS procedure_id,
    TRY_CAST(diagnosis_id AS INT) AS diagnosis_id,
    TRY_CAST(quantity AS INT) AS quantity,
    TRY_CAST(unit_cost AS DECIMAL(12, 2)) AS unit_cost,
    TRY_CAST(line_amount AS DECIMAL(12, 2)) AS line_amount,
    TRY_CAST(approved_amount AS DECIMAL(12, 2)) AS approved_amount,
    TRY_CAST(rejected_amount AS DECIMAL(12, 2)) AS rejected_amount,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM claim_lines_raw;