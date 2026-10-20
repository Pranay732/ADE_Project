SELECT
    TRY_CAST(claim_id AS INT) AS claim_id,
    TRY_CAST(patient_id AS INT) AS patient_id,
    TRY_CAST(encounter_id AS INT) AS encounter_id,
    TRY_CAST(patient_insurance_id AS INT) AS patient_insurance_id,
    TRY_CAST(claim_date AS TIMESTAMP) AS claim_date,
    TRY_CAST(submitted_date AS TIMESTAMP) AS submitted_date,
    TRY_CAST(processed_date AS TIMESTAMP) AS processed_date,
    CAST(claim_status AS STRING) AS claim_status,
    TRY_CAST(claim_amount AS DECIMAL(12, 2)) AS claim_amount,
    TRY_CAST(approved_amount AS DECIMAL(12, 2)) AS approved_amount,
    TRY_CAST(rejected_amount AS DECIMAL(12, 2)) AS rejected_amount,
    TRY_CAST(patient_responsibility AS DECIMAL(12, 2)) AS patient_responsibility,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM claims_raw;