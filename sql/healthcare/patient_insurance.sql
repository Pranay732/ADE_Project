SELECT
    TRY_CAST(patient_insurance_id AS INT) AS patient_insurance_id,
    TRY_CAST(patient_id AS INT) AS patient_id,
    TRY_CAST(insurance_plan_id AS INT) AS insurance_plan_id,
    CAST(policy_number AS STRING) AS policy_number,
    TRY_CAST(coverage_start_date AS DATE) AS coverage_start_date,
    TRY_CAST(coverage_end_date AS DATE) AS coverage_end_date,
    CAST(status AS STRING) AS status,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM patient_insurance_raw;