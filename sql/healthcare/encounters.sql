SELECT
    TRY_CAST(encounter_id AS INT) AS encounter_id,
    TRY_CAST(patient_id AS INT) AS patient_id,
    TRY_CAST(provider_id AS INT) AS provider_id,
    TRY_CAST(hospital_id AS INT) AS hospital_id,
    TRY_CAST(diagnosis_id AS INT) AS diagnosis_id,
    TRY_CAST(procedure_id AS INT) AS procedure_id,
    TRY_CAST(appointment_id AS BIGINT) AS appointment_id,
    TRY_CAST(encounter_date AS TIMESTAMP) AS encounter_date,
    CAST(encounter_type AS STRING) AS encounter_type,
    CAST(encounter_status AS STRING) AS encounter_status,
    TRY_CAST(total_charge AS DECIMAL(12, 2)) AS total_charge,
    CAST(notes AS STRING) AS notes,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM encounters_raw;