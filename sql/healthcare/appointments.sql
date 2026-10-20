SELECT
    TRY_CAST(appointment_id AS BIGINT) AS appointment_id,
    TRY_CAST(patient_id AS INT) AS patient_id,
    TRY_CAST(provider_id AS INT) AS provider_id,
    TRY_CAST(hospital_id AS INT) AS hospital_id,
    TRY_CAST(appointment_date AS TIMESTAMP) AS appointment_date,
    CAST(appointment_type AS STRING) AS appointment_type,
    CAST(appointment_status AS STRING) AS appointment_status,
    CAST(reason AS STRING) AS reason,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM appointments_raw;