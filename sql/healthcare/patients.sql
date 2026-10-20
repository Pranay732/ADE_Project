SELECT
    TRY_CAST(patient_id AS INT) AS patient_id,
    CAST(first_name AS STRING) AS first_name,
    CAST(last_name AS STRING) AS last_name,
    TRY_CAST(date_of_birth AS DATE) AS date_of_birth,
    CAST(gender AS STRING) AS gender,
    CAST(phone AS STRING) AS phone,
    CAST(email AS STRING) AS email,
    CAST(address AS STRING) AS address,
    CAST(city AS STRING) AS city,
    CAST(state AS STRING) AS state,
    TRY_CAST(registration_date AS DATE) AS registration_date,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM patients_raw;