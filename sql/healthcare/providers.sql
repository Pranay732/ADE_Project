SELECT
    TRY_CAST(provider_id AS INT) AS provider_id,
    CAST(provider_name AS STRING) AS provider_name,
    CAST(specialization AS STRING) AS specialization,
    TRY_CAST(hospital_id AS INT) AS hospital_id,
    CAST(license_number AS STRING) AS license_number,
    CAST(city AS STRING) AS city,
    CAST(state AS STRING) AS state,
    CAST(status AS STRING) AS status,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(provider_type AS STRING) AS provider_type
FROM providers_raw;