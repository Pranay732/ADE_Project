SELECT
    TRY_CAST(payment_id AS INT) AS payment_id,
    TRY_CAST(claim_id AS INT) AS claim_id,
    TRY_CAST(patient_id AS INT) AS patient_id,
    TRY_CAST(payment_date AS TIMESTAMP) AS payment_date,
    TRY_CAST(payment_amount AS DECIMAL(12, 2)) AS payment_amount,
    CAST(payment_method AS STRING) AS payment_method,
    CAST(payment_status AS STRING) AS payment_status,
    CAST(transaction_ref AS STRING) AS transaction_ref,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM payments_raw;