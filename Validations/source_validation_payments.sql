USE Healthcare_Source;

SELECT
    COUNT(*) AS total_records,

    SUM(CASE
        WHEN payment_id IS NULL THEN 1 ELSE 0
    END) AS MISSING_PAYMENT_ID,

    SUM(CASE
        WHEN payment_id <= 0 THEN 1 ELSE 0
    END) AS INVALID_PAYMENT_ID,

    SUM(CASE
        WHEN claim_id IS NULL THEN 1 ELSE 0
    END) AS MISSING_CLAIM_ID,

    SUM(CASE
        WHEN claim_id <= 0 THEN 1 ELSE 0
    END) AS INVALID_CLAIM_ID,

    SUM(CASE
        WHEN patient_id IS NULL THEN 1 ELSE 0
    END) AS MISSING_PATIENT_ID,

    SUM(CASE
        WHEN patient_id <= 0 THEN 1 ELSE 0
    END) AS INVALID_PATIENT_ID,

    SUM(CASE
        WHEN payment_amount <= 0.00 THEN 1 ELSE 0
    END) AS INVALID_PAYMENT_AMOUNT,

    SUM(CASE
        WHEN payment_method IS NULL
          OR TRIM(payment_method) = ''
        THEN 1 ELSE 0
    END) AS MISSING_PAYMENT_METHOD,

    SUM(CASE
        WHEN payment_status NOT IN ('SUCCESSFUL', 'PENDING', 'FAILED') THEN 1 ELSE 0
    END) AS INVALID_PAYMENT_STATUS,

    SUM(CASE
        WHEN transaction_ref IS NULL
          OR TRIM(transaction_ref) = ''
        THEN 1 ELSE 0
    END) AS MISSING_TRANSACTION_REF,

    SUM(CASE
        WHEN updated_at > current_timestamp THEN 1 ELSE 0
    END) AS FUTURE_UPDATED_AT,

    SUM(CASE
        WHEN updated_at IS NULL THEN 1 ELSE 0
    END) AS MISSING_UPDATED_AT

FROM dbo.payments;

SELECT *
FROM dbo.payments
WHERE payment_id IS NULL
   OR payment_id <= 0
   OR claim_id IS NULL
   OR claim_id <= 0
   OR patient_id IS NULL
   OR patient_id <= 0
   OR payment_amount <= 0
   OR payment_method IS NULL
   OR TRIM(payment_method) = ''
   OR transaction_ref IS NULL
   OR TRIM(transaction_ref) = ''
   OR payment_status IS NULL
   OR payment_status NOT IN ('SUCCESSFUL', 'PENDING', 'FAILED')
   OR updated_at IS NULL;

/* ============================================================
   HEALTHCARE PROJECT - PAYMENT SOURCE DATA VALIDATION
   Source table: dbo.payments
   Purpose: Read-only data-quality checks
   ============================================================ */

SET NOCOUNT ON;


/* ============================================================
   1. TOTAL RECORD COUNT
   ============================================================ */

SELECT
    '01 - Total Records' AS validation_check,
    COUNT(*) AS record_count
FROM dbo.payments;


/* ============================================================
   2. MISSING VALUES IN ALL COLUMNS
   ============================================================ */

SELECT
    '02 - Missing Values' AS validation_check,
    SUM(CASE WHEN payment_id IS NULL
             THEN 1 ELSE 0 END) AS missing_payment_id,

    SUM(CASE WHEN claim_id IS NULL
             THEN 1 ELSE 0 END) AS missing_claim_id,

    SUM(CASE WHEN patient_id IS NULL
             THEN 1 ELSE 0 END) AS missing_patient_id,

    SUM(CASE WHEN payment_amount <= 0.00
             THEN 1 ELSE 0 END) AS invalid_payment_amount,

    SUM(CASE WHEN payment_method IS NULL
                  OR LTRIM(RTRIM(payment_method)) = ''
             THEN 1 ELSE 0 END) AS missing_payment_method,

    SUM(CASE WHEN payment_status IS NULL
                  OR payment_status NOT IN ('SUCCESSFUL', 'PENDING', 'FAILED')
             THEN 1 ELSE 0 END) AS invalid_payment_status,

    SUM(CASE WHEN transaction_ref IS NULL
                  OR LTRIM(RTRIM(transaction_ref)) = ''
             THEN 1 ELSE 0 END) AS missing_transaction_ref,

    SUM(CASE WHEN updated_at IS NULL
             THEN 1 ELSE 0 END) AS missing_updated_at
FROM dbo.payments;


/* ============================================================
   3. ALL PAYMENT METHODS
   Review unexpected values.
   ============================================================ */

SELECT
    '03 - Payment Method Distribution' AS validation_check,
    payment_method,
    COUNT(*) AS record_count
FROM dbo.payments
GROUP BY payment_method
ORDER BY payment_method;

/* ============================================================
   4. FUTURE UPDATED TIMESTAMPS
   ============================================================ */

SELECT
    '04 - Future Updated Timestamps' AS validation_check,
    payment_id,
    updated_at
FROM dbo.payments
WHERE updated_at > GETDATE();


/* ============================================================
   5. DUPLICATE PAYMENT IDs
   ============================================================ */

SELECT
    '05 - Duplicate Payment_id IDs' AS validation_check,
    payment_id,
    COUNT(*) AS record_count
FROM dbo.payments
WHERE payment_id IS NOT NULL
GROUP BY payment_id
HAVING COUNT(*) > 1
ORDER BY payment_id;
