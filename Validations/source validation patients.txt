USE Healthcare_Source;

SELECT
    COUNT(*) AS total_records,

    SUM(CASE
        WHEN patient_id IS NULL THEN 1 ELSE 0
    END) AS missing_patient_id,

    SUM(CASE
        WHEN first_name IS NULL
          OR LTRIM(RTRIM(first_name)) = ''
        THEN 1 ELSE 0
    END) AS missing_first_name,

    SUM(CASE
        WHEN last_name IS NULL
          OR LTRIM(RTRIM(last_name)) = ''
        THEN 1 ELSE 0
    END) AS missing_last_name,

    SUM(CASE
        WHEN date_of_birth IS NULL THEN 1 ELSE 0
    END) AS missing_date_of_birth,

    SUM(CASE
        WHEN registration_date IS NULL THEN 1 ELSE 0
    END) AS missing_registration_date,

    SUM(CASE
        WHEN updated_at IS NULL THEN 1 ELSE 0
    END) AS missing_updated_at

FROM dbo.patients;

SELECT *
FROM dbo.patients
WHERE patient_id IS NULL
   OR first_name IS NULL
   OR LTRIM(RTRIM(first_name)) = ''
   OR last_name IS NULL
   OR LTRIM(RTRIM(last_name)) = ''
   OR date_of_birth IS NULL
   OR registration_date IS NULL
   OR updated_at IS NULL;
   
=================================================================

/* ============================================================
   HEALTHCARE PROJECT - PATIENT SOURCE DATA VALIDATION
   Source table: dbo.patients
   Purpose: Read-only data-quality checks
   ============================================================ */

SET NOCOUNT ON;


/* ============================================================
   1. TOTAL RECORD COUNT
   ============================================================ */

SELECT
    '01 - Total Records' AS validation_check,
    COUNT(*) AS record_count
FROM dbo.patients;


/* ============================================================
   2. MISSING VALUES IN ALL COLUMNS
   ============================================================ */

SELECT
    '02 - Missing Values' AS validation_check,
    SUM(CASE WHEN patient_id IS NULL
             THEN 1 ELSE 0 END) AS missing_patient_id,

    SUM(CASE WHEN first_name IS NULL
                  OR LTRIM(RTRIM(first_name)) = ''
             THEN 1 ELSE 0 END) AS missing_first_name,

    SUM(CASE WHEN last_name IS NULL
                  OR LTRIM(RTRIM(last_name)) = ''
             THEN 1 ELSE 0 END) AS missing_last_name,

    SUM(CASE WHEN date_of_birth IS NULL
             THEN 1 ELSE 0 END) AS missing_date_of_birth,

    SUM(CASE WHEN gender IS NULL
                  OR LTRIM(RTRIM(gender)) = ''
             THEN 1 ELSE 0 END) AS missing_gender,

    SUM(CASE WHEN phone IS NULL
                  OR LTRIM(RTRIM(phone)) = ''
             THEN 1 ELSE 0 END) AS missing_phone,

    SUM(CASE WHEN email IS NULL
                  OR LTRIM(RTRIM(email)) = ''
             THEN 1 ELSE 0 END) AS missing_email,

    SUM(CASE WHEN address IS NULL
                  OR LTRIM(RTRIM(address)) = ''
             THEN 1 ELSE 0 END) AS missing_address,

    SUM(CASE WHEN city IS NULL
                  OR LTRIM(RTRIM(city)) = ''
             THEN 1 ELSE 0 END) AS missing_city,

    SUM(CASE WHEN state IS NULL
                  OR LTRIM(RTRIM(state)) = ''
             THEN 1 ELSE 0 END) AS missing_state,

    SUM(CASE WHEN registration_date IS NULL
             THEN 1 ELSE 0 END) AS missing_registration_date,

    SUM(CASE WHEN updated_at IS NULL
             THEN 1 ELSE 0 END) AS missing_updated_at
FROM dbo.patients;


/* ============================================================
   3. ALL GENDER VALUES
   Review unexpected values such as X.
   ============================================================ */

SELECT
    '03 - Gender Distribution' AS validation_check,
    gender,
    COUNT(*) AS record_count
FROM dbo.patients
GROUP BY gender
ORDER BY gender;


/* ============================================================
   4. INVALID EMAIL FORMATS
   Basic format check only; not full email verification.
   ============================================================ */

SELECT
    '04 - Invalid Emails' AS validation_check,
    patient_id,
    email
FROM dbo.patients
WHERE email IS NOT NULL
  AND LTRIM(RTRIM(email)) <> ''
  AND email NOT LIKE '%_@_%._%';


/* ============================================================
   5. INVALID PHONE NUMBERS
   Assumes phone numbers should contain exactly 10 digits.
   ============================================================ */

SELECT
    '05 - Invalid Phones' AS validation_check,
    patient_id,
    phone
FROM dbo.patients
WHERE phone IS NOT NULL
  AND LTRIM(RTRIM(phone)) <> ''
  AND (
      LEN(LTRIM(RTRIM(phone))) <> 10
      OR LTRIM(RTRIM(phone)) LIKE '%[^0-9]%'
  );


/* ============================================================
   6. FUTURE DATES OF BIRTH
   ============================================================ */

SELECT
    '06 - Future Date of Birth' AS validation_check,
    patient_id,
    date_of_birth
FROM dbo.patients
WHERE date_of_birth > CAST(GETDATE() AS DATE);


/* ============================================================
   7. FUTURE REGISTRATION DATES
   ============================================================ */

SELECT
    '07 - Future Registration Dates' AS validation_check,
    patient_id,
    registration_date
FROM dbo.patients
WHERE registration_date > CAST(GETDATE() AS DATE);


/* ============================================================
   8. FUTURE UPDATED TIMESTAMPS
   ============================================================ */

SELECT
    '08 - Future Updated Timestamps' AS validation_check,
    patient_id,
    updated_at
FROM dbo.patients
WHERE updated_at > GETDATE();


/* ============================================================
   9. DUPLICATE PATIENT IDs
   ============================================================ */

SELECT
    '09 - Duplicate Patient IDs' AS validation_check,
    patient_id,
    COUNT(*) AS record_count
FROM dbo.patients
WHERE patient_id IS NOT NULL
GROUP BY patient_id
HAVING COUNT(*) > 1
ORDER BY patient_id;
