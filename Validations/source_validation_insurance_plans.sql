USE Healthcare_Source;

/* ============================================================
   HEALTHCARE PROJECT - INSURANCE PLAN SOURCE DATA VALIDATION
   Source table: dbo.insurance_plans
   Purpose: Read-only data-quality checks
   ============================================================ */


/* ============================================================
   1. TOTAL RECORD COUNT
   ============================================================ */

SELECT
    '01 - Total Records' AS validation_check,
    COUNT(*) AS record_count
FROM dbo.insurance_plans;


/* ============================================================
   2. MISSING AND INVALID VALUES
   ============================================================ */

SELECT
    '02 - Missing and Invalid Values' AS validation_check,
    SUM(CASE WHEN insurance_plan_id IS NULL
             THEN 1 ELSE 0 END) AS missing_insurance_plan_id,

    SUM(CASE WHEN insurance_plan_id <= 0
             THEN 1 ELSE 0 END) AS invalid_insurance_plan_id,

    SUM(CASE WHEN insurance_company IS NULL
                  OR LTRIM(RTRIM(insurance_company)) = ''
             THEN 1 ELSE 0 END) AS missing_insurance_company,

    SUM(CASE WHEN plan_name IS NULL
                  OR LTRIM(RTRIM(plan_name)) = ''
             THEN 1 ELSE 0 END) AS missing_plan_name,

    SUM(CASE WHEN plan_type IS NULL
                  OR LTRIM(RTRIM(plan_type)) = ''
             THEN 1 ELSE 0 END) AS missing_plan_type,

    SUM(CASE WHEN coverage_percentage IS NULL
             THEN 1 ELSE 0 END) AS missing_coverage_percentage,

    SUM(CASE WHEN coverage_percentage < 0
                   OR coverage_percentage > 100
             THEN 1 ELSE 0 END) AS invalid_coverage_percentage,

    SUM(CASE WHEN annual_limit < 0
             THEN 1 ELSE 0 END) AS invalid_annual_limit,

    SUM(CASE WHEN status IS NOT NULL
                   AND UPPER(LTRIM(RTRIM(status))) NOT IN ('ACTIVE', 'INACTIVE')
             THEN 1 ELSE 0 END) AS invalid_status,

    SUM(CASE WHEN updated_at IS NULL
             THEN 1 ELSE 0 END) AS missing_updated_at,

    SUM(CASE WHEN updated_at > GETDATE()
             THEN 1 ELSE 0 END) AS future_updated_at
FROM dbo.insurance_plans;


/* ============================================================
   3. PLAN TYPE DISTRIBUTION
   Review unexpected values.
   ============================================================ */

SELECT
    '03 - Plan Type Distribution' AS validation_check,
    plan_type,
    COUNT(*) AS record_count
FROM dbo.insurance_plans
GROUP BY plan_type
ORDER BY plan_type;


/* ============================================================
   4. STATUS DISTRIBUTION
   NULL status values are allowed.
   ============================================================ */

SELECT
    '04 - Status Distribution' AS validation_check,
    status,
    COUNT(*) AS record_count
FROM dbo.insurance_plans
GROUP BY status
ORDER BY status;


/* ============================================================
   5. INVALID SOURCE RECORDS
   ============================================================ */

SELECT *
FROM dbo.insurance_plans
WHERE insurance_plan_id IS NULL
   OR insurance_plan_id <= 0
   OR insurance_company IS NULL
   OR LTRIM(RTRIM(insurance_company)) = ''
   OR plan_name IS NULL
   OR LTRIM(RTRIM(plan_name)) = ''
   OR plan_type IS NULL
   OR LTRIM(RTRIM(plan_type)) = ''
   OR coverage_percentage IS NULL
   OR coverage_percentage < 0
   OR coverage_percentage > 100
   OR annual_limit < 0
   OR (
       status IS NOT NULL
       AND UPPER(LTRIM(RTRIM(status))) NOT IN ('ACTIVE', 'INACTIVE')
   )
   OR updated_at IS NULL
   OR updated_at > GETDATE();


/* ============================================================
   6. DUPLICATE INSURANCE PLAN IDs
   ============================================================ */

SELECT
    '06 - Duplicate Insurance Plan IDs' AS validation_check,
    insurance_plan_id,
    COUNT(*) AS record_count
FROM dbo.insurance_plans
WHERE insurance_plan_id IS NOT NULL
GROUP BY insurance_plan_id
HAVING COUNT(*) > 1
ORDER BY insurance_plan_id;
