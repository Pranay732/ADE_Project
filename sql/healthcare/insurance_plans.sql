SELECT
    TRY_CAST(insurance_plan_id AS INT) AS insurance_plan_id,
    CAST(insurance_company AS STRING) AS insurance_company,
    CAST(plan_name AS STRING) AS plan_name,
    CAST(plan_type AS STRING) AS plan_type,
    TRY_CAST(coverage_percentage AS DECIMAL(5, 2)) AS coverage_percentage,
    TRY_CAST(annual_limit AS DECIMAL(12, 2)) AS annual_limit,
    CAST(status AS STRING) AS status,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM insurance_plans_raw;