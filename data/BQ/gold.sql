-- 1. Total Charge Amount per provider by department.
CREATE OR REPLACE TABLE `duynp20-project.gold.total_charge_amount_per_provider_by_department` AS
SELECT 
    p.provider_key,
    p.firstname AS provider_first_name,
    p.lastname AS provider_last_name,
    p.specialization,
    d.dept_name,
    p.datasource,
    ROUND(SUM(t.amount), 2) AS total_charge_amount,
    COUNT(t.transaction_key) AS total_transactions,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM `duynp20-project.silver.fact_transactions` t
JOIN `duynp20-project.silver.dim_providers` p 
ON t.provider_key = p.provider_key
JOIN `duynp20-project.silver.dim_departments` d 
ON t.dept_key = d.dept_key
WHERE p.is_current = TRUE
GROUP BY 
  p.provider_key,
  provider_first_name,
  provider_last_name,
  p.specialization,
  d.dept_name,
  p.datasource;

-- 2. Patient History: This table provides a complete history of a patient’s visits, diagnoses, and financial interactions
CREATE OR REPLACE TABLE `duynp20-project.gold.patient_history` AS
SELECT 
    p.patient_key,
    p.source_patient_id,
    CONCAT(p.firstname, ' ', p.lastname) AS patient_full_name,
    p.gender,
    p.dob AS date_of_birth,
    e.encounter_key,
    e.encounter_date,
    e.encountertype AS visit_type,
    prov.firstname || ' ' || prov.lastname AS provider_name,
    dept.dept_name AS department_name,
    t.icdcode AS diagnosis_code,
    t.procedurecode,
    t.amount AS charged_amount,
    t.paidamount AS amount_paid,
    t.paid_date,
    t.amounttype AS payment_method,
    p.datasource,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM `duynp20-project.silver.dim_patients` p
JOIN `duynp20-project.silver.fact_encounters` e 
ON p.patient_key = e.patient_key
LEFT JOIN `duynp20-project.silver.fact_transactions` t 
ON e.encounter_key = t.encounter_key
LEFT JOIN `duynp20-project.silver.dim_providers` prov 
ON e.provider_key = prov.provider_key
LEFT JOIN `duynp20-project.silver.dim_departments` dept 
ON e.dept_key = dept.dept_key
WHERE p.is_current = TRUE;

-- 3. Provider Performance Summary (Gold) : This table summarizes provider activity, including the number of encounters, total billed amount, and claim success rate.
CREATE OR REPLACE TABLE `duynp20-project.gold.provider_performance_summary` AS
WITH provider_encounters AS (
    SELECT 
        provider_key,
        COUNT(DISTINCT encounter_key) AS total_encounters
    FROM `duynp20-project.silver.fact_encounters`
    GROUP BY provider_key
),

provider_financials AS (
    SELECT 
        provider_key,
        ROUND(SUM(CAST(amount AS FLOAT64)), 2) AS total_billed_amount
    FROM `duynp20-project.silver.fact_transactions`
    GROUP BY provider_key
),

provider_claims AS (
    SELECT 
        provider_key,
        COUNT(claimid) AS total_claims,
        COUNTIF(claimstatus = 'Approved') AS approved_claims,
        ROUND((COUNTIF(claimstatus = 'Approved') / COUNT(claimid)) * 100,2) AS claim_success_rate
    FROM `duynp20-project.silver.fact_claims`
    GROUP BY provider_key
)

SELECT 
    p.provider_key,
    p.firstname AS provider_first_name,
    p.lastname AS provider_last_name,
    p.specialization,
    p.datasource,
    COALESCE(e.total_encounters, 0) AS total_encounters,
    COALESCE(f.total_billed_amount, 0) AS total_billed_amount,
    COALESCE(c.total_claims, 0) AS total_claims,
    COALESCE(c.approved_claims, 0) AS total_approved_claims,
    COALESCE(c.claim_success_rate, 0) AS claim_success_rate_percentage,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM `duynp20-project.silver.dim_providers` p
LEFT JOIN provider_encounters e 
ON p.provider_key = e.provider_key
LEFT JOIN provider_financials f 
ON p.provider_key = f.provider_key
LEFT JOIN provider_claims c 
ON p.provider_key = c.provider_key
WHERE p.is_current = TRUE;

-- 4. Department Performance Analytics (Gold): Provides insights into department-level efficiency, revenue, and patient volume.
CREATE OR REPLACE TABLE `duynp20-project.gold.department_performance_analytics` AS
WITH dept_encounters AS (
    SELECT 
        dept_key,
        COUNT(DISTINCT encounter_key) AS total_encounters,
        COUNT(DISTINCT patient_key) AS unique_patients
    FROM `duynp20-project.silver.fact_encounters`
    GROUP BY dept_key
),

dept_finance AS (
    SELECT 
        dept_key,
        ROUND(SUM(CAST(amount AS FLOAT64)), 2) AS total_billed_amount,
        ROUND(SUM(CAST(paidamount AS FLOAT64)), 2) AS total_revenue_collected
    FROM `duynp20-project.silver.fact_transactions`
    GROUP BY dept_key
),

dept_staffing AS (
    SELECT 
        dept_key,
        COUNT(DISTINCT provider_key) AS total_providers
    FROM `duynp20-project.silver.dim_providers`
    WHERE is_current = TRUE
    GROUP BY dept_key
)

SELECT 
    d.dept_key,
    d.dept_name,
    d.datasource,
    COALESCE(e.unique_patients, 0) AS patient_volume,
    COALESCE(e.total_encounters, 0) AS total_encounters,
    COALESCE(f.total_billed_amount, 0) AS total_billed_amount,
    COALESCE(f.total_revenue_collected, 0) AS total_revenue,
    COALESCE(s.total_providers, 0) AS provider_count,
    ROUND((e.total_encounters / s.total_providers), 2) AS avg_encounters_per_provider,
    ROUND((f.total_revenue_collected / f.total_billed_amount) * 100, 2) AS collection_rate_percentage,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM `duynp20-project.silver.dim_departments` d
LEFT JOIN dept_encounters e 
ON d.dept_key = e.dept_key
LEFT JOIN dept_finance f 
ON d.dept_key = f.dept_key
LEFT JOIN dept_staffing s 
ON d.dept_key = s.dept_key;

-- 5. Financial Metrics: Aggregates financial KPIs, such as total revenue, claim success rate, and outstanding balances.
CREATE OR REPLACE TABLE `duynp20-project.gold.financial_metrics` AS
WITH financial_base AS (
    SELECT 
        datasource,
        LAST_DAY(visit_date) AS month_end_date,
        ROUND(SUM(CAST(amount AS FLOAT64)), 2) AS total_billed_amount,
        ROUND(SUM(CAST(paidamount AS FLOAT64)), 2) AS total_revenue,
        COUNT(DISTINCT transaction_key) AS transaction_count
    FROM `duynp20-project.silver.fact_transactions`
    GROUP BY datasource, month_end_date
),

claim_base AS (
    SELECT 
        datasource,
        LAST_DAY(servicedate) AS month_end_date,
        COUNT(claimid) AS total_claims,
        COUNTIF(claimstatus = 'Approved') AS approved_claims,
        ROUND(SUM(CAST(claimamount AS FLOAT64)), 2) AS total_claim_amount
    FROM `duynp20-project.silver.fact_claims`
    GROUP BY datasource, month_end_date
)

SELECT 
    COALESCE(f.datasource, c.datasource) AS datasource,
    COALESCE(f.month_end_date, c.month_end_date) AS reporting_month,
    f.total_revenue,
    f.total_billed_amount,
    f.transaction_count,
    ROUND(f.total_billed_amount - f.total_revenue, 2) AS outstanding_balance,
    c.total_claims,
    c.approved_claims,
    ROUND((c.approved_claims / c.total_claims) * 100, 2) AS claim_success_rate_percentage,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM financial_base f
FULL OUTER JOIN claim_base c 
ON f.datasource = c.datasource AND f.month_end_date = c.month_end_date
ORDER BY reporting_month DESC, datasource;
