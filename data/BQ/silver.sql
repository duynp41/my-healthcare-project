-- Create dim_departments table
CREATE OR REPLACE TABLE `duynp20-project.silver.dim_departments` AS
SELECT 
    CONCAT('HOSP-A-', deptid) AS dept_key,
    deptid AS dept_id,
    name AS dept_name,
    'hospital-a' AS datasource,
    CURRENT_TIMESTAMP() AS ingestion_at
FROM `duynp20-project.bronze.hospital_a_departments`

UNION ALL

SELECT 
    CONCAT('HOSP-B-', deptid) AS dept_key,
    deptid AS dept_id,
    name AS dept_name,
    'hospital-b' AS datasource,
    CURRENT_TIMESTAMP() AS ingestion_at
FROM `duynp20-project.bronze.hospital_b_departments`;

-- Create fact_encounters table
CREATE OR REPLACE TABLE `duynp20-project.silver.fact_encounters` AS
SELECT 
    CONCAT('HOSP-A-', encounterid) AS encounter_key,  
    CONCAT('HOSP-A-', patientid) AS patient_key,     
    CONCAT('HOSP-A-', departmentid) AS dept_key,      
    CONCAT('HOSP-A-', providerid) AS provider_key,    
    encounterid,
    encountertype,
    procedurecode,
    DATE(TIMESTAMP_MILLIS(encounterdate)) AS encounter_date,
    DATE(TIMESTAMP_MILLIS(inserteddate)) AS inserted_date,
    DATE(TIMESTAMP_MILLIS(modifieddate)) AS modified_date,
    'hospital-a' AS datasource,
    CURRENT_TIMESTAMP() AS ingestion_at
FROM `duynp20-project.bronze.hospital_a_encounters`

UNION ALL

SELECT 
    CONCAT('HOSP-B-', encounterid) AS encounter_key,
    CONCAT('HOSP-B-', patientid) AS patient_key,
    CONCAT('HOSP-B-', departmentid) AS dept_key,
    CONCAT('HOSP-B-', providerid) AS provider_key,
    encounterid,
    encountertype,
    procedurecode,
    DATE(TIMESTAMP_MILLIS(encounterdate)) AS encounter_date,
    DATE(TIMESTAMP_MILLIS(inserteddate)) AS inserted_date,
    DATE(TIMESTAMP_MILLIS(modifieddate)) AS modified_date,
    'hospital-b' AS datasource,
    CURRENT_TIMESTAMP() AS ingestion_at
FROM `duynp20-project.bronze.hospital_b_encounters`;

-- Create dim_patients table
CREATE OR REPLACE TABLE `duynp20-project.silver.dim_patients` AS
WITH source_data AS (
    SELECT 
        CONCAT('HOSP-A-', patientid) AS patient_key,
        patientid AS source_patient_id,
        firstname,
        lastname,
        middlename,
        ssn,
        phonenumber,
        gender,
        DATE(TIMESTAMP_MILLIS(dob)) AS dob,
        address,
        DATE(TIMESTAMP_MILLIS(modifieddate)) AS modified_date,
        'hospital-a' AS datasource
    FROM `duynp20-project.bronze.hospital_a_patients`

    UNION ALL

    SELECT 
        CONCAT('HOSP-B-', id) AS patient_key,
        id AS source_patient_id,
        f_name AS firstname,
        l_name AS lastname,
        m_name AS middlename,
        ssn,
        phonenumber,
        gender,
        DATE(TIMESTAMP_MILLIS(dob)) AS dob,
        address,
        DATE(TIMESTAMP_MILLIS(modifieddate)) AS modified_date,
        'hospital-b' AS datasource
    FROM `duynp20-project.bronze.hospital_b_patients`
)
SELECT 
    patient_key,
    source_patient_id,
    firstname,
    lastname,
    middlename,
    ssn,
    phonenumber,
    gender,
    dob,
    address,
    datasource,
    modified_date AS valid_from,
    CAST(NULL AS DATE) AS valid_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS ingestion_at
FROM source_data;

-- Create dim_providers table
CREATE OR REPLACE TABLE `duynp20-project.silver.dim_providers` AS
WITH source_providers AS (
    SELECT 
        CONCAT('HOSP-A-', REGEXP_EXTRACT(providerid, r'PROV.*')) AS provider_key, 
        CONCAT('HOSP-A-', deptid) AS dept_key,         
        providerid AS source_provider_id,
        firstname,
        lastname,
        specialization,
        npi,
        'hospital-a' AS datasource,
        CURRENT_DATE() AS valid_from
    FROM `duynp20-project.bronze.hospital_a_providers`

    UNION ALL

    SELECT 
        CONCAT('HOSP-B-', REGEXP_EXTRACT(providerid, r'PROV.*')) AS provider_key,
        CONCAT('HOSP-B-', deptid) AS dept_key,
        providerid AS source_provider_id,
        firstname,
        lastname,
        specialization,
        npi,
        'hospital-b' AS datasource,
        CURRENT_DATE() AS valid_from
    FROM `duynp20-project.bronze.hospital_b_providers`
)
SELECT 
    provider_key,
    dept_key,
    source_provider_id,
    firstname,
    lastname,
    specialization,
    npi,
    datasource,
    valid_from,
    CAST(NULL AS DATE) AS valid_to, 
    TRUE AS is_current,          
    CURRENT_TIMESTAMP() AS ingestion_at
FROM source_providers;

-- Create fact_transactions table
CREATE OR REPLACE TABLE `duynp20-project.silver.fact_transactions` AS
SELECT 
    CONCAT('HOSP-A-', transactionid) AS transaction_key, 
    CONCAT('HOSP-A-', encounterid) AS encounter_key,     
    CONCAT('HOSP-A-', patientid) AS patient_key,         
    CONCAT('HOSP-A-', providerid) AS provider_key,       
    CONCAT('HOSP-A-', deptid) AS dept_key,               
    DATE(TIMESTAMP_MILLIS(visitdate)) AS visit_date,
    DATE(TIMESTAMP_MILLIS(servicedate)) AS service_date,
    DATE(TIMESTAMP_MILLIS(paiddate)) AS paid_date,
    DATE(TIMESTAMP_MILLIS(insertdate)) AS insert_date,
    DATE(TIMESTAMP_MILLIS(modifieddate)) AS modified_date,
    visittype,
    amount,
    amounttype,
    paidamount,
    claimid,
    payorid,
    procedurecode,
    icdcode,
    lineofbusiness,
    medicaidid,
    medicareid,
    'hospital-a' AS datasource,
    CURRENT_TIMESTAMP() AS ingestion_at
FROM `duynp20-project.bronze.hospital_a_transactions`

UNION ALL

SELECT 
    CONCAT('HOSP-B-', transactionid) AS transaction_key,
    CONCAT('HOSP-B-', encounterid) AS encounter_key,
    CONCAT('HOSP-B-', patientid) AS patient_key,
    CONCAT('HOSP-B-', providerid) AS provider_key,
    CONCAT('HOSP-B-', deptid) AS dept_key,
    DATE(TIMESTAMP_MILLIS(visitdate)) AS visit_date,
    DATE(TIMESTAMP_MILLIS(servicedate)) AS service_date,
    DATE(TIMESTAMP_MILLIS(paiddate)) AS paid_date,
    DATE(TIMESTAMP_MILLIS(insertdate)) AS insert_date,
    DATE(TIMESTAMP_MILLIS(modifieddate)) AS modified_date,
    visittype,
    amount,
    amounttype,
    paidamount,
    claimid,
    payorid,
    procedurecode,
    icdcode,
    lineofbusiness,
    medicaidid,
    medicareid,
    'hospital-b' AS datasource,
    CURRENT_TIMESTAMP() AS ingestion_at
FROM `duynp20-project.bronze.hospital_b_transactions`;

-- Create dim_cptcodes table
CREATE OR REPLACE TABLE `duynp20-project.silver.dim_cptcodes` AS
SELECT 
    cpt_codes,
    procedure_code_descriptions,
    procedure_code_category,
    code_status,
    extraction_timestamp
FROM `duynp20-project.bronze.cptcodes`;

-- Create fact_claims table
CREATE OR REPLACE TABLE `duynp20-project.silver.fact_claims` AS
SELECT 
    claimid,
    CONCAT(IF(source_file_path LIKE '%hospital1%', 'HOSP-A-', 'HOSP-B-'), transactionid) AS transaction_key,
    CONCAT(IF(source_file_path LIKE '%hospital1%', 'HOSP-A-', 'HOSP-B-'), patientid) AS patient_key,
    CONCAT(IF(source_file_path LIKE '%hospital1%', 'HOSP-A-', 'HOSP-B-'), encounterid) AS encounter_key,
    CONCAT(IF(source_file_path LIKE '%hospital1%', 'HOSP-A-', 'HOSP-B-'), providerid) AS provider_key,
    CONCAT(IF(source_file_path LIKE '%hospital1%', 'HOSP-A-', 'HOSP-B-'), deptid) AS dept_key,
    servicedate,
    claimdate,
    payorid,
    claimamount,
    paidamount,
    claimstatus,
    payortype,
    deductible,
    coinsurance,
    copay,
    insertdate,
    modifieddate,
    source_file_path,
    IF(source_file_path LIKE '%hospital1%', 'hospital-a', 'hospital-b') AS datasource,
    extraction_timestamp AS ingestion_at
FROM `duynp20-project.bronze.claims`;