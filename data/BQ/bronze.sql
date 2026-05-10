-- encounters hospital_a
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_a_encounters`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-a/encounters/*.json']
);

-- patients hospital_a
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_a_patients`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-a/patients/*.json']
);

-- transactions hospital_a
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_a_transactions`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-a/transactions/*.json']
);

-- providers (Full Load) hospital_a
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_a_providers`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-a/providers/*.json']
);

-- departments (Full Load)  hospital_a
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_a_departments`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-a/departments/*.json']
);

-- encounters hospital_b
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_b_encounters`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-b/encounters/*.json']
);

-- patients hospital_b
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_b_patients`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-b/patients/*.json']
);

-- transactions hospital_b
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_b_transactions`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-b/transactions/*.json']
);

-- providers (Full Load) hospital_b
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_b_providers`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-b/providers/*.json']
);

-- departments (Full Load)  hospital_b
CREATE OR REPLACE EXTERNAL TABLE `duynp20-project.bronze.hospital_b_departments`
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-duynp20/landing/hospital-b/departments/*.json']
);