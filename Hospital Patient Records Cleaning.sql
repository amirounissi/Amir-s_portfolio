/*
Cleaning Hospital Patient Data
Skills: UPDATE, ALTER TABLE, CASE, CTE, STRING_SPLIT, DATE functions
*/

-- View initial data
SELECT * FROM patient_records;

-- 1. Standardize admission dates
SELECT `Admission Date`, CAST(`Admission Date` AS DATE) AS standardized_date
FROM patient_records;

UPDATE patient_records 
SET `Admission Date` = CAST(`Admission Date` AS DATE);

-- 2. Fix patient gender codes
SELECT DISTINCT gender, COUNT(*)
FROM patient_records
GROUP BY gender;

UPDATE patient_records
SET gender = CASE 
    WHEN gender IN ('M', 'Male', 'MALE') THEN 'Male'
    WHEN gender IN ('F', 'Female', 'FEMALE') THEN 'Female'
    ELSE 'Unknown'
END;

-- 3. Split full address into components
SELECT 
    patient_address,
    SUBSTRING_INDEX(patient_address, ',', 1) AS street,
    SUBSTRING_INDEX(SUBSTRING_INDEX(patient_address, ',', 2), ',', -1) AS city,
    SUBSTRING_INDEX(patient_address, ',', -1) AS state
FROM patient_records;

ALTER TABLE patient_records
ADD COLUMN street VARCHAR(100),
ADD COLUMN city VARCHAR(50),
ADD COLUMN state VARCHAR(50);

UPDATE patient_records
SET 
    street = TRIM(SUBSTRING_INDEX(patient_address, ',', 1)),
    city = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(patient_address, ',', 2), ',', -1)),
    state = TRIM(SUBSTRING_INDEX(patient_address, ',', -1));

-- 4. Handle missing phone numbers
SELECT patient_id, phone_number
FROM patient_recuments
WHERE phone_number IS NULL OR phone_number = '';

UPDATE patient_records
SET phone_number = 'Not Provided'
WHERE phone_number IS NULL OR phone_number = '';

-- 5. Remove duplicate patient records
WITH DuplicateCTE AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id, first_name, last_name, date_of_birth
            ORDER BY admission_date DESC
        ) AS row_num
    FROM patient_records
)
DELETE FROM patient_records
WHERE (patient_id, first_name, last_name, date_of_birth) IN (
    SELECT patient_id, first_name, last_name, date_of_birth
    FROM DuplicateCTE
    WHERE row_num > 1
);
