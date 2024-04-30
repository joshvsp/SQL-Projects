-- Data Cleaning

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns

-- Had issues importing all rows as .csv file, so I converted file to .json
-- When I converted the csv file to json all the column data types turned into str
-- Converting two columns back to int whilst also accounting for NULL values 

-- Converting 'NULL' string values to actual NULL vallues
SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs SET total_laid_off=NULL 
WHERE total_laid_off='NULL';

UPDATE layoffs SET funds_raised_millions=NULL 
WHERE funds_raised_millions='NULL';

UPDATE layoffs SET percentage_laid_off=NULL 
WHERE percentage_laid_off='NULL';

-- Modifying columns datatype to INT
ALTER TABLE layoffs MODIFY funds_raised_millions INT;

ALTER TABLE layoffs MODIFY total_laid_off INT;

SELECT * 
FROM layoffs;


-- Creating copy to maintain raw data
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Copying raw data to new table
INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- Creating rows to identify duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;

-- Isolating duplicates

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
) 
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;

-- Checking

SELECT * 
FROM layoffs_staging;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- Creating new table to add new column for duplicate removal

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Inserting data into new table

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Deleting Duplicates
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Checking to see if duplicates were deleted
SELECT * 
FROM layoffs_staging2;

-- Standardizing

-- Trimming white space
SELECT DISTINCT(TRIM(company)) 
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Identifying redundant industries
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2; 

-- replacing NULLS
UPDATE layoffs_staging2 SET industry=NULL 
WHERE industry='NULL';

UPDATE layoffs_staging SET industry=NULL 
WHERE industry='NULL';

-- Checking Location

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

-- Checking Country

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Changing Date Type

UPDATE layoffs_staging2 SET date=NULL 
WHERE date='NULL';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE `layoffs_staging2`
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2 
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging2;

-- Dealing with NULL and blank values

-- Identifying rows with NULL values

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Setting blank rows as NULL values 

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Inserting industry value using matching company information

SELECT * 
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company= t2.company 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2;

-- Removing Unnecessary Rows/Columns

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;