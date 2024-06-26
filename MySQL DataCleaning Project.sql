-- Data Cleaning Project- MySQL

-- Creating schema
Create database world_layoffs;
use world_layoffs;
-- Creating new table 
CREATE TABLE layoffs_staging LIKE layoffs_raw;
-- inserting the values into new table from raw dataset
Insert into layoffs_staging
Select * from layoffs_raw;

-- Data Cleaning Steps
-- Step 1: Remove all the duplicates
-- Step 2: Standardize the data
-- Step 3: Working with null values- Populating the null or blank values or deleting them
-- Step 4: Remove  any rows or columns which are not required

-- Step 1- Removing the duplicate rows

-- Using row_number to identify the duplicates in the table
With row_CTE as(
Select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging)
Select * from row_CTE
where row_num>1;

-- Creating another new table called layoffs2 where row number is another attrinute , so it gets easier to remove duplicates by using row_numbers 
CREATE TABLE `layoffs2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

Insert into layoffs2 
Select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

DELETE FROM layoffs2 
WHERE
    row_num > 1;


SELECT 
    *
FROM
    layoffs2;

-- Step 2: Standardization

SELECT 
    company, TRIM(company)
FROM
    layoffs2;

UPDATE layoffs2 
SET 
    company = TRIM(company);
SELECT 
    company
FROM
    layoffs2;

-- Removing the typo in location here we correct dusseldorf
SELECT DISTINCT
    location
FROM
    layoffs2
ORDER BY 1;
SELECT DISTINCT
    *
FROM
    layoffs2
WHERE
    location LIKE 'DÃ¼sseldorf'
        OR location = 'Dusseldorf'

Update layoffs2 set location= 'Dusseldorf'
where location='DÃ¼sseldorf';

-- Removing duplicate industry
select distinct industry from layoffs2
order by 1;

-- Updating the industry with synonymous name like Crypto, Crypto currency etc to a common single name
Update layoffs2 set industry='Crypto'
where industry like 'Crypto%';
-- Removing any duplicate countries or removing periods(.) etc 
select distinct country from layoffs2
order by 1;

update layoffs2 set country= trim(trailing '.' from country)
where country like 'United States%';

-- Converting the date column which is in string format to date format

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs2;

update layoffs2 set date= str_to_date(`date`, '%m/%d/%Y');

-- Converting the data type of date into datetime format
Alter table layoffs2 modify column `date` date; 


-- Step 3: Working with null or blank values

-- Populating the null or blank values

-- Updating all the blank values to null to make it easier to populate
Update layoffs2 set industry= null where industry='';

Select t1.industry, t2.industry
from layoffs2 t1
join layoffs2 t2 on t1.company=t2.company
where (t1.industry is null or t1.industry='') and t2.industry is not null ;

Update layoffs2 t1 
join layoffs2 t2 on t1.company=t2.company
set t1.industry=t2.industry
where (t1.industry is null or t1.industry='') and t2.industry is not null;

-- Step 4: Removing rows and columns that are not required 

/* For EDA we need total_laid_off and percentage_laid_off and data for this not available, 
therefore deleting the rows where both the total_laid_off and percentage_laid_off is null 
*/
select * 
from layoffs2 where total_laid_off is null and percentage_laid_off is null;

Delete
from layoffs2 where total_laid_off is null and percentage_laid_off is null;

-- Removing the row_num column as it is not required
alter table layoffs2 drop column row_num;
