-- Select all records and order them by filename and pickup date
SELECT * FROM green_tripdata
ORDER BY filename, DATE(lpep_pickup_datetime);

-- Count frequency of each pickup and dropoff date
SELECT trip_date, COUNT(*) AS frequency
FROM (
	SELECT DATE(lpep_pickup_datetime) AS trip_date FROM green_tripdata
	UNION ALL
	SELECT DATE(lpep_dropoff_datetime) AS trip_date FROM green_tripdata
) AS all_dates
GROUP BY trip_date
ORDER BY trip_date;

-- Get distinct filenames
SELECT DISTINCT filename
FROM green_tripdata
WHERE filename IS NOT NULL
ORDER BY filename;

-- Select records from July 2021
SELECT * FROM green_tripdata
WHERE filename = 'green_tripdata_2021-07.csv';


-- Copy outliers records in an error table
CREATE TABLE green_tripdata_errors (LIKE green_tripdata INCLUDING ALL);
SELECT * FROM green_tripdata_errors; -- confirm shema

-- Insert outliers (dates out of range)
INSERT INTO green_tripdata_errors
SELECT *
FROM green_tripdata
WHERE DATE(lpep_pickup_datetime) IN (
    '2008-10-21', '2008-12-31', '2009-01-01', '2009-01-02', '2009-01-04',
	'2009-01-05', '2010-09-23', '2010-09-24', '2018-03-07', '2018-09-13',
	'2018-12-04', '2018-12-05', '2018-12-06', '2018-12-07', '2018-12-08',
	'2018-12-21', '2018-12-31','2021-08-01', '2035-09-02', '2041-08-17',
	'2062-08-15'

)
OR DATE(lpep_dropoff_datetime) IN (
    '2008-10-21', '2008-12-31', '2009-01-01', '2009-01-02', '2009-01-04',
	'2009-01-05', '2010-09-23', '2010-09-24', '2018-03-07', '2018-09-13',
	'2018-12-04', '2018-12-05', '2018-12-06', '2018-12-07', '2018-12-08',
	'2018-12-21', '2018-12-31','2021-08-01', '2035-09-02', '2041-08-17',
	'2062-08-15'

);

-- Delete outliers from the final table
DELETE FROM green_tripdata
WHERE DATE(lpep_pickup_datetime) IN (
    '2008-10-21', '2008-12-31', '2009-01-01', '2009-01-02', '2009-01-04',
	'2009-01-05', '2010-09-23', '2010-09-24', '2018-03-07', '2018-09-13',
	'2018-12-04', '2018-12-05', '2018-12-06', '2018-12-07', '2018-12-08',
	'2018-12-21', '2018-12-31','2021-08-01', '2035-09-02', '2041-08-17',
	'2062-08-15'
)
OR DATE(lpep_dropoff_datetime) IN (
    '2008-10-21', '2008-12-31', '2009-01-01', '2009-01-02', '2009-01-04',
	'2009-01-05', '2010-09-23', '2010-09-24', '2018-03-07', '2018-09-13',
	'2018-12-04', '2018-12-05', '2018-12-06', '2018-12-07', '2018-12-08',
	'2018-12-21', '2018-12-31','2021-08-01', '2035-09-02', '2041-08-17',
	'2062-08-15'
);