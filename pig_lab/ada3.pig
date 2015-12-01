/* A flight is delayed if the delay is greater than 15 minutes. 
   Compute the fraction of delayed flights per different time 
   granularities (hour, day, week, month, year).
*/

-- Set default parameters
%default input		'/laboratory/airlines'
%default output 	'./sample-output/ADA_3'
%default N			15

-- Load input data from local input directory
dataset = LOAD '$input' using PigStorage(',') AS (year: int, month: int, day: int, dow: int,
	dtime: int, sdtime: int, arrtime: int, satime: int, 
	carrier: chararray, fn: int, tn: chararray, 
	etime: int, setime: int, airtime: int, 
	adelay: int, ddelay: int, 
	scode: chararray, dcode: chararray, dist: int, 
	tintime: int, touttime: int, 
	cancel: chararray, cancelcode: chararray, diverted: int, 
	cdelay: int, wdelay: int, ndelay: int, sdelay: int, latedelay: int);

-- Calculate delay for each flight
reduced_dataset = FOREACH dataset GENERATE
					year,
					month,
					day,
					dow,
					arrtime - satime AS delay;

-- Group By Year and Month (month granularity - Ex: 2010-01)
-- Grouping by different time fields we can obtain the result at
-- different time granularities.
groups = GROUP reduced_dataset BY (year, month);

-- For each year-month couple, calculate the delayed flights ratio
result = FOREACH groups {
			-- reduced_dataset is the field associated to the
			-- current processed group
			delays = FILTER reduced_dataset BY delay >= N;
			GENERATE group,
					COUNT(delays) AS delayed_flights_count,
					COUNT(reduced_dataset) AS flights_count,
					(float) COUNT(delays) / COUNT(reduced_dataset) AS fraction;
		}

-- Store the output (and start to execute the script)
STORE result INTO '$output';