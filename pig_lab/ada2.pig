/* Some carriers come and go, others demonstrate regular growth. Compute the 
   (log base 10) volume -- total flights -- over each year, by carrier. 
   The carriers are ranked by their median volume (over the 4 year span).
*/

-- Set default parameters
%default input		'/laboratory/airlines'
%default output 	'./sample-output/ADA_2'

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

-- Group by carrier and year
grouped_by_year_carriers = GROUP dataset BY (carrier, year);

-- For each carrier-year couple calculate the total flights number
carrier_counts = FOREACH grouped_by_year_carriers GENERATE 
					FLATTEN(group) AS (carrier, year),
					LOG10(COUNT(dataset)) AS total_flights;

-- Now, group by carrier only
grouped_by_carrier = GROUP carrier_counts BY carrier;

-- Perform the average_flights number = #total_flights/#years
averages = FOREACH grouped_by_carrier GENERATE 
					group AS carrier,
					AVG(carrier_counts.total_flights) AS average_fligths;

-- Sort result by average_flights
result = ORDER averages BY average_fligths DESC;

-- Store the output (and start to execute the script)
STORE result INTO '$output';