/* Which routes are the busiest? A simple first approach is 
   to create a frequency table for the unordered pair (i,j) 
   where i and j are distinct airport codes.
*/

-- Set default parameters
%default input		'/laboratory/airlines'
%default output 	'./sample-output/ADA_5'

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

-- Delete unused fields
reduced_dataset = FOREACH dataset GENERATE scode, dcode;
	
-- Group by origin/destination airport
-- The route (Brindisi, Torino) != (Torino, Brindisi)
groups = GROUP reduced_dataset by (scode, dcode);

-- For each scode/dcode couple, count the number of flights
result = FOREACH groups GENERATE
			FLATTEN(group) AS (scode, dcode),
			COUNT(reduced_dataset);

-- Store the output (and start to execute the script)
STORE result INTO '$output';