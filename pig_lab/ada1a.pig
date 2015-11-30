/* For each airport code compute the number of inbound, outbound and all 
   flights. */

-- Set default parameters
%default input		'/laboratory/airlines'
%default output 	'./sample-output/ADA_1A'
%default N			20

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

-- Group by origin airport code
outbound_flights = GROUP dataset BY (scode);

-- Calculate the number of inbound, outbound and all flights
of_counts = FOREACH outbound_flights GENERATE group AS scode, COUNT(outbound_flights) AS total_outbound;

-- Group by destination airport code
inbound_flights = GROUP dataset BY (dcode);

-- Calculate the number of inbound, outbound and all flights
if_counts = FOREACH inbound_flights GENERATE group AS dcode, COUNT(inbound_flights) AS total_inbound;

-- Join the two previous results in order to obtain the following schema
-- (airport_code, total_outbound, total_inbound)
join_result = JOIN of_counts BY scode, if_counts BY dcode;

total_result = FOREACH join_result GENERATE 
					scode AS code,
					total_outbound, 
					total_inbound, 
					total_outbound + total_inbound AS total;

-- Sort total_result
sorted_result = ORDER total_result BY total DESC;

-- Return top N airports
topN = LIMIT sorted_result 'N';

-- Store the output (and start to execute the script)
STORE topN INTO 'output';