-- Load input data from local input directory
dataset = LOAD '/laboratory/twitter-big.txt' AS (id: long, fr: long);

-- Check is user IDs are valid and clean the dataset
clean_dataset = FILTER dataset BY $1 is not null;

-- Organize data such that each node ID is associated to a list of neighbors
grouped_dataset = GROUP clean_dataset BY fr;

-- Foreach node ID generate an output relation consisting of the node ID and the number of friends he follows
following = FOREACH grouped_dataset GENERATE group, COUNT(clean_dataset) as following;

STORE following into './sample-output/TW_COUNT_FOLLOWING/';