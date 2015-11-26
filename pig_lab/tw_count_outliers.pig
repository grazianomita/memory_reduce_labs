-- Load input data from local input directory
dataset = LOAD './sample-input/OSN/tw.txt' AS (id: long, fr: long);

-- Check is user IDs are valid and clean the dataset
clean_dataset = FILTER dataset BY $1 is not null;

-- Organize data such that each node ID is associated to a list of neighbors
grouped_dataset = GROUP clean_dataset BY id;

-- Foreach node ID generate an output relation consisting of the node ID and the number of friends
friends = FOREACH grouped_dataset GENERATE group, COUNT(clean_dataset) AS followers;

-- Find the outliers
outliers = FILTER friends BY followers>3;

STORE outliers into './sample-output/TW_COUNT_OUTLIERS/';