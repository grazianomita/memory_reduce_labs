SET DEFAULT_PARALLEL 10;

-- Load input data from local input directory
-- The input file is loaded twice in order to perform the join
datasetA = LOAD '/laboratory/twitter-big.txt' AS (id: long, fr: long);
datasetB = LOAD '/laboratory/twitter-big.txt' AS (id: long, fr: long);

-- For both datasets, check is user IDs are valid and clean the dataset
clean_datasetA = FILTER datasetA BY $1 is not null;
clean_datasetB = FILTER datasetB BY $1 is not null;

-- Compute all the two-hop paths 
twohop = JOIN clean_datasetA by $1, clean_datasetB by $0;

-- datasetA		datasetB
-- 12 13		12 13
-- 12 14		12 14
-- 12 15		12 15
-- 13 16		13 16
-- 13 17		13 17
-- 14 12		14 12
-- 14 13		14 13
-- 14 15		14 15
-- 14 16		14 16
-- 16 17		16 17
--
-- OUTPUT: twohop
-- 12 13 13 16
-- 12 13 13 17
-- 12 14 14 12 *
-- 12 14 14 13
-- 12 14 14 15
-- 12 14 14 16
-- 13 16 16 17
-- 14 12 12 13
-- 14 12 12 14 *
-- 14 12 12 15
-- 14 13 13 16
-- 14 16 16 17
-- 15 13 13 17

p_result = FOREACH twohop GENERATE $0, $3;

-- OUTPUT: p_result
-- 12 16 duplicated
-- 12 17
-- 12 12 *
-- 12 13
-- 12 15
-- 12 16 duplicated
-- 13 17
-- 14 13
-- 14 14 *
-- 14 15
-- 14 16
-- 14 17
-- 15 17

-- Delete duplicated by using the clause DISTINCT
d_result = DISTINCT p_result;

-- Delete "self" results
result = FILTER d_result BY $0!=$1;

STORE result INTO './sample-output/TW_TWOHOP_JOIN/';