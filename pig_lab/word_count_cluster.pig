-- Load input data from local input directory
A = LOAD '/laboratory/gutenberg_small.txt';

-- Parse and clean input data
B = FOREACH A GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;
C = FILTER B BY word MATCHES '\\w+';

-- Explicit the GROUP-BY / SHUFFLE Phase
D = GROUP C BY word;

-- Generate output data in the form: <word, counts>
E = FOREACH D GENERATE group, COUNT(C);

-- Store output data in local output directory
store E into './sample-output/WORD_COUNT/';
