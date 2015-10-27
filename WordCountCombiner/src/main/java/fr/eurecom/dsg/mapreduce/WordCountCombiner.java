package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 *
 */
public class WordCountCombiner extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountCombiner(args), args);
        System.exit(res);
    }

    public WordCountCombiner (String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: WordCountCombiner <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "Word Count");

        // Set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set Map Class
        job.setMapperClass(WCMapperCombiner.class);
        // Set the map output key
        job.setMapOutputKeyClass(Text.class);
        // Set the value classes
        job.setMapOutputValueClass(LongWritable.class);

        // Set combiner class
        job.setCombinerClass(WCReducerCombiner.class);

        // Set reduce class
        job.setReducerClass(WCReducerCombiner.class);
        // Set the reduce output key
        job.setOutputKeyClass(Text.class);
        // Set the value classes
        job.setOutputValueClass(LongWritable.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // Add the input file as job input (from HDFS) to the variable inputPath
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // Set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // Set the number of reducers using variable numberReducers
        job.setNumReduceTasks(Integer.parseInt(args[0]));
        // TODO: set the jar class
        job.setJarByClass(WordCountCombiner.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

}

class WCMapperCombiner extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text word = new Text();
    private final static LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
        StringTokenizer iter = new StringTokenizer(text.toString());

        while (iter.hasMoreTokens()) {
            this.word.set(iter.nextToken());
            context.write(this.word, one);
        }
    }

}

class WCReducerCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text word, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long accumulator = 0;

        for (LongWritable value : values)
            accumulator += value.get();

        context.write(word, new LongWritable(accumulator));
    }

}