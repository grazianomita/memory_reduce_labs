package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Pair extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Pair(args), args);
        System.exit(res);
    }

    public Pair(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Pair <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "Word Co-occurrence");

        // Set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set Map Class
        job.setMapperClass(PairMapper.class);
        // Set the map output key
        job.setMapOutputKeyClass(Text.class);
        // Set the value classes
        job.setMapOutputValueClass(IntWritable.class);

        // Set reduce class
        job.setReducerClass(PairReducer.class);
        // Set the reduce output key
        job.setOutputKeyClass(Text.class);
        // Set the value classes
        job.setOutputValueClass(IntWritable.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // Add the input file as job input (from HDFS) to the variable inputPath
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // Set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // Set the number of reducers using variable numberReducers
        job.setNumReduceTasks(Integer.parseInt(args[0]));
        // Set the jar class
        job.setJarByClass(Pair.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

    public static class PairMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

        private TextPair tp = new TextPair();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Split Text value into tokens and save it into String[] tokens.
            String[] tokens = value.toString().split("\\s+");
            if (tokens.length > 1) {
                //For each word
                for (int i=0; i<tokens.length; i++) {
                    tp.setFirst(tokens[i]);
                    //Explore the neighborhood (the whole Text value)
                    for (int j = 0; j<tokens.length; j++) {
                        if (j == i)
                            continue;
                        if (tokens[i].compareTo(tokens[j]) == 0)
                            continue;
                        tp.setSecond(tokens[j]);
                        context.write(tp, one);
                    }
                }
            }
        }

    }

    public static class PairReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values)
                sum += value.get();

            context.write(key, new IntWritable(sum));
        }

    }

}
