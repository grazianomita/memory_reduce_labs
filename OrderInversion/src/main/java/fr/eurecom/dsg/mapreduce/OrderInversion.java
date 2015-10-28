package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class OrderInversion extends Configured implements Tool {

    protected final static String ASTERISK = "\0";
    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
        System.exit(res);
    }

    public OrderInversion(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: OrderInversion <num_reducers> <input_file> <output_dir>");
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
        job.setMapperClass(PairMapper.class);
        // Set the map output key
        job.setMapOutputKeyClass(TextPair.class);
        // Set the value classes
        job.setMapOutputValueClass(IntWritable.class);

        // Set reduce class
        job.setReducerClass(PairReducer.class);
        // Set the reduce output key
        job.setOutputKeyClass(TextPair.class);
        // Set the value classes
        job.setOutputValueClass(DoubleWritable.class);

        // TODO: Set the partitioner class
        job.setPartitionerClass(PartitionerTextPair.class);
        // TODO: Set sort comparator class
        job.setSortComparatorClass(TextPair.Comparator.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // Add the input file as job input (from HDFS) to the variable inputPath
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // Set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // Set the number of reducers using variable numberReducers
        job.setNumReduceTasks(Integer.parseInt(args[0]));
        // Set the jar class
        job.setJarByClass(OrderInversion.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class PartitionerTextPair extends Partitioner<TextPair, IntWritable> {

        @Override
        public int getPartition(TextPair key, IntWritable value, int numPartitions) {
            return toUnsigned(key.getFirst().hashCode()) % numPartitions;
        }

        public static int toUnsigned(int val) {
            return val & Integer.MAX_VALUE;
        }

    }

    public static class PairMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

        private TextPair tp = new TextPair();
        private IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String line = value.toString();
            line = line.replaceAll("[^a-zA-Z0-9_]+]", " ");
            String[] tokens = line.split("\\s+");
            if (tokens.length > 1) {
                //For each word
                for (int i=0; i<tokens.length; i++) {
                    tp.setFirst(tokens[i]);
                    //Explore the neighborhood (the whole Text value)
                    for (int j=0; j<tokens.length; j++) {
                        if (j == i)
                            continue;
                        if (tokens[i].compareTo(tokens[j]) == 0)
                            continue;
                        tp.setSecond(tokens[j]);
                        context.write(tp, one);
                    }
                    context.write(new TextPair(tokens[i], ASTERISK), new IntWritable(tokens.length));
                }
            }

        }

    }

    public static class PairReducer extends Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {

        private int marginal;
        private int sum;

        @Override
        public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
            if (key.getSecond().toString().compareTo(ASTERISK) == 0) {
                marginal = 0;
                for (IntWritable value : values)
                    marginal += value.get();
            }
            else {
                sum = 0;
                for (IntWritable value : values)
                    sum += value.get();
                context.write(key, new DoubleWritable((double)sum/marginal));
            }
        }

    }

}
