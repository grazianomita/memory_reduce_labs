package fr.eurecom.dsg.mapreduce;

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

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 *
 */
public class WordCount extends Configured implements Tool {

    private int     numReducers;
    private Path    inputPath;
    private Path    outputDir;

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(args), args);
        System.exit(res);
    }

    public WordCount (String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: WordCount <num_reducers> <input_path> <output_path>");
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
        job.setMapperClass(WCMapper.class);
        // Set the map output key
        job.setMapOutputKeyClass(Text.class);
        // Set the value classes
        job.setMapOutputValueClass(IntWritable.class);

        // Set reduce class
        job.setReducerClass(WCReducer.class);
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
        job.setJarByClass(WordCount.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

}

/* WCMapper takes as arguments:
    @LongWritable   Input Key Type      --> Equivalent to Long
    @Text           Input Value Type    --> Equivalent to String
    @Text           Output Key Type     --> Equivalent to String
    @IntWritable    Output Value Type   --> Equivalent to Integer
*/
class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }

}

/* WCReducer is called for each <key, list_of_values> pair in the
   grouped input. It takes as arguments:
    @Text           Input Key Type      --> Equivalent to String
    @IntWritable    Input Value Type    --> Equivalent to Integer
    @Text           Output Key Type     --> Equivalent to String
    @IntWritable    Output Value Type   --> Equivalent to Integer
*/
class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values)
            sum += value.get();

        context.write(key, new IntWritable(sum));
    }

}