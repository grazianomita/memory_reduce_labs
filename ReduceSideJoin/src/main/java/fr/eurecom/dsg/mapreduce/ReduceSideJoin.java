package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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


public class ReduceSideJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputPath;
    private int numReducers;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        Job job = new Job (conf);
        job.setJobName("ReduceSideJoin");

        // Set input format class
        job.setInputFormatClass(TextInputFormat.class);

        // Set map class and the map output key and value classes
        job.setMapperClass(RSMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextPair.class);

        // Set reduce class and the reduce output key and value classes
        job.setReducerClass(RSReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // Input file as job input (from HDFS) to the variable inputFile
        FileInputFormat.setInputPaths(job, inputPath);
        // Set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, outputDir);
        // Set the number of reducers using variable numberReducers
        job.setNumReduceTasks(numReducers);
        // Set the jar class
        job.setJarByClass(ReduceSideJoin.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public ReduceSideJoin(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: ReduceSideJoin <num_reducers> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(args), args);
        System.exit(res);
    }

}

class RSMap extends Mapper<LongWritable, Text, Text, TextPair> {

    static final public String LEFT = new String("0");
    static final public String RIGHT = new String("1");

    private Text joinA = new Text();
    private TextPair v = new TextPair();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer st= new StringTokenizer(value.toString());

        while(st.hasMoreTokens()) {
            String a = st.nextToken();
            String b = st.nextToken();

            joinA.set(b);
            v.setFirst(a);
            v.setSecond(LEFT);
            context.write(joinA, v);

            joinA.set(a);
            v.setFirst(b);
            v.setSecond(RIGHT);
            context.write(joinA, v);
        }
    }

}

class RSReduce extends Reducer<Text, TextPair, Text, Text> {

    private Text outR = new Text();

    @Override
    protected void reduce(Text joinAttr, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
        List<Text> users = new ArrayList<Text>();
        List<Text> friendsOfFriends = new ArrayList<Text>();

        for (TextPair value : values) {
            // LEFT TAG
            if (value.getSecond().toString().compareTo("0") == 0)
                users.add(value.getFirst());
            else //RIGHT TAG
                friendsOfFriends.add(value.getFirst());
        }

        for (Text user : users) {
            for (Text friendOfFriend : friendsOfFriends)
                context.write(user, friendOfFriend);
        }
    }

}
