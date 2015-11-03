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
import org.apache.hadoop.mapreduce.Partitioner;
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

        // TODO: define new job instead of null using conf e setting a name
        Configuration conf = this.getConf();

        Job job = new Job (conf);
        job.setJobName("ReduceSideJoin");

        // TODO: set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // TODO: set map class and the map output key and value classes
        job.setMapperClass(RSMap.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);

        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(RSReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set partitioner
        job.setPartitionerClass(RSPartitioner.class);

        // TODO: set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // TODO: add the input file as job input (from HDFS) to the variable
        //       inputFile
        FileInputFormat.setInputPaths(job, inputPath);

        // TODO: set the output path for the job results (to HDFS) to the variable
        //       outputPath
        FileOutputFormat.setOutputPath(job, outputDir);

        // TODO: set the number of reducers using variable numberReducers
        job.setNumReduceTasks(numReducers);

        // TODO: set the jar class
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

class RSPartitioner extends Partitioner<TextPair, Text> {
    @Override
    public int getPartition(TextPair key, Text value,
                            int numPartitions) {
        // TODO: implement getPartition such that pairs with the same first element
        //       will go to the same reducer. You can use toUnsigned as utility.
        return toUnsigned(key.getFirst().toString().hashCode())%numPartitions;
    }

    public static int toUnsigned(int val) {
        return val & Integer.MAX_VALUE;
    }
}
