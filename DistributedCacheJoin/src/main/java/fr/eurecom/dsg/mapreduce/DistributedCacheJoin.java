package fr.eurecom.dsg.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

public class DistributedCacheJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputFile;
    protected static Path inputTinyFile;
    private int numReducers;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DistributedCacheJoin(args), args);
        System.exit(res);
    }

    public DistributedCacheJoin(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: DistributedCacheJoin <num_reducers> <input_tiny_file> <input_file> <output_dir>");
            System.exit(0);
        }

        this.numReducers = Integer.parseInt(args[0]);
        this.inputTinyFile = new Path(args[1]);
        this.inputFile = new Path(args[2]);
        this.outputDir = new Path(args[3]);

        // inputTinyFile is expected to be on the local filesystem
        File inputTinyFileDescriptor = new File(args[1]);
        if(!inputTinyFileDescriptor.exists()) {
            System.out.println("Error: file " + args[1] + "does not exist");
            System.exit(-1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        //Add the "secondary" file to the distributed cache
        DistributedCache.addCacheFile(inputTinyFile.toUri(), conf);

        Job job = new Job (conf);
        job.setJobName("DistributedCacheJoin");

        // Set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set Map Class
        job.setMapperClass(MSMap.class);
        // Set the map output key
        job.setMapOutputKeyClass(Text.class);
        // Set the value classes
        job.setMapOutputValueClass(LongWritable.class);

        // set combiner class
        job.setCombinerClass(MSReduce.class);

        // Set reduce class
        job.setReducerClass(MSReduce.class);
        // Set the reduce output key
        job.setOutputKeyClass(Text.class);
        // Set the value classes
        job.setOutputValueClass(LongWritable.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // Add the input file as job input (from HDFS) to the variable inputPath
        FileInputFormat.addInputPath(job, inputFile);
        // Set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, outputDir);
        // Set the number of reducers using variable numberReducers
        job.setNumReduceTasks(numReducers);
        // Set the jar class
        job.setJarByClass(DistributedCacheJoin.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}

class MSMap extends Mapper<LongWritable, Text, Text, LongWritable> {

    Set<String> exclude = new HashSet<String>();
    static final private LongWritable one = new LongWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path [] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (cacheFilesLocal == null)
            return;
        for (Path p : cacheFilesLocal) {
            if (p.getName().toString().trim().equals(DistributedCacheJoin.inputTinyFile.toString())) {
                exclude.clear();
                String line;
                BufferedReader in = new BufferedReader(new FileReader(p.toString()));
                while ((line = in.readLine()) != null) {
                    String splitted[] = line.split("\\s+");
                    for (String s : splitted)
                        exclude.add(s);
                }
                in.close();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer st= new StringTokenizer(value.toString());
        while (st.hasMoreTokens()) {
            String tmp = st.nextToken();
            if (exclude.contains(tmp) == false)
                context.write(new Text(tmp), one);
        }
    }

}

class MSReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text word, Iterable<LongWritable> vals, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : vals) {
            sum += value.get();
        }
        context.write(word, new LongWritable(sum));
    }

}