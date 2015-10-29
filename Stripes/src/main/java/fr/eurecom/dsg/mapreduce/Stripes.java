package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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


public class Stripes extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
        System.exit(res);
    }

    public Stripes (String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "Word Co-occurrence - Stripes");

        // Set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set Map Class
        job.setMapperClass(StripesMapper.class);
        // Set the map output key
        job.setMapOutputKeyClass(Text.class);
        // Set the value classes
        job.setMapOutputValueClass(IntWritable.class);

        // Set reduce class
        job.setReducerClass(StripesReducer.class);
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
        job.setJarByClass(Stripes.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

}

class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 1) {
            for (int i=0; i<tokens.length; i++) {
                Text t1 = new Text(tokens[i]);
                MapWritable h = new MapWritable();
                for (int j=0; j<tokens.length; j++) {
                    if (i == j || tokens[i].compareTo(tokens[j]) == 0)
                        continue;
                    Text t2 = new Text(tokens[j]);
                    if (h.containsKey(t2)) {
                        int total = ((IntWritable)h.get(t2)).get() + 1;
                        h.put(t2, new IntWritable(total));
                    }
                    else
                        h.put(t2, new IntWritable(1));
                }
                context.write(t1, h);
                h.clear();
            }
        }
    }

}

class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable map = new MapWritable();

        //For each stripe
        for (MapWritable value : values) {
            for (MapWritable.Entry element : value.entrySet()) {
                //Key and value of current Object inside the current Stripe
                Text k = (Text)element.getKey();
                IntWritable v = (IntWritable)element.getValue();
                //if map already contains k, upgrade its value = oldValue + v
                if (map.containsKey(k)) {
                    int total = ((IntWritable)map.get(k)).get() + v.get();
                    map.put(k, new IntWritable(total));
                }
                else
                    map.put(k, v);
            }
        }
        context.write(key, map);
        map.clear();
    }

}