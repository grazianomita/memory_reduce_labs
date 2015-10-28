package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntMapWritable implements Writable {

    private MapWritable map;

    public StringToIntMapWritable() {
        map = new MapWritable();
    }

    public boolean containsKey(String key) {
        Text k = new Text(key);
        return map.containsKey(k);
    }

    public boolean containsValue(Integer value) {
        IntWritable v = new IntWritable(value);
        return map.containsValue(v);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }


    public IntWritable get(String key) {
        Text k = new Text(key);
        return (IntWritable)map.get(k);
    }

    public IntWritable put(String key, Integer value) {
        Text k = new Text(key);
        IntWritable v = new IntWritable(value);
        return (IntWritable)map.put(k, v);
    }

    public IntWritable remove(String key) {
        Text k = new Text(key);
        return (IntWritable)map.remove(k);
    }

    public int size() {
        return map.size();
    }

    public void clear() {
        map.clear();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        map.readFields(in);

        // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
        // StringToIntMapWritable when reading new records. Remember to initialize your variables
        // inside this function, in order to get rid of old data.
    }

    @Override
    public void write(DataOutput out) throws IOException {
        map.write(out);
    }

}
