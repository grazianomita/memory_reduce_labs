package fr.eurecom.dsg.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


/**
 * TextPair is a Pair of Text that is Writable (Hadoop serialization API)
 * and Comparable to itself.
 *
 */
public class TextPair implements WritableComparable<TextPair> {

    Text first;
    Text second;

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public void setFirst(String first) {
        this.first = new Text(first);
    }

    public void setSecond(String second) {
        this.second = new Text(second);
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public TextPair() {
		TextPair(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        this.set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        this.set(first, second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int hashCode() {
        int hash = 5;

        hash = 89 * hash + (this.first != null ? this.first.hashCode() : 0);
        hash = 89 * hash + (this.second != null ? this.second.hashCode() : 0);

        return hash;
    }

    @Override
    public boolean equals(Object o) {
        TextPair tp = (TextPair) o;

        if (o == this)
            return true;

        if (!(o instanceof TextPair))
            return false;

        if (this.compareTo(tp) == 0)
            return true;
        return false;
    }

    @Override
    public int compareTo(TextPair tp) {
        int status = this.getFirst().toString().compareTo(tp.getFirst().toString());
        if (status == 0) {
            status = this.getSecond().toString().compareTo(tp.getSecond().toString());
            if ( status == 0)
                return 0;
        }
        return status;
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }

    /** Compare two pairs based on their values */
    public static class Comparator extends WritableComparator {

        /** Reference to standard Hadoop Text comparator */
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {
        WritableComparator.define(TextPair.class, new Comparator());
    }

    /** Compare just the first element of the Pair */
    public static class FirstComparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair) a).getFirst().compareTo(((TextPair) b).getFirst());
            }
            return super.compare(a, b);
        }

    }

}