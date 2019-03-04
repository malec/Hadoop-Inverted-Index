import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements Writable, WritableComparable<Pair> {

    private Text key;
    private IntWritable value;

    public Pair(Text key, IntWritable value) {
        this.key = key;
        this.value = value;
    }

    public Pair(String key, IntWritable value) {
        this(new Text(key), value);
    }

    public Pair() {
        this.key = new Text();
        this.value = new IntWritable();
    }

    public static Pair read(DataInput in) throws IOException {
        Pair wordPair = new Pair();
        wordPair.readFields(in);
        return wordPair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        value.readFields(in);
    }

    @Override
    public int compareTo(Pair other) {                         // A compareTo B
        int returnVal = this.key.compareTo(other.getKey());      // return -1: A < B
        if(returnVal != 0){                                        // return 0: A = B
            return returnVal;                                      // return 1: A > B
        } else {
            return this.value.compareTo(other.getValue());
        }
    }

    public void setKey(Object key){
        this.key = new Text(key.toString());
    }
    public void setValue(int value){
        this.value.set(value);
    }

    public Text getKey() {
        return key;
    }

    public IntWritable getValue() {
        return value;
    }
}