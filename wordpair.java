import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class wordpair implements Writable, WritableComparable<wordpair> {

    private Text key;
    private Text value;

    public wordpair(Text key, Text value) {
        this.key = key;
        this.value = value;
    }

    public wordpair(String key, Text value) {
        this(new Text(key), value);
    }

    public wordpair() {
        this.key = new Text();
        this.value = new Text();
    }

    public static wordpair read(DataInput in) throws IOException {
        wordpair wordPair = new wordpair();
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
    public int compareTo(wordpair other) {                         // A compareTo B
        int returnVal = this.key.compareTo(other.getKey());      // return -1: A < B
        if(returnVal != 0){                                        // return 0: A = B
            return returnVal;                                      // return 1: A > B
        } else {
            return this.value.compareTo(other.getValue());
        }
    }
    @Override
    public String toString() {
        return this.key.toString() + "," + this.value.toString();
    }

    public void setKey(Object key){
        this.key = new Text(key.toString());
    }
    public void setValue(Text value){
        this.value.set(value);
    }

    public Text getKey() {
        return key;
    }

    public Text getValue() {
        return value;
    }
}