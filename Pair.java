import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements Writable {

    private Text key;
    private IntWritable value;

    public Pair(Text key, IntWritable value) {
        this.key = key;
        this.value = value;
    }

    public Pair(Object key, IntWritable value) {
        this(new Text(key.toString()), value);
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

    // @Override
    // public String toString() {
    //     return new String(key+":"+value+";");
    // }

    // @Override
    // public boolean equals(Object o) {
    //     if (this == o) return true;
    //     if (o == null || getClass() != o.getClass()) return false;

    //     WordPair wordPair = (WordPair) o;

    //     if (neighbor != null ? !neighbor.equals(wordPair.neighbor) : wordPair.neighbor != null) return false;
    //     if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

    //     return true;
    // }

    // @Override
    // public int hashCode() {
    //     int result = (word != null) ? word.hashCode() : 0;
    //     result = 163 * result + ((neighbor != null) ? neighbor.hashCode() : 0);
    //     return result;
    // }

    public void setKey(Object key){
        this.key = new Text(key.toString());
    }
    public void setValue(int value){
        this.value.set(value);
    }

    public Object getKey() {
        return key;
    }

    public IntWritable getValue() {
        return value;
    }
}