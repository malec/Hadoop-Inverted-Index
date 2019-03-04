import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordPair implements Writable {

    private IntWritable key;
    private IntWritable value;

    public WordPair(IntWritable key, IntWritable value) {
        this.key = key;
        this.value = value;
    }

    public WordPair(int key, int value) {
        this(new IntWritable(key), new IntWritable(value));
    }

    public WordPair() {
        this.key = new IntWritable();
        this.value = new IntWritable();
    }

    public static WordPair read(DataInput in) throws IOException {
        WordPair wordPair = new WordPair();
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
    public String toString() {
        return "{key=["+key+"]"+
               " value=["+value+"]}";
    }

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

    public void setKey(int key){
        this.key.set(key);
    }
    public void setValue(int value){
        this.value.set(value);
    }

    public IntWritable getKey() {
        return key;
    }

    public IntWritable getValue() {
        return value;
    }
}