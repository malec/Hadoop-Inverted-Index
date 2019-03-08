import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;;

public class InvertedIndex {
	public static class IIMapper extends Mapper<Object, Text, Pair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private HashMap<Text, IntWritable> hashMap = new HashMap<Text, IntWritable>();
		private Text docId = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				Text text = new Text(itr.nextToken());
				IntWritable count = hashMap.get(text);
				hashMap.put(text, count == null ? one : new IntWritable(count.get() + 1));
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<Text, IntWritable> word : hashMap.entrySet()) {
				Text docId = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
				context.write(new Pair(word.getKey(), docId), word.getValue());
			}
		}
	}

	public static class IIPartitioner extends Partitioner<Pair, IntWritable> {
		private final static char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
		@Override
		public int getPartition(Pair key, IntWritable value, int numReduceTasks) {
			int hash = Arrays.binarySearch(alphabet, key.getKey().toString().toLowerCase().toCharArray()[0]) * numReduceTasks / alphabet.length;
			return hash;
		}
	}

	public static class IIReducer extends Reducer<Pair, IntWritable, Text, Text> {
		private LinkedHashMap<String, LinkedList<Tuple>> hashMap = new LinkedHashMap<String, LinkedList<Tuple>>();

		public void reduce(Pair key, Iterable<IntWritable> wordCounts, Context context)
				throws IOException, InterruptedException {
			int wordCount = 0;
			for (IntWritable count : wordCounts) {
				wordCount += count.get();
			}
			// System.out.println("word " + key.getKey().toString() + " is " + wordCount + "
			// in doc " + key.getValue().toString());
			LinkedList<Tuple> list = hashMap.get(key.getKey().toString());
			// System.out.println("list is " + (list == null ? "null" : list.toString()));
			if (list != null) {
				// add pair of doc id and count to the list
				list.add(new Tuple(key.getValue().toString(), wordCount));
			} else {
				list = new LinkedList<Tuple>();
				list.add(new Tuple(key.getValue().toString(), wordCount));
				hashMap.put(key.getKey().toString(), list);
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String, LinkedList<Tuple>> wordEntries : hashMap.entrySet()) {
				String word = wordEntries.getKey();
				String output = "";
				for (Tuple pair : wordEntries.getValue()) {
					// System.out.println("word is: " + word + "Pair is " + pair.toString());
					output += pair.x + ":" + pair.y + ";";
				}
				output = output.substring(0, output.length() - 1);
				context.write(new Text(word), new Text(output));
			}
		}
	}

	public static class Tuple { 
		public final String x; 
		public final int y; 
		public Tuple(String x, int y) { 
		  this.x = x; 
		  this.y = y; 
		} 
	  } 

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(IIMapper.class);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(IIPartitioner.class);
		job.setReducerClass(IIReducer.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
