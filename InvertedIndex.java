import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

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
			this.docId.set(((FileSplit) context.getInputSplit()).getPath().getName());
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				Text text = new Text(itr.nextToken());
				IntWritable count = hashMap.get(text);
				hashMap.put(text, count == null ? one : new IntWritable(count.get() + 1));
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<Text, IntWritable> word : hashMap.entrySet()) {
				context.write(new Pair(word.getKey(), docId), word.getValue());
			}
			System.out.println("done mapping ");
		}
	}

	public static class IIPartitioner extends Partitioner<Pair, IntWritable> {
		@Override
		public int getPartition(Pair key, IntWritable value, int numReduceTasks) {
			// System.out.println("key: " + key.getKey() + ", " + key.getValue() + " val:" +
			// value);
			String fileName = key.getValue().toString();
			int fileNumber = Integer.parseInt(fileName.substring(fileName.length() - 1, fileName.length()));
			int result = fileNumber % (numReduceTasks + 1);
			if (fileNumber != result) {
				System.out.println("Bad Reducer number. File Number != Mapper Number " + fileNumber + " != " + result);
				return 0;
			} else {
				System.out.println("File " + fileName + " goes to " + result);
				return result;
			}
		}
	}

	public static class IIReducer extends Reducer<Pair, IntWritable, Text, Text> {
		public void reduce(Iterable<Pair> keys, IntWritable value, Context context) throws IOException, InterruptedException {
			// <word , file0>, count
			// <word, file1>, count
			System.out.println(value);
			String result = "";
			for (Pair val : keys) {
				result = result + (val.getKey().toString() + ":" + val.getValue() + ";");
			}
			
			result = result.substring(0, result.length() - 1);
			context.write(new Text(result), new Text(value.toString()));
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
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
