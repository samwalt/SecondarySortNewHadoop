import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondarySort {

	public static class Pair implements WritableComparable<Pair> {

		private int first;
		private String second;

		public static class KeyComparator extends WritableComparator {

			/*static {
				WritableComparator.define(Pair.class, new KeyComparator());
			}*/

			public KeyComparator() {
				super(Pair.class, true);
			}

			public int compare(Pair w1, Pair w2) {
				int cmp = w1.getFirst() - w2.getFirst();
				if (cmp != 0) {
					return cmp;
				}
				return -w1.getSecond().compareTo(w2.getSecond());
			}
		}

		/** 此默认的无参构造方法非常重要，缺失会导致异常
		 * Caused by: java.lang.NoSuchMethodException: SecondarySort$Pair.<init>()
			at java.lang.Class.getConstructor0(Class.java:2730)
			at java.lang.Class.getDeclaredConstructor(Class.java:2004)
			at org.apache.hadoop.util.ReflectionUtils.newInstance(ReflectionUtils.java:109)
		 */
		public Pair(){}
		
		public Pair(int first, String second) {
			this.first = first;
			this.second = second;
		}

		public int getFirst() {
			return first;
		}

		public String getSecond() {
			return second;
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeUTF(second);
		}

		public void readFields(DataInput in) throws IOException {
			first = in.readInt();
			second = in.readUTF();
		}

		public int compareTo(Pair o) {
			if (first != o.getFirst()) {
				return first < o.getFirst() ? -1 : 1;
			}
			if (!second.equals(o.getSecond())) {
				return second.compareTo(o.getSecond()) < 0 ? -1 : 1;
			}
			return 0;
		}

		public int hashCode() {
			return first * 157;
		}

		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (this == o)
				return true;
			if (o instanceof Pair) {
				Pair r = (Pair) o;
				return r.first == first && r.second.equals(second);
			} else {
				return false;
			}
		}

	}

	public static class FirstPartitioner extends
			Partitioner<Pair, NullWritable> {
		public int getPartition(Pair key, NullWritable value, int numPartitions) {
			return Math.abs(key.getFirst() * 127) % numPartitions;
		}

	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(Pair.class, true);
		}

		public int compare(Pair w1, Pair w2) {
			return w1.getFirst() - w2.getFirst();
		}
	}

	public static class OrderMapper extends
			Mapper<LongWritable, Text, Pair, NullWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException {
			String line = value.toString().trim();
			String[] items = line.split(" ");
			context.write(new Pair(Integer.parseInt(items[1]), items[0]),
					NullWritable.get());
		}
	}

	public static class OrderReducer extends
			Reducer<Pair, NullWritable, IntWritable, Text> {
		public void reduce(Pair key, Iterator<NullWritable> values,
				Context context) throws InterruptedException, IOException {
			while (values.hasNext()) {
				context.write(new IntWritable(key.getFirst()),
						new Text(key.getSecond()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: secondarysrot <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "secondary sort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		// group and partition by the first int in the pair
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(Pair.KeyComparator.class);

		// the map output is Pair, NullWritable
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(NullWritable.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
