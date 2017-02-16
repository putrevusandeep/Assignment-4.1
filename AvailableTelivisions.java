package mapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvailableTelivisions {

	public static class TVmapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text mapOpKey = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			boolean isNA = false;
			while (itr.hasMoreTokens()) {
				String[] parts = itr.nextToken().split("\\|");
				for (int i = 0; i < 2; i++) {
					if (parts[i].equalsIgnoreCase("na")) {
						isNA = true;
					}
				}

				if (!isNA) {
					mapOpKey.set(value.toString());
					context.write(mapOpKey, one);
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Available TV Mapper");
		job.setJarByClass(AvailableTelivisions.class);
		job.setMapperClass(TVmapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
