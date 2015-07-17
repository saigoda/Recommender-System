package heterogeneous.brainStorm;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * This class is used in computing the conditional probability of link types (p(A|B)). In this class, 
 * we simply count the number of links in this network. 
 * 
 * @author sai bharath/rohitp
 *
 */
public class CountNetworkLinks extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class ComputeUserRecipeLinkCountMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, LongWritable> {

		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * This is run both on the ingredient and the recipe network to get the
		 * link count.
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * Input:
		 * 
		 * Key:<User:degree>
		 * 
		 * Value:<User1:degree1:commonNeighbors1>,<User2:degree2:commonNeighbors2
		 * >,<User3:degree3:commonNeighbors3>
		 * 
		 * Output
		 * 
		 * Key-----1 Value-----<length_of_neighbors>
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String text = value.toString().trim();
			try {
				String[] splits = text.split("\\t");
				output.collect(new Text("1"),
						new LongWritable(splits[1].split(",").length));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ComputeUserRecipeLinkCountReduce extends MapReduceBase
			implements Reducer<Text, LongWritable, LongWritable, Text> {
		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * Input
		 * 
		 * Key--1 Value---<length_of_neighbors1>,<length_of_neighbors2>,<
		 * length_of_neighbors3>
		 * 
		 * Output:
		 * 
		 * Key---Total_Length
		 * Value--''
		 */
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			long count = 0;
			while (values.hasNext()) {
				count += values.next().get();
			}
			output.collect(new LongWritable(count), new Text(""));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),
					new CountNetworkLinks(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Compute link count");
		job.setJobPriority(JobPriority.NORMAL);
		job.setMapperClass(ComputeUserRecipeLinkCountMap.class);
		job.setReducerClass(ComputeUserRecipeLinkCountReduce.class);
		// job.setNumMapTasks(50);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		 
		System.out.println("The input path is:" + args[0]);
		System.out.println("The output path is:" + args[1]);
		
		
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
