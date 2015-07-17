package heterogeneous.brainStorm;

import java.io.IOException;
import java.util.HashSet;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * This class is used to assist in computing the conditional probability of the links in two or more networks. For example, if we have three networks 
 * of types A, B, and C, we need to compute the following probabilities: p(A|B) = p(A int B) / p(B), P(A|C), P(B|A), P(B|C), P(C|A), P(C|B). 
 * 
 * In this class we compute the intersection count which is the number links common in the two networks => summation of common neighbors in both the networks
 * for a user over all the users.
 * 
 * @author rohitp/sai bharath
 *
 */
public class IntersectionCountComputer extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class ConcatenateTwoNetworksMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * This file takes the ingredient and the recipe neighborhood without
		 * putting a restriction on the top ten neighbors(It takes the whole neighborhood) 
		 * 
		 * Input:
		 * 
		 * <User:degree>\t<User1:degree1:commonItems>,<User2:degree2:commonItems>
		 * ,<User3:degree3:commonItems>
		 * 
		 * Output: Key ---- <User> 
		 * 
		 * Value --- <User1:degree1:commonItems>,<User2:degree2:commonItems>
		 * ,<User3:degree3:commonItems>
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			String[] splits = value.toString().trim().split("\\t");
			output.collect(new Text(splits[0].split("\\:")[0].trim()), new Text(splits[1]));
		}
	}
	
	
	public static class ConcatenateTwoNetworksReduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * Input:
		 * 
		 * Key---<User>
		 * Value---<User1:degree1:commonItems>,<User2:degree2:commonItems>
		 * ,<User3:degree3:commonItems>,<User1:degree1:commonItems>
		 * 
		 * Output:
		 * 
		 * <User>\t<common_neighbors_count>
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuffer buffer = new StringBuffer();
			// This hash set is used to store neighbors
			HashSet<String> neighbors = new HashSet<String>();
			int commonNeighbors = 0;
			
			while (values.hasNext()) {
				
				String[] neighborString = values.next().toString().split(",");
				
				for(int i=0;i<neighborString.length;i++)
				{
					String neighborUser = neighborString[i].trim().split("\\:")[0].trim();
					if (neighbors.contains(neighborUser))
						commonNeighbors++;
					else
						neighbors.add(neighborUser);
				}
				
				
			}
//			// This hash set is used to store neighbors
//			HashSet<String> neighbors = new HashSet<String>();
//			int commonNeighbors = 0;
//			for (String s : buffer.toString().split(",")) {
//				s = s.split(":")[0];
//				// If the neighbor is already in the hash set increment it's
//				// count. Otherwise we store it in the hashset
//				if (neighbors.contains(s))
//					commonNeighbors++;
//				else
//					neighbors.add(s);
//			}
			// We emit the user and the common neighbors count
			output.collect(key, new Text(commonNeighbors + ""));
		}

	}

	public static class ConcatenateTwoNetworksMapStep2 extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 * 
		 * Input:
		 * <user>\t<common_neighbors>
		 * 
		 * Output
		 * 
		 * Key---------1
		 * 
		 * Value-------Common_neighbors
		 * 
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub

			String[] splits = value.toString().trim().split("\\t");
			output.collect(new Text("1"), new Text(splits[1]));
		}
	}



	public static class ConcatenateTwoNetworksReduceStep2 extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter) 
		 * 
		 * Input 
		 * 
		 * Key----1
		 * 
		 * Value:--common-neighbors1,common-neighbors2,common-neighbors3...
		 * 
		 * 
		 * Output:
		 * 
		 * Key--sum of common neighbors
		 * Value--''
		 * 
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int count = 0;
			while (values.hasNext()) {
				count += Integer.parseInt(values.next().toString());
			}
			output.collect(new Text(count + ""), new Text(""));
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new IntersectionCountComputer(),
					args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		
		job.setJarByClass(this.getClass());
		job.setJobName("Neighbourhood containing two neighbors(User and Ingredient)");
		job.setJobPriority(JobPriority.NORMAL);
		job.setMapperClass(ConcatenateTwoNetworksMap.class);
		job.setReducerClass(ConcatenateTwoNetworksReduce.class);
		// job.setNumMapTasks(100);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		System.out.println("Input Path 1: " + args[0]);
		System.out.println("Input Path 2: " + args[1]);
		System.out.println("Output Path: " + args[2]);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class);
		FileSystem.get(job).delete(new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		JobClient.runJob(job);
		
		System.out.println("***********DONE********");
		
		
		JobConf job1 = new JobConf(super.getConf(), this.getClass());
		job1.setJarByClass(this.getClass());
		job1.setJobName("Neighbourhood containing two neighbors(User and Ingredient) Step 2");
		job1.setJobPriority(JobPriority.NORMAL);
		job1.setMapperClass(ConcatenateTwoNetworksMapStep2.class);
		job1.setReducerClass(ConcatenateTwoNetworksReduceStep2.class);
		// job.setNumMapTasks(100);
		job1.setNumReduceTasks(1);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		System.out.println("Input Path: " + args[2]);
		System.out.println("Output Path: " + args[3]);
		
		FileInputFormat.setInputPaths(job1, new Path(args[2]));
		FileSystem.get(job1).delete(new Path(args[3]));
		FileOutputFormat.setOutputPath(job1, new Path(args[3]));
		JobClient.runJob(job1);
//		FileSystem.get(job1).delete(new Path(args[2]));
		return 0;
	}

}
