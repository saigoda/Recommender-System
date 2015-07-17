package org.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PickTopNFlowNeighbors extends Configured implements Tool {

	/**
	 * @param args
	 */

	public static class PickTopNFlowNeighborsMap extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		/*
		 * 
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter) value -- user:count \t
		 * item1,item2.... output:
		 */
		private int _typeOfNetwork;
		private int _numOfNeighbors;
		private List<String> networks = new ArrayList<String>();

		private int _count;

		@Override
		public void configure(JobConf job) {
			_typeOfNetwork = Integer.parseInt(job.get("typeOfNetwork")) - 1;
			_numOfNeighbors = Integer.parseInt(job.get("numOfNeighbors"));
			_count = Integer.parseInt(job.get("count"));
			for (int i = 0; i < _count; i++)
				networks.add(job.get("type" + (i + 1)));
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] splits = value.toString().trim().split("\\t");
			PriorityQueue<String> queue = new PriorityQueue<String>(100000,
					new Comparator<String>() {

						@Override
						public int compare(String o1, String o2) {
							String[] o2Splits = o2.split(":");
							String[] o1Splits = o1.split(":");
							double s2 = Double
									.parseDouble(o2Splits[o2Splits.length - 1]);
							double s1 = Double
									.parseDouble(o1Splits[o1Splits.length - 1]);
							return s2 > s1 ? 1 : s2 == s1 ? 0 : -1;
						}

					});
			for (String edge : splits[1].split(",")) {
				queue.add(edge);
			}
			StringBuffer s1 = new StringBuffer();
			for (int i = 0; i < _numOfNeighbors && i < queue.size(); i++) {
				String s = queue.remove();
				String[] splits1 = s.split(":");
				s = splits1[0] + ":" + splits1[((_typeOfNetwork * 2) + 1)]
						+ ":" + splits1[splits1.length - 1];
				s1.append(s + ",");
			}
			String[] splits1 = key.toString().split(":");
			String updatedKey = splits1[0] + ":" + splits1[_typeOfNetwork + 1];
			output.collect(new Text(updatedKey), new Text(s1.toString()));
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new PickTopNFlowNeighbors(),
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
		job.setJobName("Pick Top flow neighbors");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(PickTopNFlowNeighborsMap.class);
		job.setReducerClass(IdentityReducer.class);
		// job.setNumMapTasks(100);
		job.setNumReduceTasks(10);
		job.set("typeOfNetwork", args[args.length - 1]);
		job.set("numOfNeighbors", args[2]);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		job.set("count", args[args.length - 2]);
		for (int i = 0; i < Integer.parseInt(args[args.length - 2]); i++) {
			job.set("type" + (i + 1), args[3 + i]);
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
