package org.allrecipes.CosineSimilarity;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNeighbors extends Configured implements Tool {
	public static class TopNeighborsMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			String text = value.toString().trim();
			String[] splits = text.split("\\t");
			output.collect(new Text(splits[0]), new Text(splits[1]));
		}
	}
	public static class TopNeighborsReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			PriorityQueue<String> queue = new PriorityQueue<String>(100000,
					new Comparator<String>() {

						@Override
						public int compare(String s1, String s2) {
							double num1 = Double.parseDouble(s1.split(",")[1]);
							double num2 = Double.parseDouble(s2.split(",")[1]);
							return num2 > num1 ? 1 : num1 == num2 ? 0 : -1;
						}
					});
			
			while(values.hasNext()){
				queue.add(values.next().toString());
			}
			StringBuilder sb = new StringBuilder();
			while(!queue.isEmpty()){
				sb.append(queue.remove().split(",")[0]+",");
			}
			output.collect(key, new Text(sb.toString()));
		}
		
	}
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new TopNeighbors(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(super.getConf(), this.getClass());
		job.setJarByClass(this.getClass());
		job.setJobName("Top Neighbors");
		job.setJobPriority(JobPriority.VERY_HIGH);
		job.setMapperClass(TopNeighborsMap.class);
		job.setReducerClass(TopNeighborsReduce.class);
		// job.setNumMapTasks(20);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileSystem.get(job).delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		System.out.println("***********DONE********");
		return 0;
	}

}
